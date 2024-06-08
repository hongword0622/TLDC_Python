import pymysql.cursors
from pymysql import OperationalError
import requests
from datetime import datetime
import time

#DataBase Config設定
connection_pool = pymysql.connect(
    host="10.8.1.3",
    user="root",
    password="tldc8899",
    database="temp_humd",
    port=3307,
    autocommit=True
)
# Line Notify 設定  
line_notify_token = 'SVBeAIKTJECrGSU8m7NeFhczlBiI43yyDjMOPY3NIcI'
line_notify_api = 'https://notify-api.line.me/api/notify'

def Send_Check():
    # 全域變數
    global KM_state, KM_cinema_state, Taipei_state

    # 建立資料庫連線
    cursor1 = connection_pool.cursor()

    # 取得所有啟用的 location資料
    locations_query = "SELECT DISTINCT Location FROM temp_humd_info WHERE Enable_Status = 'ON'"
    cursor1.execute(locations_query)
    country_result = cursor1.fetchall()
    print(f"Enabled locations: {country_result}")

    # 初始化狀態列表
    KM_state = []
    KM_cinema_state = []
    Taipei_state = []

    # 建立資料庫連線
    cursor = connection_pool.cursor()

    # 定義查詢語句以取得最新的狀態資料
    send_check_query = """
    SELECT state, Location, Type, MAX(Time) AS max_time
    FROM (
        SELECT state, Location, Type, Time,
               DENSE_RANK() OVER (PARTITION BY Location ORDER BY Time DESC) AS location_rank
        FROM temp_humd_state
        WHERE Location IN ('金門', '金門影城', '台北', '花蓮')
    ) AS RankedRecords
    GROUP BY Location, Type;
    """

    # 執行查詢語句
    cursor.execute(send_check_query)
    locations_result = cursor.fetchall()

    # 逐一查詢結果
    for location_data in locations_result:

        # 分割欄位資料
        state, location, data_type, Dates = location_data

        # 檢查狀態是否為 "1"
        if state == "1":
            # 狀態為 "1" 時，檢查溫濕度
            check_temperature_humidity(location, data_type)
        else:
            # 狀態不為 "1" 時，顯示 None
            print("None")

#發送LINE通知
def send_line_notify(message, token):
    url = "https://notify-api.line.me/api/notify"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    payload = {
        "message": message
    }
    response = requests.post(url, headers=headers, params=payload)
    return response.status_code

#檢查溫溼度是否超過標準
def check_temperature_humidity(location, data_type):
    try:
    #建立資料庫連線
        cursor = connection_pool.cursor()
    #查詢所有地點
        locations_query = "SELECT DISTINCT Location FROM temp_humd_info WHERE country = %s"
        cursor.execute(locations_query, (location,))
    #取出地點資料並將資料進行逗點分隔處理
        locations_result = cursor.fetchall()
        location_list = [f"'{row[0]}'" for row in locations_result]
        placeholders = ', '.join(location_list)
    #查詢各地點已啟用最新溫溼度資料
        temp_query = """
        SELECT t1.type, t1.value, t1.location, t1.datetime
        FROM t1
        INNER JOIN (
            SELECT DISTINCT Location
            FROM temp_humd_info
            WHERE country = %s AND alarm_Status = 'ON'
        ) AS locations ON t1.location = locations.Location
        INNER JOIN (
            SELECT location, type, MAX(datetime) AS max_datetime
            FROM t1
            WHERE location IN ({}) AND type = %s
            GROUP BY location, type
        ) AS latest_dates ON t1.location = latest_dates.location AND t1.type = latest_dates.type AND t1.datetime = latest_dates.max_datetime
        """.format(placeholders)

        cursor.execute(temp_query, (location, data_type))
        #取得各地點溫溼度查詢資料
        temp_results = cursor.fetchall()

        temperature_data = {}
        humidity_data = {}

        #將溫溼度進行分類，並將觀測值與時間分開儲存
        for row in temp_results:
            data_type, value, location, timestamp = row

            if data_type == 'temperature':
                if location not in temperature_data:
                    temperature_data[location] = {'value': [], 'timestamp': []}
                temperature_data[location]['value'].append(value)
                temperature_data[location]['timestamp'].append(timestamp)

            if data_type == 'humidity':
                if location not in humidity_data:
                    humidity_data[location] = {'value': [], 'timestamp': []}
                humidity_data[location]['value'].append(value)
                humidity_data[location]['timestamp'].append(timestamp)

        cursor2 = connection_pool.cursor()
        
#查詢溫溼度標準
        std_query = "SELECT Temp_Min, Temp_Max, Humd_Min, Humd_Max FROM temp_humd_stardard ORDER BY id desc limit 1;"
        cursor2.execute(std_query)
        temphund_std = cursor2.fetchone()
        
#確認是否取得標準
        if temphund_std:
            lt, gt, lh, gh = temphund_std
            
# 設定與轉換溫度濕度標準
        temperature_threshold = int(gt)
        humidity_threshold = int(gh)
        temperature_lthreshold = int(lt)
        humidity_lthreshold = int(lh)

 # 準備與對應所有地點的溫度和濕度資料
        location_list = [row[0].strip("'") for row in locations_result]
        all_temp_data = []
        all_humd_data = []
        for loc in location_list:
            if loc in temperature_data:
                all_temp_data.extend(temperature_data[loc]['value'])

            if loc in humidity_data:
                all_humd_data.extend(humidity_data[loc]['value'])

        # 檢查是否超過溫度或濕度閾值，並發送 LINE Notify
        if any(temp > temperature_threshold for temp in all_temp_data):
            # 所在地點設備溫度高於閾值
            exceeded_temp_locations = [loc for loc in location_list if any(temp > temperature_threshold for temp in temperature_data[loc]['value'])]
            exceeded_values_str = ', '.join(map(str, [temp for temp in all_temp_data if temp > temperature_threshold]))
            huhs = f"\n目前溫度 : {exceeded_values_str}˚C\n地點 : {', '.join(exceeded_temp_locations)}\n狀態 : 已超過標準{temperature_threshold}˚C!!!"
            send_line_notify(huhs, line_notify_token)
            print("溫度Over", huhs)

        elif any(temp < temperature_lthreshold for temp in all_temp_data):
            # 所在地點設備溫度低於標準
            exceeded_temp_locations = [loc for loc in location_list if any(temp < temperature_lthreshold for temp in temperature_data[loc]['value'])]
            exceeded_values_str = ', '.join(map(str, [temp for temp in all_temp_data if temp < temperature_lthreshold]))
            huhs = f"\n目前溫度 : {exceeded_values_str}˚C\n地點 : {', '.join(exceeded_temp_locations)}\n狀態 : 已低於標準{temperature_lthreshold}˚C!!!"
            send_line_notify(huhs, line_notify_token)
            print("溫度Down", huhs)

        if any(humd > humidity_threshold for humd in all_humd_data):
            # 所在地點設備濕度高於標準
            exceeded_humd_locations = [loc for loc in location_list if any(humd > humidity_threshold for humd in humidity_data[loc]['value'])]
            exceeded_values_str = ', '.join(map(str, [humd for humd in all_humd_data if humd > humidity_threshold]))
            huhs = f"\n目前濕度 : {exceeded_values_str}%\n地點 : {', '.join(exceeded_humd_locations)}\n狀態 : 已超過標準{humidity_threshold}%!!!"
            send_line_notify(huhs, line_notify_token)
            print("濕度Over", huhs)

        elif any(humd < humidity_lthreshold for humd in all_humd_data):
            # 所在地點設備濕度低於標準
            exceeded_humd_locations = [loc for loc in location_list if any(humd < humidity_lthreshold for humd in humidity_data[loc]['value'])]
            exceeded_values_str = ', '.join(map(str, [humd for humd in all_humd_data if humd < humidity_lthreshold]))
            huhs = f"\n目前濕度 : {exceeded_values_str}%\n地點 : {', '.join(exceeded_humd_locations)}\n狀態 : 已低於標準{humidity_lthreshold}%!!!"
            send_line_notify(huhs, line_notify_token)
            print("濕度Down", huhs)
#例外狀況
    except Exception as e:
        print(f"Error: {e}")
# 無論是否發生異常，都要關閉資料庫游標，減少資料占用
    finally:
        if cursor:
            cursor.close()

# 主程式執行
Send_Check()
#由於目前警報是每小時進行偵測與通知，上述程式為一次性，現透過排程以每小時進行

