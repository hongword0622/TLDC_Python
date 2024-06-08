import pymysql.cursors
from pymysql import OperationalError
import requests
from datetime import datetime
import time
# MySQL 連線設定
connection_pool = pymysql.connect(
    host="10.8.1.3",
    user="root",
    password="tldc8899",
    database="temp_humd",
    port=3307,
    autocommit=True
)
# Line Notify 設定  
#line_notify_token = '2qQr0Bh9Pbfl6MyMujxK7HjS2KftJal5qJegbF9Klae'
line_notify_token = 'SVBeAIKTJECrGSU8m7NeFhczlBiI43yyDjMOPY3NIcI'
line_notify_api = 'https://notify-api.line.me/api/notify'
#KM_state, KM_cinema_state, Taipei_state = False, False, False

def Send_Check():
    global KM_state, KM_cinema_state, Taipei_state
    cursor1 = connection_pool.cursor()

    # 獲取所有已啟用的 location
    locations_query = "SELECT DISTINCT Location FROM temp_humd_info WHERE Enable_Status = 'ON'"
    cursor1.execute(locations_query)
    country_result = cursor1.fetchall()
    print(f"Enabled locations: {country_result}")

    KM_state = []
    KM_cinema_state = []
    Taipei_state = []

    cursor = connection_pool.cursor()

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
    cursor.execute(send_check_query)
    locations_result = cursor.fetchall()

    for location_data in locations_result:
        #print(location_data)

        state, location, data_type, Dates = location_data

        if state == "1":
            check_temperature_humidity(location, data_type)
        else:
            print("None")

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

def check_temperature_humidity(location, data_type):
    try:
        cursor = connection_pool.cursor()

        locations_query = "SELECT DISTINCT Location FROM temp_humd_info WHERE country = %s"
        cursor.execute(locations_query, (location,))
        locations_result = cursor.fetchall()

        # 檢查是否獲取到有效的 location
        #print(f"Locations for {location}: {locations_result}")

        location_list = [f"'{row[0]}'" for row in locations_result]
        placeholders = ', '.join(location_list)

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
        temp_results = cursor.fetchall()
        #print(f"Results for {location} ({data_type}): {temp_results}")

        temperature_data = {}
        humidity_data = {}

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

        std_query = "SELECT Temp_Min, Temp_Max, Humd_Min, Humd_Max FROM temp_humd_stardard ORDER BY id desc limit 1;"
        cursor2.execute(std_query)
        temphund_std = cursor2.fetchone()

        if temphund_std:
            lt, gt, lh, gh = temphund_std

        temperature_threshold = int(gt)
        humidity_threshold = int(gh)
        temperature_lthreshold = int(lt)
        humidity_lthreshold = int(lh)

        location_list = [row[0].strip("'") for row in locations_result]
        all_temp_data = []
        all_humd_data = []

        for loc in location_list:
            if loc in temperature_data:
                all_temp_data.extend(temperature_data[loc]['value'])

            if loc in humidity_data:
                all_humd_data.extend(humidity_data[loc]['value'])

        #print("All Temperature Data:", all_temp_data)
        #print("All Humidity Data:", all_humd_data)

        # 檢查是否超過溫度或濕度閾值，並發送 LINE Notify
        if any(temp > temperature_threshold for temp in all_temp_data):
            exceeded_temp_locations = [loc for loc in location_list if any(temp > temperature_threshold for temp in temperature_data[loc]['value'])]
            exceeded_values_str = ', '.join(map(str, [temp for temp in all_temp_data if temp > temperature_threshold]))
            huhs = f"\n目前溫度 : {exceeded_values_str}˚C\n地點 : {', '.join(exceeded_temp_locations)}\n狀態 : 已超過標準{temperature_threshold}˚C!!!"
            send_line_notify(huhs, line_notify_token)
            print("溫度Over", huhs)

        elif any(temp < temperature_lthreshold for temp in all_temp_data):
            exceeded_temp_locations = [loc for loc in location_list if any(temp < temperature_lthreshold for temp in temperature_data[loc]['value'])]
            exceeded_values_str = ', '.join(map(str, [temp for temp in all_temp_data if temp < temperature_lthreshold]))
            huhs = f"\n目前溫度 : {exceeded_values_str}˚C\n地點 : {', '.join(exceeded_temp_locations)}\n狀態 : 已低於標準{temperature_lthreshold}˚C!!!"
            send_line_notify(huhs, line_notify_token)
            print("溫度Down", huhs)

        if any(humd > humidity_threshold for humd in all_humd_data):
            exceeded_humd_locations = [loc for loc in location_list if any(humd > humidity_threshold for humd in humidity_data[loc]['value'])]
            exceeded_values_str = ', '.join(map(str, [humd for humd in all_humd_data if humd > humidity_threshold]))
            huhs = f"\n目前濕度 : {exceeded_values_str}%\n地點 : {', '.join(exceeded_humd_locations)}\n狀態 : 已超過標準{humidity_threshold}%!!!"
            send_line_notify(huhs, line_notify_token)
            print("濕度Over", huhs)

        elif any(humd < humidity_lthreshold for humd in all_humd_data):
            exceeded_humd_locations = [loc for loc in location_list if any(humd < humidity_lthreshold for humd in humidity_data[loc]['value'])]
            exceeded_values_str = ', '.join(map(str, [humd for humd in all_humd_data if humd < humidity_lthreshold]))
            huhs = f"\n目前濕度 : {exceeded_values_str}%\n地點 : {', '.join(exceeded_humd_locations)}\n狀態 : 已低於標準{humidity_lthreshold}%!!!"
            send_line_notify(huhs, line_notify_token)
            print("濕度Down", huhs)
    except Exception as e:
        print(f"Error: {e}")

# 呼叫檢查函式
Send_Check()
#time.sleep(60)

#https://10-214-0-55.tldc-hl-nas3.direct.quickconnect.to:5001/#
#https://10.214.0.55:5001/
