import xmltodict
import pymysql.cursors
from datetime import datetime, timedelta
import time
from pymysql import OperationalError
import os
import csv
from lxml import etree
import xmltodict
import sys

#DataBase Config設定
connection_pool = pymysql.connect(
    host="localhost",
    user="root",
    password="tldc8899",
    database="temp_humd",
    port=3307,
    autocommit=True
)

def process_sensor_data(sensor_file, location):
    try:
        # 獲取資料庫連線
        cursor = connection_pool.cursor()

        # 讀取 XML 檔案並轉換為 Python 字典
        with open(sensor_file, 'r', encoding='utf-8') as xml_file:
            # 提取檔案名稱和 sensor_ID
            filename = os.path.basename(sensor_file)
            sensor_ID, _ = os.path.splitext(filename)
            xml_data = xml_file.read()
            data_dict = xmltodict.parse(xml_data)

        # 提取溫度和濕度資料，並準備插入資料庫的暫存列表
        records_to_insert = []

        for ch in data_dict['file']['remote']['ch']:
            unit = ch['unit']
            
            # 反轉時間戳記的順序，以確保插入最新的資料
            for v in reversed(ch['current']['v']):
                timestamp = int(v['@t'])
                dt_object = datetime.fromtimestamp(timestamp)
                formatted_date = dt_object.strftime('%Y-%m-%d %H:%M:%S')
                value = float(v['#text'])

                # 判斷資料的測量類型（溫度或濕度）
                if unit == '°C':
                    measurement_type = 'temperature'
                elif unit == '%':
                    measurement_type = 'humidity'

                # 檢查是否存在相同的資料記錄
                check_query = "SELECT COUNT(*) FROM t1 WHERE Datetime = %s AND location = %s AND type = %s"
                cursor.execute(check_query, (formatted_date, location, measurement_type))
                count = cursor.fetchone()[0]

                if count == 0:
                    # 如果資料記錄不存在，將其添加到插入列表中
                    records_to_insert.append((formatted_date, measurement_type, value, location, sensor_ID))
                    break  # 在插入第一條記錄後跳出循環

        # 進行批量插入
        if records_to_insert:
            insert_query = "INSERT INTO t1 (Datetime, type, value, location, sensor_ID) VALUES (%s, %s, %s, %s, %s)"
            cursor.executemany(insert_query, records_to_insert)

        # 輸出插入時間和檔案名稱
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"Insert time: {current_time}, File: {sensor_file}")

    except Exception as error:
        print(f"Error: {error}")

    finally:
        # 關閉資料庫連線
        if cursor:
            cursor.close()

def main():
    # 儲存 sensor 檔案路徑和相應地點的列表
    sensor_files_list = []

    try:
        # 建立資料庫游標
        cursor = connection_pool.cursor()
        
        # 查詢所有啟用的溫溼度設備地點
        query = "SELECT File_name, location, country FROM temp_humd_info WHERE Write_Status = 'ON' ORDER BY country desc"
        cursor.execute(query)
        sensor_files_list = cursor.fetchall()

        # 針對每個 sensor，處理其相應的檔案
        for sensor in sensor_files_list:
            sensor_file = sensor[0]  # 取得 sensor 檔案名稱
            location = sensor[1]  # 取得 sensor 的地點
            country = sensor[2]  # 取得 sensor 的國家
            sensor_file_with_extension = sensor_file + ".xml"  # 加上檔案名
            print(sensor)
            
            #處理 sensor 資料的函數
            process_sensor_data(sensor_file=sensor_file_with_extension, location=location)

#例外情況
    except KeyboardInterrupt:
        print("Program terminated by user.")
        #break
    except Exception as e:
        print(f"An error occurred: {e}")
        #break
#執行主程式
if __name__ == "__main__":
    main()
