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

# 資料庫設定
connection_pool = pymysql.connect(
    host="localhost",
    user="root",
    password="tldc8899",
    database="temp_humd",
    port=3307,
    autocommit=True
)

#處理溫溼度資料(.xml)
def process_sensor_data(sensor_file, location):
    try:
        cursor = connection_pool.cursor()

        # Parse XML file to Python dictionary
        with open(sensor_file, 'r', encoding='utf-8') as xml_file:
            filename = os.path.basename(sensor_file)
            sensor_ID, _ = os.path.splitext(filename)
            xml_data = xml_file.read()
            data_dict = xmltodict.parse(xml_data)

        # Extract temperature and humidity data
        records_to_insert = []

        for ch in data_dict['file']['remote']['ch']:
            unit = ch['unit']
            
            # Reverse the order of timestamps
            for v in reversed(ch['current']['v']):
                timestamp = int(v['@t'])
                dt_object = datetime.fromtimestamp(timestamp)
                formatted_date = dt_object.strftime('%Y-%m-%d %H:%M:%S')
                value = float(v['#text'])


                if unit == '°C':
                    measurement_type = 'temperature'
                elif unit == '%':
                    measurement_type = 'humidity'

                # Check for existing record
                check_query = "SELECT COUNT(*) FROM t1 WHERE Datetime = %s AND location = %s AND type = %s"
                cursor.execute(check_query, (formatted_date, location, measurement_type))
                count = cursor.fetchone()[0]

                if count == 0:
                    # If the record does not exist, add it to the batch
                    records_to_insert.append((formatted_date, measurement_type, value, location,sensor_ID))
                    break  # Break out of the loop after the first record

        # Bulk insert with a transaction
        if records_to_insert:
            insert_query = "INSERT INTO t1 (Datetime, type, value, location,sensor_ID) VALUES (%s, %s, %s, %s, %s)"
            cursor.executemany(insert_query, records_to_insert)

        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"Insert time: {current_time}, File: {sensor_file}")

    except Exception as error:
        print(f"Error: {error}")

    finally:
        if cursor:
            cursor.close()

# Define a list of sensor files and their corresponding locations
def main():
    sensor_files_list = []

    try:
        cursor = connection_pool.cursor()

        query = "SELECT File_name, location, country FROM temp_humd_info WHERE Write_Status = 'ON' ORDER BY country desc"
        cursor.execute(query)

        sensor_files_list = cursor.fetchall()

        for sensor in sensor_files_list:
            sensor_file = sensor[0]
            location = sensor[1]
            country = sensor[2]
            sensor_file_with_extension = sensor_file + ".xml"
            print(sensor)
            process_sensor_data(sensor_file=sensor_file_with_extension, location=location)

            # Wait for one minute
            # time.sleep(20)
            #sys.exit()
    except KeyboardInterrupt:
        print("Program terminated by user.")
        #break
    except Exception as e:
        print(f"An error occurred: {e}")
        #break

if __name__ == "__main__":
    main()
