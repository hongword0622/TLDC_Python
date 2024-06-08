import pymysql.cursors
from pythonping import ping
import time
from datetime import datetime, timedelta

connection_pool = pymysql.connect(
            host="10.8.1.3",
            user="root",
            password="tldc8899",
            database="smart_meter",
            port=3307,
            autocommit=True
        )
def get_device_info(cursor):
    """取得監控設備資料"""
    cursor.execute("SELECT Device_Name, IP_Address, Enable_State,Peroid_Time,Device_id,City,Place_name FROM device_info")
    return cursor.fetchall()

def check_device_status(device_name, IP_Address, period_time):
    """檢查設備狀態"""
    result = ping(IP_Address, count=3)  # ping 3 次
    #print(f"Checked device: {device_name}, Status: {result.success()}")
    if result.success():
        Status = "Yes"
    else:
        Status = "No"
#    time.sleep(period_time)
    return Status

def write_data_db(Device_id, device_name, Status, IP_Address, City,Place_name,period_time):
    cursor = connection_pool.cursor()
    record_to_insert = (Device_id, device_name, Status, IP_Address, City,Place_name)
    insert_query = "INSERT INTO internet_device_data (Device_id, device_name, Status, IP_Address, City,Place_name) VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.execute(insert_query, record_to_insert)
    connection_pool.commit()
    current_time = datetime.now()
    print("Insert time:",current_time)
    #print(f"Write OK {current_time}")
    time.sleep(period_time)

def main():
    try:
        while True:
            cursor = connection_pool.cursor()

            devices = get_device_info(cursor)

            for device in devices:
                device_name, IP_Address, Enable_State, period_time, Device_id, City, Place_name = device
                if Enable_State:
                    Status = check_device_status(device_name, IP_Address, period_time)
                    write_data_db(Device_id, device_name, Status, IP_Address, City, Place_name, period_time)

            cursor.close()  # Close the cursor after processing all devices
            #time.sleep(10)
    except KeyboardInterrupt:
        print("Program Interrupting.")
        pass
    finally:
        connection_pool.close()  # Ensure the connection pool is properly closed when the program exits

main()
