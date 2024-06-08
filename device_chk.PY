#引入所需套件
import pymysql.cursors
from pythonping import ping
import time
from datetime import datetime, timedelta

#MySQL Config設定
connection_pool = pymysql.connect(
            host="10.8.1.3",
            user="root",
            password="tldc8899",
            database="smart_meter",
            port=3307,
            autocommit=True
        )

def get_device_info(cursor):
    #取得監控設備資料
    cursor.execute("SELECT Device_Name, IP_Address, Enable_State,Peroid_Time,Device_id,City,Place_name FROM device_info")
    return cursor.fetchall()

def check_device_status(device_name, IP_Address, period_time):
    #檢查設備狀態
    result = ping(IP_Address, count=3)  # ping 3 次
    #print(f"Checked device: {device_name}, Status: {result.success()}")
    #判斷連線狀態是否正常
    if result.success():
        Status = "Yes"
    else:
        Status = "No"
   #回傳狀態
    return Status

#寫入資料庫
def write_data_db(Device_id, device_name, Status, IP_Address, City,Place_name,period_time):
#初始化資料庫游標(處理查詢資料)
    cursor = connection_pool.cursor()
#寫入SQL與包含資料欄位
    record_to_insert = (Device_id, device_name, Status, IP_Address, City,Place_name)
    insert_query = "INSERT INTO internet_device_data (Device_id, device_name, Status, IP_Address, City,Place_name) VALUES (%s, %s, %s, %s, %s, %s)"
#執行SQL    
    cursor.execute(insert_query, record_to_insert)
#提交
    connection_pool.commit()
    current_time = datetime.now()
    print("Insert time:",current_time)
    #print(f"Write OK {current_time}")
#時間間格(依序每台設備的間格時間，輪流進行檢測)
    time.sleep(period_time)

#主程式
def main():
    try:
        while True:
            cursor = connection_pool.cursor()
#取得設備資料
            devices = get_device_info(cursor)

#將資料依序取出，並判斷是否啟用，如啟用再進行檢測，並寫入資料庫
            for device in devices:
                device_name, IP_Address, Enable_State, period_time, Device_id, City, Place_name = device
                if Enable_State:
                    Status = check_device_status(device_name, IP_Address, period_time)
                    write_data_db(Device_id, device_name, Status, IP_Address, City, Place_name, period_time)

            cursor.close()  #資料庫關閉游標
#例外情況
    except Exception as e:
        print(f"Error: {e}")
        pass
#最終 將資料庫連線關閉
    finally:
        connection_pool.close()  # Ensure the connection pool is properly closed when the program exits
#執行主程式
main()
