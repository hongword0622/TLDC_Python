#智慧電表寫入程式
import os
import time
import pandas as pd
from pymysql import OperationalError
import pymysql.cursors
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime, timedelta
import numpy as np
import multiprocessing
# 資料庫連接配置
connection_pool = pymysql.connect(
    host="localhost",
    user="root",
    password="tldc8899",
    database="smart_meter",
    port=3307,
    autocommit=True
)
def replace_parentheses(column_name):
    return column_name.replace('(', '_').replace(')', '_').replace('+', '_')

# 函式：資料庫寫入
def write_to_database(file_path):
    #conn = None
    cursor = None

    try:
        # 讀取CSV檔案
        time.sleep(2)
        df = pd.read_csv(file_path, header=4, skipinitialspace=True, error_bad_lines=False)

        # 處理欄位名稱
        df.columns = df.columns.map(replace_parentheses)
        df.columns = df.columns.str.replace(' ', '_')
        device_id = file_path.split('\\')[-1][5:10] 
        df['device_id'] = device_id

        df['Local_Time_Stamp'] = pd.to_datetime(df['Local_Time_Stamp']) + timedelta(hours=8)
        df['Local_Time_Stamp'] = df['Local_Time_Stamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df = df.replace({np.nan: ''})
        # 建立資料庫連接
        #conn = mysql.connector.connect(**db_config)
        cursor = connection_pool.cursor()

        # 將DataFrame轉換為資料列表
        data_tuples = [tuple(row) for row in df.itertuples(index=False)]

        # 建立SQL INSERT INTO
        columns = ', '.join([f"`{col}`" for col in df.columns])
        placeholders = ', '.join(['%s'] * len(df.columns))
        sql = f"INSERT INTO meter_data ({columns}) VALUES ({placeholders})"

        # 執行批量插入
        cursor.executemany(sql, data_tuples)
        # 提交事務
        #conn.commit()
        print("批量插入成功！")
        cursor.close()
    except Exception as e:
        print("批量插入失敗:", e)
    finally:
        if cursor:
            cursor.close()


class MyHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:  
            print(f"發現新檔案：{event.src_path}")
            process = multiprocessing.Process(target = write_to_database,args=(event.src_path,))
            process.start()
            #write_to_database(event.src_path)

# 主函数
def main():
    directory_to_watch = 'C:\\RebexTinySftpServer-Binaries-Latest\\data'

    observer = Observer()
    observer.schedule(MyHandler(), directory_to_watch, recursive=True)

    observer.start()

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
