#引入所需套件
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

#處理資料CSV格式(將 '(' ')' '+' 取代為 '_')
def replace_parentheses(column_name):
    return column_name.replace('(', '_').replace(')', '_').replace('+', '_')

# 資料庫寫入
def write_to_database(file_path):
    cursor = None

    try:
        # 讀取CSV檔案
        time.sleep(2)
        df = pd.read_csv(file_path, header=4, skipinitialspace=True, error_bad_lines=False)

        # 處理CSV欄位名稱(針對device_id)
        df.columns = df.columns.map(replace_parentheses)
        df.columns = df.columns.str.replace(' ', '_')
        device_id = file_path.split('\\')[-1][5:10] 
        df['device_id'] = device_id

        #處理CSV時間欄位資料(將時間日期進行格式化)
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

        # 提交工作
        #conn.commit()
        print("批量插入成功！")
        cursor.close()

#例外狀況
    except Exception as e:
        print("批量插入失敗:", e)
    finally:
        if cursor:
            cursor.close()

#處理新增CSV檔案
class MyHandler(FileSystemEventHandler):
    def on_created(self, event):

#如果發現的事件不是目錄
        if not event.is_directory:  
# 輸出發現的新檔案路徑
            print(f"發現新檔案：{event.src_path}")
# 使用多處理程序來處理新檔案，將其寫入資料庫
            process = multiprocessing.Process(target = write_to_database,args=(event.src_path,))
# 啟動多處理程序
            process.start()


# 主函数
def main():
#監視寫入檔案的路徑
    directory_to_watch = 'C:\\RebexTinySftpServer-Binaries-Latest\\data'

# 創建觀察者對象
    observer = Observer()
# 指定觀察者應該監視的處理程序和目錄
    observer.schedule(MyHandler(), directory_to_watch, recursive=True)
# 啟動觀察者
    observer.start()

    try:
# 持續運行觀察者，間隔時間為60秒
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        observer.stop()
# 等待觀察者完全停止
    observer.join()
#啟動主程式
if __name__ == "__main__":
    main()
