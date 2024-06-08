#引用所需套件
import time
import pandas as pd#資料處理
import pymysql.cursors#資料庫連線
from watchdog.observers import Observer#監視資料夾異動
from watchdog.events import FileSystemEventHandler#監視資料夾異動
from datetime import datetime, timedelta#日期時間
import numpy as np#NA資料處理
import multiprocessing#多行程處理

# 資料庫配置
connection_pool = pymysql.connect(
    host="localhost",
    user="root",
    password="tldc8899",
    database="smart_meter",
    port=3307,
    autocommit=True
)

# 將包含 '(', ')', '+' 符號的字串替換為 '_'
def replace_parentheses(column_name):
    return column_name.replace('(', '_').replace(')', '_').replace('+', '_')

# 資料庫寫入
def write_to_database(file_path):
    cursor = None

    try:
        # 讀取CSV檔案
        df = pd.read_csv(file_path, header=4, skipinitialspace=True, error_bad_lines=False)

        # 處理欄位名稱(針對devic_id)
        df.columns = df.columns.map(replace_parentheses)
        df.columns = df.columns.str.replace(' ', '_')
        device_id = file_path.split('\\')[-1][5:10] 
        df['device_id'] = device_id
        
        # 處理欄位名稱(轉換時間格式，並處理NaN異常資料)
        df['Local_Time_Stamp'] = pd.to_datetime(df['Local_Time_Stamp']) + timedelta(hours=8)
        df['Local_Time_Stamp'] = df['Local_Time_Stamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        # 將NaN值替換為空字符串，以便後續處理
        df = df.replace({np.nan: ''})
        
        # 建立資料庫連接
        cursor = connection_pool.cursor()

        # 將DataFrame轉換為資料列表
        data_tuples = [tuple(row) for row in df.itertuples(index=False)]

        # 格式處理與建立寫入資料SQL
        columns = ', '.join([f"`{col}`" for col in df.columns])
        placeholders = ', '.join(['%s'] * len(df.columns))
        sql = f"INSERT INTO meter_data ({columns}) VALUES ({placeholders})"

        # 執行批量寫入
        cursor.executemany(sql, data_tuples)
        print("批量插入成功！")
        #關閉資料庫
        cursor.close()
        
    #發生異常情況   
    except Exception as e:
        print("批量插入失敗:", e)
        
    # 無論是否發生異常，都要關閉資料庫游標
    finally:
        if cursor:
            cursor.close()

# 定義處理新增的CSV檔案的類別
class MyHandler(FileSystemEventHandler):
    def on_created(self, event):
        # 如果發現的事件不是設定資料夾
        if not event.is_directory:  
            # 輸出發現的新檔案路徑
            print(f"發現新檔案：{event.src_path}")
            # 使用多處理程序來處理新檔案，並將其寫入資料庫
            process = multiprocessing.Process(target = write_to_database,args=(event.src_path,))
            # 啟動多處理程序
            process.start()

# 主函数
def main():
    #監視寫入資料的路徑
    directory_to_watch = 'C:\\RebexTinySftpServer-Binaries-Latest\\data'
    
    #新增觀察者
    observer = Observer()
    #指定觀察者監視的程式函式與對應資料夾
    observer.schedule(MyHandler(), directory_to_watch, recursive=True)
    #觀察開始
    observer.start()

    try:
        # 持續觀察，每間隔時間為60秒
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        # 如果偵測到鍵盤中斷（Ctrl+C），停止觀察者
        observer.stop()
        
    # 等待觀察者停止工作
    observer.join()
    
#執行主程式
if __name__ == "__main__":
    main()
