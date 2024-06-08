import pymysql.cursors
import requests
from datetime import datetime
import time

# LINE通知設定
line_notify_token = 'SVBeAIKTJECrGSU8m7NeFhczlBiI43yyDjMOPY3NIcI'
line_notify_api = 'https://notify-api.line.me/api/notify'
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

#檢測與更新網站運作狀態
def check_and_update_website_statuses(line_notify_token):
    #資料庫設定
    connection_pool = pymysql.connect(
    host="localhost",
    user="root",
    password="tldc8899",
    database="temp_humd",
    port=3307,
    autocommit=True
)
    
    try:
        #初始化資料庫游標
        cursor = connection_pool.cursor()
        cursor1 = connection_pool.cursor()
        
        #查詢並取得各網站的名稱與IP資料
        cursor.execute('SELECT DISTINCT IP_Address, Website_name FROM website_list')
        ip_website_pairs = cursor.fetchall()  

        #逐一處理每個網站
        for ip_address, website_name in ip_website_pairs:
            try:
                #使用Request向網站發出請求
                response = requests.get(ip_address)
                if response.status_code == 200:
                    status = "OK"
                    #如狀態碼為200，代表網站運行正常
                else:
                    status = "Error"
                    send_line_notify(f"Error for {website_name}: {response.status_code} \n URL : {ip_address}", line_notify_token)
                    #如狀態碼不為200，判斷網站異常，並發送LINE通知訊息
            except requests.RequestException as e:
                status = "Error"
                # 發生 RequestException，表示無法發送請求，可能是網站無法連接或超時等問題，並發送LINE通知訊息
                error_message = str(e)
                send_line_notify(f"Request_Exception for {website_name}: {error_message} \n URL : {ip_address}", line_notify_token)

            #更新網站狀態與檢測時間
            update_query = 'UPDATE website_list SET state = %s, Current_times = %s WHERE IP_Address = %s'
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(update_query, (status, current_time, ip_address))
            #conn.commit()
            print("OK")

    finally:
        #關閉資料庫游標，減少資料占用
        cursor.close()
        cursor1.close()
        connection_pool.close()
        
#執行主程式
check_and_update_website_statuses(line_notify_token)
