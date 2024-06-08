import pymysql.cursors
import requests
from datetime import datetime
import time
# MySQL 連線設定
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
def check_and_update_website_statuses(line_notify_token):
    connection_pool = pymysql.connect(
    host="localhost",
    user="root",
    password="tldc8899",
    database="temp_humd",
    port=3307,
    autocommit=True
)
    try:
        cursor = connection_pool.cursor()
        cursor1 = connection_pool.cursor()

        cursor.execute('SELECT DISTINCT IP_Address, Website_name FROM website_list')

  
        ip_website_pairs = cursor.fetchall()  

        cursor1.execute('SELECT DISTINCT Website_name FROM website_list')
        Website_name = [row[0] for row in cursor1.fetchall()]

        for ip_address, website_name in ip_website_pairs:
            try:
                response = requests.get(ip_address)
                if response.status_code == 200:
                    status = "OK"
                    #send_line_notify(f"{website_name} is OK", line_notify_token)
                else:
                    status = "Error"
                    send_line_notify(f"Error for {website_name}: {response.status_code} \n URL : {ip_address}", line_notify_token)
            except requests.RequestException as e:
                status = "Error"
                error_message = str(e)
                send_line_notify(f"Request_Exception for {website_name}: {error_message} \n URL : {ip_address}", line_notify_token)

            update_query = 'UPDATE website_list SET state = %s, Current_times = %s WHERE IP_Address = %s'
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(update_query, (status, current_time, ip_address))
            #conn.commit()
            print("OK")

    finally:

        cursor.close()
        cursor1.close()
        connection_pool.close()

check_and_update_website_statuses(line_notify_token)
