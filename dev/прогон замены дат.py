import requests
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import datetime
from datetime import datetime
import time
import json
import requests
from requests.exceptions import SSLError
import psycopg2
import random

headers = {
    'authority': 'moto.av.by',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'ru,en;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "YaBrowser";v="24.1", "Yowser";v="2.5"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 YaBrowser/24.1.0.0 Safari/537.36',
}

# Чтение json конфига
with open('config.json') as file:
    config = json.load(file)

mail_login = config['sender login']
mail_password = config['sender password']
pgre_login = config['postgre login']
pgre_password = config['postgre password']
pgre_host = config['postgre host']
pgre_port = config['postgre port']
pgre_db = config['postgre database']
recipients = config['mail recipients']

#Подключение к postgres
conn = psycopg2.connect(
    host = pgre_host,
    port = pgre_port,
    database = pgre_db,
    user = pgre_login,
    password = pgre_password
)
import pandas as pd
cursor = conn.cursor()
# Чтение json конфига
df = pd.read_csv('to_check_dates.csv')

urls_to_check = df.iloc[:, 0].tolist()
for index, row in df.iterrows():
    id_to_check = row['ID']
    url_to_check = row['URL']

    # Вынять имеющуюся дату
    old_date_query = """
select date from av_full where id = %s 
""" % (id_to_check)
    cursor.execute(old_date_query)
    old_date_for_id = cursor.fetchone()
    #old_date_for_id = datetime.strptime(old_date_for_id[0], "%Y-%m-%dT%H:%M:%S%z")
    old_date_for_id = old_date_for_id[0].strftime("'%Y-%m-%d %H:%M:%S'") 

    print(f'В проверке id - {id_to_check}, url - {url_to_check}')

    try:
        response = requests.get(url_to_check, headers=headers)
        # Дальнейшая обработка успешного запроса
    except SSLError:
        # Обработка ошибки SSL
        print(f"Произошла ошибка SSL при обращении к URL: {url_to_check}. Пропускаем выполнение для данного URL.")
    except requests.exceptions.ConnectionError as e:
        print("Ошибка подключения:", e)
        continue
    except Exception as e:
        continue

    src = response.text
    soup = BeautifulSoup(src, 'lxml')

    if response.status_code == 200:
        src = response.text 
        soup = BeautifulSoup(src, 'lxml')
        script_element = soup.find("script", id="__NEXT_DATA__") #Достаем жсон
        json_string = script_element.string #Конвертируем жсон в стринг
        data = json.loads(json_string) #Пакуем в data

        if data['props']['initialState']['advert']['advert']:
            try:
                actual_date = data['props']['initialState']['advert']['advert']['publishedAt']
                actual_date = datetime.strptime(actual_date, "%Y-%m-%dT%H:%M:%S%z")
                actual_date = actual_date.strftime("'%Y-%m-%d %H:%M:%S'") 
            except KeyError:
                print(f'Не нашел даты для {id_to_check}, {url_to_check}')
                continue
            
            if old_date_for_id != actual_date:
                update_query = ("UPDATE av_full SET date = %s WHERE id = %s") % (actual_date, id_to_check) 
                cursor.execute(update_query)
                conn.commit()
                                    
                update_query = ("UPDATE av_full SET date_corrected = %s WHERE id = %s") % (old_date_for_id, id_to_check) 
                cursor.execute(update_query)
                conn.commit()

            else:
                update_query = ("UPDATE av_full SET date = %s WHERE id = %s") % (actual_date, id_to_check) 
                cursor.execute(update_query)
                conn.commit()

            wait_amount = random.randint(4, 7)
            time.sleep(wait_amount)
        else:
            print(f'Не смог открыть {id_to_check}, {url_to_check}')
            continue
    else:
        print(f'Не смог открыть {id_to_check}, {url_to_check}')
        continue

# Закрытие курсора и подключения    
cursor.close()
conn.close()