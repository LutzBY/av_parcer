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
import csv
from email.mime.text import MIMEText
from email.utils import COMMASPACE
import smtplib

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
    # если теперь что-то будет не так, то попробовать requests.Session()
    #подставить ключи если будет надо
    #"X-Api-Key": "-",
    #"X-User-Group": "-",

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
cursor = conn.cursor()
# Готовим нужные столбцы и строки
select_query = """
SELECT id, seller, seller_id, url
FROM av_full 
WHERE seller is null
ORDER by id desc
"""
cursor.execute(select_query)
rows = cursor.fetchall()
rows_count_na = cursor.rowcount
print(f"Строк для проверки: {rows_count_na}")
counter_id = 1
counter_written = 0
counter_fl = 0
counter_ul = 0
dead_link_count = 0
broken_link_count = 0
# Сам цикл
for row in rows:
    id_value = row[0] # стобец id с индексом 0
    seller = row[1]
    seller_id = row[2]
    url = row[3]
    print(f'--------------------------')
    print(f"{counter_id} - Обрабатываю ID {id_value}, url - {url}")

    try:
        response = requests.get(url, headers=headers)
        # Дальнейшая обработка успешного запроса
    except SSLError:
        # Обработка ошибки SSL
        print(f"Произошла ошибка SSL при обращении к URL: {url}. Пропускаем выполнение для данного URL.")
    except requests.exceptions.ConnectionError as e:
        print("Ошибка подключения:", e)
        continue
    except Exception as e:
        print(f"Произошла ошибка при обработке id {id_value}: {e}")
        broken_link_count += 1
        continue
    
    src = response.text
    soup = BeautifulSoup(src, 'lxml')

    if response.status_code == 200:
        src = response.text 
        soup = BeautifulSoup(src, 'lxml')
        script_element = soup.find("script", id="__NEXT_DATA__") #Достаем жсон
        json_string = script_element.string #Конвертируем жсон в стринг
        data = json.loads(json_string) #Пакуем в data

        #Извлекаем даты из data
        try:
            organization = data['props']['initialState']['advert']['advert'].get('organizationTitle', '')
            n_seller = data['props']['initialState']['advert']['advert'].get('sellerName', '')
            
            if organization != '':
                n_seller_id = data['props']['initialState']['advert']['advert'].get('organizationId', 'null')
                organization_query = ("UPDATE av_full SET seller = '%s', seller_id = %s WHERE id = %s") % (organization, n_seller_id, id_value) 
                cursor.execute(organization_query)
                counter_ul +=1
                print ('Записана организация')
                conn.commit()
                counter_written +=1               
            elif n_seller != '':
                n_seller_query = ("UPDATE av_full SET seller = '%s' WHERE id = %s") % (n_seller, id_value) 
                cursor.execute(n_seller_query)
                counter_fl +=1
                print ('Записано имя')
                conn.commit()
                counter_written +=1
            else:
                print('Нет имени или организации')

        # Если страница открылась но она с домиком 404
        except (KeyError, json.JSONDecodeError, TypeError, AttributeError):
            # Обн. базу, уст. статус и стат. дату для соотв id
            print(f"ссылка сдохла")
            dead_link_count += 1
    else: # Если респонс не 200, т.е. страница не прочиталась
        print(f"ссылка сдохла")
        dead_link_count += 1
    
    counter_id +=1

    # Диапазон рандомных значений для задержки
    wait_amount = random.randint(5, 15)
    print(f"Ждем {wait_amount} секунд")
    
    time.sleep(wait_amount)
# После обработки получить актуальные данные из базы - дубликаты
select_query = """select count(*)
from av_full
where seller is null"""
cursor.execute(select_query)
nulls_left = cursor.fetchone()[0]

# Закрытие курсора и подключения    
cursor.close()
conn.close()
# Функция отправки результата на email
def send_email(subject, body, recipient):
    sender = mail_login 
    password = mail_password
    smtp_server = 'smtp.yandex.ru'
    smtp_port = 465

    message = MIMEText(body)
    message['Subject'] = subject
    message['From'] = sender
    message['To'] = recipient

    try:
        server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        server.login(sender, password)
        server.sendmail(sender, recipient, message.as_string())
        server.quit()
        print(f'Email successfully sent to {recipient}')
    except Exception as e:
        print('Error sending email:', str(e))
# Параметры отправки на email
mail_contents = (f"""
Для проверки отобрано {rows_count_na} строк
Обработано - {counter_id} шт.
    записано имен физлиц {counter_fl} штук
    записано организаций {counter_ul} штук
    не смог открыть или прочитать {dead_link_count} штук
    осталось без имен {nulls_left} объявлений
Спасибо за внимание <3
"""
)

subject = 'Результат работы разового скрипта сбора имен'
for recipient in recipients:
    send_email(subject, mail_contents, recipient)