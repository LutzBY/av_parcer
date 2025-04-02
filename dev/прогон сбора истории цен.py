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
from psycopg2.extras import RealDictCursor
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

cursor = conn.cursor(cursor_factory=RealDictCursor)

# Готовим нужные столбцы и строки
select_query = """
SELECT id, price, url
FROM av_full
WHERE capacity >=299
AND cylinders > 1
AND exclude_flag is False
AND status not in ('Недоступная ссылка', 'На проверке')
ORDER by id desc
"""
cursor.execute(select_query)
rows = cursor.fetchall()
rows_count_na = cursor.rowcount
print(f"Строк для проверки: {rows_count_na}")
# счетчики
counter_id = 1
counter_written = 0
dead_link_count = 0
broken_link_count = 0

# апи на курсы
curr_api = "https://api.nbrb.by/exrates/rates?periodicity=0"
response = requests.get(curr_api)
curr_resp = response.json()
curr_byn_usd = next((c['Cur_OfficialRate'] for c in curr_resp if c['Cur_Abbreviation'] == 'USD'), None)
curr_byn_eur = next((c['Cur_OfficialRate'] for c in curr_resp if c['Cur_Abbreviation'] == 'EUR'), None)
curr_eur_usd = curr_byn_eur / curr_byn_usd

# Функция получения и записи истории изменения цен
def get_price_history (id_value, curr_byn_usd, curr_eur_usd):
    # получения json истории по айди
    pr_history_url = f"https://api.av.by/offers/{id_value}/price-history" 
    pr_h_response = requests.get(pr_history_url, headers=headers)

    # Проверка успешности
    if pr_h_response.status_code == 200:
        pr_h_data = pr_h_response.json()  # Получаем джсон
        for i in pr_h_data:
            pr_h_date = i['date']
            pr_h_date = datetime.strptime(pr_h_date, '%Y-%m-%dT%H:%M:%S%z') # Преобразования текстового значения в дату
            pr_h_date = pr_h_date.replace(tzinfo=None) # убираем таймзон
            pr_h_currency = i['currency']
            pr_h_amount = i['amount']
            pr_h_idu = f'{id_value}_{pr_h_date.year}-{pr_h_date.month}-{pr_h_date.day}_{pr_h_date.hour}-{pr_h_date.minute}-{pr_h_date.second}'
            if pr_h_currency == 'byn':
                pr_h_usd_eq = int(round(pr_h_amount / curr_byn_usd, 0))
            elif pr_h_currency == 'eur':
                pr_h_usd_eq = int(round(pr_h_amount * curr_eur_usd, 0))
            else:
                pr_h_usd_eq = pr_h_amount
            # пишем каждую строку а в конце коммит
            print(f'idu {pr_h_idu}. Цена на дату {pr_h_date} составила {pr_h_amount} {pr_h_currency}, в экв. {pr_h_usd_eq} usd')
            # пишем
            pr_h_query = """
            INSERT INTO av_price_history (id, id_av, date, amount, currency, usd_eq)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """
            cursor.execute(pr_h_query, (pr_h_idu, id_value, pr_h_date, pr_h_amount, pr_h_currency, pr_h_usd_eq))
            conn.commit()
        return 1
    else:
        print(f"Ошибка {pr_h_response.status_code}: {pr_h_response.text}")
        return 0

# Сам цикл
for row in rows:
    id_value = row['id']
    act_price = row['price']
    url = row['url']
    print(f'--------------------------')
    print(f"{counter_id} - Обрабатываю ID {id_value}, url - {url}")

    counter = get_price_history(id_value, curr_byn_usd, curr_eur_usd)
    
    counter_id +=1

    # Диапазон рандомных значений для задержки
    wait_amount = random.randint(5, 15)
    print(f"Ждем {wait_amount} секунд")
    
    time.sleep(wait_amount)

# После обработки получить актуальные данные из базы - дубликаты
select_query = """select count(id) as id, count(id_av) as id_av
from av_price_history"""
cursor.execute(select_query)
after_report = cursor.fetchone()
prices_count = after_report['id']
ids_count = after_report['id_av']


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
    записано изменений цен {prices_count} штук
    для {ids_count} айди
    не смог открыть или прочитать {dead_link_count} штук
    не добавил цен для {rows_count_na - ids_count} объявлений
Спасибо за внимание <3
"""
)

subject = 'Результат работы разового скрипта сбора ценовой истории'
for recipient in recipients:
    send_email(subject, mail_contents, recipient)