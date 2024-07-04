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


current_time_start = datetime.now()
ctsf = current_time_start.strftime("%Y-%m-%d %H:%M:%S")
print(f"Привет! Текущая дата - {ctsf}")

# Забираем всю таблицу и НЕ делаем бекап
cursor = conn.cursor()
select_query = "SELECT * from av_full"
cursor.execute(select_query)
rows_full = cursor.fetchall()
rows_count = cursor.rowcount
print(f"Строк в базе: {rows_count}")


# Готовим нужные столбцы и строки
select_query = "SELECT id, status, status_date, url, price FROM av_full WHERE status IN ('Актуально', 'Временно недоступно', 'На проверке')"
cursor.execute(select_query)
rows = cursor.fetchall()
rows_count_na = cursor.rowcount
print(f"Строк для проверки: {rows_count_na}")

# Квери на запись и курсор execute
def update_and_write(updated_status, updated_status_date, id_value):
    update_query = ("UPDATE av_full SET status = '%s', status_date = %s WHERE id = %s") % (updated_status, updated_status_date, id_value) 
    cursor.execute(update_query)
    conn.commit()

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

# Подсчет итераций
changed_status_count = 0
stayed_active_count = 0
dead_link_count = 0
unchanged_status_count = 0
price_changed_count = 0
price_difference_sum = 0

# Сам цикл
for row in rows:
    id_value = row[0] # стобец id с индексом 0
    print(f"Обрабатываю ID {id_value}")
    status_value = row[1] # стобец status с индексом 1
    url = row[3] # стобец url с индексом 3
    price_ex = row[4] # столбец price

    # Диапазон рандомных значений для задержки
    wait_amount = random.randint(4, 7)
    print(f"Ждем {wait_amount} секунд")
    time.sleep(wait_amount)

    try:
        response = requests.get(url, headers=headers)
        # Дальнейшая обработка успешного запроса
    except SSLError:
        # Обработка ошибки SSL
        print(f"Произошла ошибка SSL при обращении к URL: {url}. Пропускаем выполнение для данного URL.")
    except requests.exceptions.ConnectionError as e:
        print("Ошибка подключения:", e)
        continue
    
    src = response.text
    soup = BeautifulSoup(src, 'lxml')

    if response.status_code == 200:
        src = response.text 
        soup = BeautifulSoup(src, 'lxml')
        status = soup.find('div', class_='card__warning') #Проверяем наличие таблички закрыто
        script_element = soup.find("script", id="__NEXT_DATA__") #Достаем жсон
        json_string = script_element.string #Конвертируем жсон в стринг
        data = json.loads(json_string) #Пакуем в data

        # Проверка на корректно закрытую страничку
        script_status = data['props']['initialState']['advert']['advert']

        #Извлекаем даты из data
        try:
            published_at_str = data['props']['initialState']['advert']['advert']['publishedAt']
            #Конвертируем в нормальный формат и обрезаем лишнее
            published_at = datetime.strptime(published_at_str, "%Y-%m-%dT%H:%M:%S%z")
            formatted_published_at = published_at.strftime("%Y-%m-%d %H:%M:%S")
            
            #Цикл условий
            # Сверка цены
            price_upd = data['props']['initialState']['advert']['advert']['price']['usd']['amount']
            if price_ex != price_upd:
                price_query = ("UPDATE av_full SET price = '%s' WHERE id = %s") % (price_upd, id_value) 
                cursor.execute(price_query)
                conn.commit()
                price_diff = price_upd - price_ex
                price_changed_count += 1
                price_difference_sum += price_diff
                print(f"Изменилась цена для id {id_value} на {price_diff} USD, price_diff_sum составляет {price_difference_sum} USD")
            else:
                print(f"Цена для id {id_value} осталась прежней")
            
            # Добавление организации
            organization = data['props']['initialState']['advert']['advert'].get('organizationTitle', 'null')
            if organization != 'null':
                organization_query = ("UPDATE av_full SET seller = '%s' WHERE id = %s") % (organization, id_value) 
                cursor.execute(organization_query)
                conn.commit()
                print(f"Organization = {organization}")

            #Если табличка закрыто есть
            if status: 
                reason = soup.find('div', class_='gallery__status')
                updated_status = reason.text

                #Если статус изменился:
                if status_value != updated_status:
                    try:
                        removed_at_str = data['props']['initialState']['advert']['advert']['removedAt']
                    except KeyError:
                        removed_at_str = data['props']['initialState']['advert']['advert']['refreshedAt']
                    removed_at = datetime.strptime(removed_at_str, "%Y-%m-%dT%H:%M:%S%z")
                    updated_status_date = removed_at.strftime("'%Y-%m-%d %H:%M:%S'") 
    
                    # Квери на запись и курсор execute
                    update_and_write(updated_status, updated_status_date, id_value)

                    print(f"смена статуса для id - {id_value}, ST-{updated_status}, STD-{updated_status_date}")
                    changed_status_count += 1 # Крутим счетчик
                else:
                    print(f"статус {updated_status} не изменился для id - {id_value}")
                    unchanged_status_count +=1 # Крутим счетчик
                    continue

            else: #Если таблички нет, то объява еще актуальная
                print(f"статус {status_value} не изменился для id - {id_value}")
                stayed_active_count += 1 # Крутим счетчик
        except:
            script_status = '404'
            updated_status = "Недоступная ссылка"
            updated_status_date = 'null'
            print(f"ссылка сдохла для id {id_value}, ST-{updated_status}, STD-{updated_status_date}")

    else: # Если респонс не 200, т.е. страница не прочиталась
        updated_status = "Недоступная ссылка"
        updated_status_date = 'null'

        # Обн. базу, уст. статус и стат. дату для соотв id
        update_and_write(updated_status, updated_status_date, id_value)

        print(f"ссылка сдохла для id {id_value}, ST-{updated_status}, STD-{updated_status_date}")
        dead_link_count += 1
    

# Закрытие курсора и подключения    
cursor.close()
conn.close()

# Блок подсчета занятого времени
current_time_finish = datetime.now()
ctff = current_time_finish.strftime("%Y-%m-%d %H:%M:%S")
elapsed_time = current_time_finish - current_time_start
elapsed_minutes = elapsed_time.total_seconds() / 60
elapsed_minutes_formatted = "{:.2f}".format(elapsed_minutes)
print(f"Дата завершения - {current_time_finish}, времени заняло - {elapsed_minutes_formatted} минут")

print(f"Проверка актуальности завершена успешно, сменило статус {changed_status_count} штук, осталось активными {stayed_active_count} штук. Ссылка недоступна у {dead_link_count} штук.")

# Параметры отправки на email
mail_contents = (f"""
Привет!
Дата начала - {ctsf}
Дата завершения - {ctff}, времени заняло - {elapsed_minutes_formatted} минут
В базе {rows_count} строк
Для проверки отобрано {rows_count_na} строк
Проверка актуальности завершена успешно:
    смена статуса у {changed_status_count} штук, 
    осталось активными {stayed_active_count} штук,
    закрытый статус сохранился у {unchanged_status_count} штук,
    ссылка недоступна у {dead_link_count} штук
Цена изменилась у {price_changed_count} штук
Общее именение цен составило {price_difference_sum} USD
Спасибо за внимание <3
"""
)

subject = 'Результат работы скриптов. №2 Проверка статуса и бекап'
for recipient in recipients:
    send_email(subject, mail_contents, recipient)