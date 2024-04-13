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


# Чтение конфига с пасвордами
with open('config.txt', 'r') as file:
    lines = file.readlines()

mail_login = lines[1].strip()
mail_password = lines[3].strip()
pgre_login = lines[5].strip()
pgre_password = lines[7].strip()
pgre_host = lines[9].strip()
pgre_port = lines[11].strip()
pgre_db = lines[13].strip()
    
#Подключение к postgres
conn = psycopg2.connect(
    host = pgre_host,
    port = pgre_port,
    database = pgre_db,
    user = pgre_login,
    password = pgre_password
)


current_time_start = datetime.now()
print(f"Привет! Текущая дата - {current_time_start}")

# Забираем всю таблицу и делаем бекап
cursor = conn.cursor()
select_query = "SELECT * from av_full"
cursor.execute(select_query)
rows_full = cursor.fetchall()
rows_count = cursor.rowcount
print(f"Строк в базе: {rows_count}")

# Создаем и сохраняем бекап в csv
backup_file = f"av_full_backup_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
with open(backup_file, 'w', newline='', encoding='utf-8 sig') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerows(rows_full)
print(f"Бекап таблицы сохранен в файле: {backup_file}")

# Готовим нужные столбцы и строки
select_query = "SELECT id, status, status_date, url FROM av_full WHERE status IN ('Актуально', 'Временно недоступно', 'На проверке')"
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
        print('Email sent successfully')
    except Exception as e:
        print('Error sending email:', str(e))    

# Подсчет итераций
changed_status_count = 0
stayed_active_count = 0
dead_link_count = 0

# Сам цикл
for row in rows:
    status_value = row[1] # стобец status с индексом 1
    id_value = row[0] # стобец id с индексом 0
    url = row[3] # стобец url с индексом 3
    # Диапазон рандомных значений для задержки
    wait_amount = random.randint(4, 9)

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
        #Извлекаем даты из data
        published_at_str = data['props']['initialState']['advert']['advert']['publishedAt']
        #Конвертируем в нормальный формат и обрезаем лишнее
        published_at = datetime.strptime(published_at_str, "%Y-%m-%dT%H:%M:%S%z")
        formatted_published_at = published_at.strftime("%Y-%m-%d %H:%M:%S")
        
        #Цикл условий
        if status: #Если табличка закрыто есть
            reason = soup.find('div', class_='gallery__status')
            updated_status = reason.text

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

        else: #Если таблички нет, то объява еще актуальная
            print(f"нет смены статуса для id - {id_value}")
            stayed_active_count += 1 # Крутим счетчик

    else: # Если респонс не 200, т.е. страница не прочиталась
        updated_status = "Недоступная ссылка"
        updated_status_date = "null"

        # Обн. базу, уст. статус и стат. дату для соотв id
        update_and_write(updated_status, updated_status_date, id_value)

        print(f"ссылка сдохла для id {id_value}, ST-{updated_status}, STD-{updated_status_date}")
        dead_link_count += 1
    print(f"Ждем {wait_amount} секунд")
    time.sleep(wait_amount)

# Закрытие курсора и подключения    
cursor.close()
conn.close()

# Блок подсчета занятого времени
current_time_finish = datetime.now()
elapsed_time = current_time_finish - current_time_start
elapsed_minutes = elapsed_time.total_seconds() / 60
elapsed_minutes_formatted = "{:.2f}".format(elapsed_minutes)
print(f"Дата завершения - {current_time_finish}, времени заняло - {elapsed_minutes_formatted} минут")

print(f"Проверка актуальности завершена успешно, сменило статус {changed_status_count} штук, осталось активными {stayed_active_count} штук. Ссылка недоступна у {dead_link_count} штук.")

# Параметры отправки на email
mail_contents = (f"Привет!\nДата начала - {current_time_start}\nДата завершения - {current_time_finish}, времени заняло - {elapsed_minutes_formatted} минут\nВ базе {rows_count} строк\nБекап базы сохранен в файле csv на локальной машине: {backup_file}\nДля проверки отобрано {rows_count_na} строк\nПроверка актуальности завершена успешно, сменило статус {changed_status_count} штук, осталось активными {stayed_active_count} штук. Ссылка недоступна у {dead_link_count} штук.")
recipient = 'lutzby@gmail.com'
subject = 'Результат работы скриптов. №2 Проверка статуса и бекап'
send_email(subject, mail_contents, recipient)
recipient = 'alxsaz@gmail.com'
send_email(subject, mail_contents, recipient)