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

# Чтение json исключений
with open('exceptions.json', encoding="utf8") as file:
    exceptions_json = json.load(file)

exclude_sellers = exceptions_json['exclude_sellers']
exclude_brands = exceptions_json['exclude_brands']

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

cursor = conn.cursor()
# Забираем всю таблицу и НЕ делаем бекап
select_query = "SELECT * from av_full"
cursor.execute(select_query)
rows_full = cursor.fetchall()
rows_count = cursor.rowcount
print(f"Строк в базе: {rows_count}")

# Готовим нужные столбцы и строки
select_query = """
SELECT id, status, status_date, url, price, seller, capacity, cylinders, brand, condition, seller_ph_nr
FROM av_full 
WHERE status IN ('Актуально', 'Временно недоступно', 'На проверке')
ORDER by date desc
"""
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

# легаси функция добавления организации
def set_orgainsation (id_value):
    # Добавление организации
    organization = data['props']['initialState']['advert']['advert'].get('organizationTitle', 'null')
    if organization != 'null':
        organization_query = ("UPDATE av_full SET seller = '%s' WHERE id = %s") % (organization, id_value) 
        cursor.execute(organization_query)
        conn.commit()
        print(f"Organization = {organization}")

# Функция проверки на дубликаты
def check_for_duplicates (id_value):
    # Ищем дубликат по id_value, список не будет содержать строку искомого айди
    select_query = """SELECT id, date, price, model_vlk, brand, model, model_misc, year, type, cylinders, capacity, mileage, duplicate_flag, url
    FROM public.av_full
    WHERE (brand, model, model_misc, year, type, cylinders, capacity, mileage, seller, locations) = 
    (
        SELECT brand, model, model_misc, year, type, cylinders, capacity, mileage, seller, locations
        FROM public.av_full
        WHERE id = %d
    )
    AND id != %d
    ORDER BY date ASC;""" % (id_value, id_value)
    cursor.execute(select_query)
    dupl_rows = cursor.fetchall()
    dupl_count = 0 # счетчик количества объяв-дубликатов
    is_not_yet_marked_as_duplicate = 0 # были ли в выборке уже помеченные как дубликаты объявы

    if dupl_rows:
        dupl_id_list = []
        dupl_date_list = []
    
        for dupl_n in dupl_rows:    
            dupl_dates = dupl_n[1]
            dupl_date_list.append(dupl_dates)
            dupl_id = dupl_n[0]
            dupl_id_list.append(dupl_id)
            #print(f"id - {dupl_id}, дата - {dupl_dates}")
            dupl_count += 1
            if dupl_n[12] is False:
                is_not_yet_marked_as_duplicate += 1

        dupl_date = min(dupl_date_list)
        
        print(f"Найдено {dupl_count} дубликатов для id:{id_value} с самой ранней датой - {dupl_date}")

        # отформатировать список для вставки в квери
        dupl_id_list = ', '.join(str(int(d)) for d in dupl_id_list)

        # квери выставить флажок дубликата
        query1 = """UPDATE public.av_full
        SET duplicate_flag = True, duplicate_id = %d
        WHERE id in (%s);""" % (id_value, dupl_id_list)
        cursor.execute(query1)

        # квери выставить новую дату
        query2 = """UPDATE public.av_full
        SET date_corrected = '%s'
        WHERE id = %d;""" % (dupl_date, id_value)
        cursor.execute(query2)
        
        # записать изменения
        conn.commit()
    if is_not_yet_marked_as_duplicate > 0:
        return 1
    else:
        print(f'Дубликатов для id:{id_value} не обнаружено')
        return 0

# Функция получения и записи номера продавца
def phone_get_request(id_value):
    phone_url = f"https://api.av.by/offers/{id_value}/phones" 
    phone_response = requests.get(phone_url, headers=headers)
    p_to_write = []

    # Проверка успешности
    if phone_response.status_code == 200:
        phone_data = phone_response.json()  # Получаем джсон
        for phone in phone_data:
            p_country = phone['country']['label']
            p_code = phone['country']['code']
            p_number = phone['number']
            p_full_number = f"+{p_code}{p_number}"
            p_to_write.append(p_full_number)
        # пишем
        phone_query = """
        UPDATE av_full
        SET seller_ph_nr = %s
        WHERE id = %s
        """
        cursor.execute(phone_query, (p_to_write, id_value))
        conn.commit()
        print(f"Для ID {id_value} записан(ы) номер(а): {p_to_write}")
        return 1
    else:
        print(f"Ошибка {phone_response.status_code}: {phone_response.text}")
        return 0

# Подсчет итераций
changed_status_count = 0
stayed_active_count = 0
dead_link_count = 0
unchanged_status_count = 0
price_changed_count = 0
price_difference_sum = 0
broken_link_count = 0
duplicates_global_count = 0
phone_writed_counter = 0

# Сам цикл
for row in rows:
    id_value = row[0] # стобец id с индексом 0
    print(f'--------------------------')
    print(f"Обрабатываю ID {id_value}")
    status_value = row[1] # стобец status с индексом 1
    url = row[3] # стобец url с индексом 3
    price_ex = row[4] # столбец price
    seller = row[5]
    capacity = int(row[6])
    cylcount = int(row[7])
    brand = row[8]
    condition = row[9]
    seller_ph_nr = row[10]
    
    # вынимаем свежий флаг
    flag_query = """
    SELECT duplicate_flag
    FROM av_full 
    WHERE id = %s
    """
    cursor.execute(flag_query, (id_value,))
    duplicate_flag = cursor.fetchone()
    duplicate_flag = duplicate_flag[0]
    
    print(f'{brand}, объем: {capacity}, {cylcount} цил, статус: {status_value}, цена: {price_ex}, {seller}, {condition}, дубль: {duplicate_flag}, {url} ')

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
            #Проверяем статус
            ad_status_script = data['props']['initialState']['advert']['advert']['status'] 
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
                if price_diff >0:
                    pd = '+'
                else:
                    pd = ''
                print(f"Изменилась цена для id {id_value} на {pd}{price_diff} USD, price_diff_sum составляет {price_difference_sum} USD")
            else:
                print(f"Цена для id {id_value} осталась прежней")
            
            # ЗДЕСЬ БЫЛО Добавление организации
            
            # Вызов функции проверки на дубликаты 
            if (
                seller not in exclude_sellers
                and int(capacity) >= 299 
                and cylcount > 1 
                and brand not in exclude_brands 
                and duplicate_flag is False 
                and condition != 'новый'
            ):
                duplicates_global_count += check_for_duplicates (id_value)    
            else:
                print(f'Проверка дубликатов для id:{id_value} не проводится')

            #Если табличка закрыто есть
            if ad_status_script != 'active': 
                updated_status = data['props']['initialState']['advert']['advert']['publicStatus']['label']

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

                    print(f"смена статуса для id - {id_value}, на {updated_status}, Дата-{updated_status_date}")
                    changed_status_count += 1 # Крутим счетчик
                else:
                    print(f"статус {status_value} не изменился для id - {id_value}")
                    unchanged_status_count +=1 # Крутим счетчик
                    continue

            else: #Если статус "active" то объява еще актуальная или стала актуальной
                print(f"id - {id_value} актуален")
                update_and_write('Актуально', 'null', id_value)
                stayed_active_count += 1 # Крутим счетчик
                
                # Вызываем функцию поиска номера для активных объяв
                if (
                    int(capacity) >= 299 # это чтобы исключить индурики
                    and cylcount > 1 # это тоже чтобы исключить индурики
                    and brand not in exclude_brands # это совкоциклы и гавно
                    and seller_ph_nr is None  
                ):
                    phone_writed_counter += phone_get_request(id_value)
                else:
                    print(f'Запись номера для id:{id_value} не проводится')
                
                
        # Если страница открылась но она с домиком 404
        except (KeyError, json.JSONDecodeError, TypeError):
            # Обн. базу, уст. статус и стат. дату для соотв id
            print(f"ссылка сдохла для id {id_value}")
            update_and_write('Недоступная ссылка', 'null', id_value)
            dead_link_count += 1

    else: # Если респонс не 200, т.е. страница не прочиталась
        # Обн. базу, уст. статус и стат. дату для соотв id
        update_and_write('Недоступная ссылка', 'null', id_value)
        print(f"ссылка сдохла для id {id_value}")
        dead_link_count += 1
    
    # Диапазон рандомных значений для задержки
    wait_amount = random.randint(4, 7)
    print(f"Ждем {wait_amount} секунд")
    
    time.sleep(wait_amount)

# После обработки получить актуальные данные из базы - дубликаты
select_query = """select count(duplicate_flag)
from av_full
where duplicate_flag is true"""
cursor.execute(select_query)
duplicates_in_db = cursor.fetchone()[0]

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
    не открылась страница у {broken_link_count} штук
Цена изменилась у {price_changed_count} штук
Общее именение цен составило {price_difference_sum} USD
Записано объявлений с дубликатами - {duplicates_global_count} шт.
Всего сейчас в базе дубликатов - {duplicates_in_db} шт.
Записано номеров телефонов - {phone_writed_counter} шт.
Спасибо за внимание <3
"""
)

subject = 'Результат работы скриптов. №2 Проверка статусов'
for recipient in recipients:
    send_email(subject, mail_contents, recipient)