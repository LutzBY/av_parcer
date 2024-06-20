import requests
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import datetime
from datetime import datetime
import json
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import psycopg2
import smtplib
from email.mime.text import MIMEText
from email.utils import COMMASPACE
import sys
import io

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

# Страница для парсинга
url_page = "https://moto.av.by/filter?category_type=1&price_usd[min]=1&condition[0]=1&condition[1]=2&sort=4"
# Страница для парсинга

# Блок сохранения терминала в буфер
output_buffer = io.StringIO()
sys.stdout = output_buffer

current_time_str = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
print(f"Привет! Текущая дата - {current_time_str}")

# Забираем последнюю дату и количество стобцов
datecursor = conn.cursor()
datequery = "SELECT count(id), max(date) from av_full"
datecursor.execute(datequery)
tmpfetch = datecursor.fetchone()
latest_ad_date = tmpfetch[1]
old_rows_count = tmpfetch[0]
print(f"Последняя дата объявления - {latest_ad_date.strftime('%Y-%m-%d-%H-%M-%S')}")
datecursor.close()


# Функция дополнения модели инф-ой из базы знаний по постре
def add_mvlk(brand, model, modification, year, cylcount, capacity, mtype, best_match): 
    model_concat = model
    displacement = float(capacity)
    model_concat += " " + modification
    vlkcursor = conn.cursor()
    query = """
        SELECT model, mtype, id
        FROM vlookup
        WHERE brand = %(brand)s
            AND year = %(year)s
            AND cylinders = %(cyl)s
            AND displacement_ccm BETWEEN %(min_displacement)s AND %(max_displacement)s
    """
    params = {
        'brand': str(brand).lower(),
        'year': year,
        'cyl': cylcount,
        'min_displacement': displacement - 53,
        'max_displacement': displacement + 49
    }
    vlkcursor.execute(query, params)
    rows = vlkcursor.fetchall()

    match_ratio = []
    best_match_list = []
    best_match = ""
    best_ratio = 0
    for row in rows:
        model_found = row[0]
        mtype_found = row[1]
        vlk_id = row[2]
    
        if brand == 'BMW':
            model_comp = fuzz.token_set_ratio(model_concat.lower(), model_found.lower())
            match_ratio.append(model_comp)
            best_match_list.append(model_found)
            
        else:
            if mtype_found == mtype:
                model_comp = fuzz.token_set_ratio(model_concat.lower(), model_found.lower())
                match_ratio.append(model_comp * 1.25)
                best_match_list.append(model_found)
                
            else:
                model_comp = fuzz.token_set_ratio(model_concat.lower(), model_found.lower())
                match_ratio.append(model_comp)
                best_match_list.append(model_found)
                
        model_ratio_list = list(zip(best_match_list, match_ratio))
        
        best_model, best_ratio = max(model_ratio_list, key=lambda x: x[1])
        best_match = best_model
        
    vlkcursor.close()
    return best_match

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

#цикл перебора страниц и парсинг
page_counter = 0
processed_ads = 0

# запуск курсора
parsecursor = conn.cursor()

stop_flag = False

while stop_flag == False:
    page_counter +=1
    url_cycle = url_page + "&page=" + str(page_counter)
    print(f"Страница - {page_counter} ---------------------------------------")
    #Функция
    response = requests.get(url_cycle, headers=headers)

    src = response.text
    soup = BeautifulSoup(src, 'lxml')
    #Проверка на доступность страницы
    if response.status_code != 200:
        print(f"Случилась ошибка загрузки этой {url_cycle} страницы :(  Код такой:", response.status_code)
        break

    script_element = soup.find("script", id="__NEXT_DATA__") #Достаем жсон
    #Проверка имеется ли жсон на странице
    if script_element is None:
        print("Элемент script не найден :(")
        break

    json_string = script_element.string #Конвертируем жсон в стринг
    data = json.loads(json_string) #Пакуем в data

    #Пустые хранилища
    ids = []
    prices = []
    published = []
    refreshed = []
    brands = []
    models = []
    modifications = []
    years = []
    mtypes = []
    cylcounts = []
    drivetypes = []
    capacitys = []
    mileages = []
    urlss = []
    locations = []
    sellers = []

    for item in data['props']['initialState']['filter']['main']['adverts']: # Цикл поиска в этом словаре этих ключей
        advert_id = item['id']
        price = item['price']['usd']['amount']
        published_at = item['publishedAt']
        refreshed_at = item['refreshedAt']
        properties = item['properties']
        public_url = item['publicUrl']
        location = item['locationName']
        seller = item['sellerName']
        
        # Искать нужные свойства по имени тега внутри пропертис
        brand = next((prop['value'] for prop in properties if prop['name'] == 'brand'), None)
        model = next((prop['value'] for prop in properties if prop['name'] == 'model'), None)
        modification = next((prop['value'] for prop in properties if prop['name'] == 'modification'), "")
        year = next((prop['value'] for prop in properties if prop['name'] == 'year'), "0")
        mtype = next((prop['value'] for prop in properties if prop['name'] == 'purpose_type'), None)
        cylcount = next((prop['value'] for prop in properties if prop['name'] == 'cylinder_number'), "0")
        drivetype = next((prop['value'] for prop in properties if prop['name'] == 'main_gear'), None)
        capacity = next((prop['value'] for prop in properties if prop['name'] == 'engine_capacity'), "0")
        mileage = next((prop['value'] for prop in properties if prop['name'] == 'mileage_km'), "0")

        # Апендить в хранилища
        ids.append(advert_id)
        prices.append(price)
        published.append(published_at)
        refreshed.append(refreshed_at)
        brands.append(brand)
        models.append(model)
        modifications.append(modification)
        years.append(year)
        mtypes.append(mtype)
        cylcounts.append(cylcount)
        drivetypes.append(drivetype)
        capacitys.append(capacity)
        mileages.append(mileage)
        urlss.append(public_url)
        locations.append(location)
        sellers.append(seller)
    
    for id, price, publish, refresh, brand, model, modification, year, mtype, cylcount, drivetype, capacity, mileage, url, location, seller in zip(ids, prices, published, refreshed, brands, models, modifications, years, mtypes, cylcounts, drivetypes, capacitys, mileages, urlss, locations, sellers):
        datetime_obj = datetime.strptime(publish, '%Y-%m-%dT%H:%M:%S%z') # Преобразования текстового значения в дату
        datetime_obj = datetime_obj.replace(tzinfo=None) # убираем таймзон

        rdatetime_obj = datetime.strptime(refresh, '%Y-%m-%dT%H:%M:%S%z') # Преобразования текстового значения в дату
        rdatetime_obj = rdatetime_obj.replace(tzinfo=None) # убираем таймзон
        publish_for_print = datetime_obj.strftime('%d.%m.%y %H:%M')
        refresh_for_print = rdatetime_obj.strftime('%d.%m.%y %H:%M')
        seller = seller.replace("'", ".")
        modification = modification.replace("'", ".")
                
        # Вызов функции дополнения vlk
        best_match = None
        best_match = add_mvlk(brand, model, modification, year, cylcount, capacity, mtype, best_match) 

        # Скрипт для пгри
        parsequery = """
            INSERT INTO av_full(id, price, date, brand, model, model_misc, year, type, cylinders, drive, capacity, mileage, url, locations, status, status_date, model_vlk, seller)
            VALUES ( %s, %s, '%s', '%s', '%s', '%s', %s, '%s', %s, '%s', %s, %s, '%s', '%s', 'Актуально', null, '%s', '%s')
            ON CONFLICT (id) DO UPDATE 
            SET 
            price = excluded.price
            WHERE av_full.id = excluded.id;
        """ % (id, price, datetime_obj, brand, model, modification, year, mtype, cylcount, drivetype, capacity, mileage, url, location, best_match, seller)
        
        # Работа курсора для пгри
        parsecursor.execute(parsequery)
        
        # Вынять влк из базы
        vlkquery = " SELECT model_vlk FROM av_full WHERE id = %s " % (id)
        parsecursor.execute(vlkquery)
        vlkfetch = parsecursor.fetchone()
        mvlk_actual = vlkfetch[0]
        
        # Сверка даты
        if rdatetime_obj <= latest_ad_date: 
            stop_flag = True
            break
        processed_ads += 1
        
# Основной принт (для маленьких сокращенный)
        print(f"-----------------------------------------------------------------")
        if int(capacity) >= 299 and cylcount > 1:
            print (f"""
№ {processed_ads}, Price - {price}, ID - {id}
Publ. at {publish_for_print}, Refr. at {refresh_for_print}
Name - {brand} {model} {modification} ({year}, {capacity} ccm)
Actual mvlk - {mvlk_actual} (Best mvlk - {best_match})
Seller - {seller}
URL - {url}""")
        
        else:
            print (f"""
№ {processed_ads}, Price - {price}, ID - {id}
Name - {brand} {model} {modification} ({year}, {capacity} ccm)
URL - {url}""")


parsecursor.close()
print(f"---- ВЫВОД ------------------------------------------------------")
print(f"Все хорошо! Прошел {page_counter} страниц, в старой базе было {old_rows_count} строк. Обработано {processed_ads} объявлений!")

# Восстанавливаем исходный вывод терминала
terminal_output = output_buffer.getvalue()
sys.stdout = sys.__stdout__
print (terminal_output)

# Делаем дамп терминала
with open('terminal_output.txt', 'w') as file:
    # Запись содержимого переменной в файл
    file.write(terminal_output)

# Параметры отправки на email
subject = 'Результат работы скриптов. №1 Парсинг и апдейт modelvlk'
for recipient in recipients:
    send_email(subject, terminal_output, recipient)

# Записать-закрыть курсор
conn.commit()
conn.close()