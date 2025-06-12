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
from requests.exceptions import ChunkedEncodingError, RequestException
import time

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

# Чтение json исключений
with open('exceptions.json', encoding="utf8") as file:
    exceptions_json = json.load(file)

exclude_locations = exceptions_json['exclude_locations']
exclude_brands = exceptions_json['exclude_brands']

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

    message = MIMEText(body, 'html')
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

# Функция "ручной" проверки дубликатов
def duplicates_manual_check(brand, model, year, mtype, cylcount, capacity, seller, location, mileage):
    duplcursor = conn.cursor()
    query = """
        SELECT id, url, duplicate_flag
        FROM public.av_full
        WHERE brand = %(brand)s
          AND model = %(model)s
          AND year = %(year)s
          AND type = %(type)s
          AND cylinders = %(cylinders)s
          AND capacity BETWEEN %(min_capacity)s AND %(max_capacity)s
          AND LOWER(seller) = %(seller)s
          AND locations = %(location)s
          AND mileage BETWEEN %(min_mileage)s AND %(max_mileage)s
        AND id != %(id)s
        ORDER BY date ASC;
    """
    params = {
        'id': id,
        'min_mileage': int(mileage) * 0.7,
        'max_mileage': int(mileage) * 1.3,
        'min_capacity': int(capacity) * 0.9,
        'max_capacity': int(capacity) * 1.1,
        'brand': brand, 
        'model': model,
        'year': year,
        'type': mtype,
        'cylinders': cylcount,
        'seller':seller.lower(),
        'location': location
    }
    duplcursor.execute(query, params)
    dmc_results = duplcursor.fetchall()

    duplcursor.close()

    return dmc_results

# Создание HTML отчета
html_mail_contents = f"""
    <html>
    <body>
    <h2 style="text-align: center;">Привет! Отчет скрипта парсинга на дату:</h2>
    <h4 style="text-align: center;">{current_time_str}</h4>
    <h2 style="text-align: center;">Последняя дата объявления из базы:</h2>
    <h4 style="text-align: center;">{latest_ad_date.strftime('%Y-%m-%d-%H-%M-%S')}</h4>
    <hr />
    <p>&nbsp;</p>
    </body>
    </html>
    """

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
    html_mail_contents += f"""
<h2><strong>Страница {page_counter}</strong></h2>
<hr />
<p>&nbsp;</p>
<p>&nbsp;</p>
"""
    #Функция парсинга страницы page
    response_attempt = 0
    max_response_retries = 10
    error_counter = 0
    for response_attempt in range(max_response_retries):
        try:
            response = requests.get(url_cycle, headers=headers)
            src = response.text
            soup = BeautifulSoup(src, 'lxml')
            script_element = soup.find("script", id="__NEXT_DATA__") #Достаем жсон
            break
        except ChunkedEncodingError or RequestException or json.JSONDecodeError or script_element is None or response.status_code != 200:
            print(f"Ошибка при чтении страницы. Попытка {response_attempt + 1} из {max_response_retries} не удалась. Повтор через 10 секунд.")
            time.sleep(10)
            error_counter += 1
        except:
            print(f'Непредвиденная ошибка при чтении страницы {url_cycle}')
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
    img_srcs = []
    conditions = []
    flags_on_order = []
    user_descriptions = []
    sellers_ids = []

    for item in data['props']['initialState']['filter']['main']['adverts']: # Цикл поиска в этом словаре этих ключей
        advert_id = item['id']
        price = item['price']['usd']['amount']
        published_at = item['publishedAt']
        refreshed_at = item['refreshedAt']
        properties = item['properties']
        public_url = item['publicUrl']
        location = item['locationName']
        seller = item['sellerName']
        if len(item['photos']) > 0:
            pic = item['photos'][0]['medium']['url'] #small?
        else:
            pic = 'https://commons.wikimedia.org/wiki/File:No_Image_Available.jpg' 
        condition = item['metadata']['condition']['label']
        flag_on_order = item['metadata']['onOrder']
        user_description = item.get('description', '')
        seller_id = item.get('organizationId', 'null')

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
        img_srcs.append(pic)
        conditions.append(condition)
        flags_on_order.append(flag_on_order)
        user_descriptions.append(user_description)
        sellers_ids.append(seller_id)
    
    for id, price, publish, refresh, brand, model, modification, year, mtype, cylcount, drivetype, capacity, mileage, url, location, seller, img_src, condition, flag_on_order, user_description, seller_id in zip(ids, prices, published, refreshed, brands, models, modifications, years, mtypes, cylcounts, drivetypes, capacitys, mileages, urlss, locations, sellers, img_srcs, conditions, flags_on_order, user_descriptions, sellers_ids):
        datetime_obj = datetime.strptime(publish, '%Y-%m-%dT%H:%M:%S%z') # Преобразования текстового значения в дату
        datetime_obj = datetime_obj.replace(tzinfo=None) # убираем таймзон

        rdatetime_obj = datetime.strptime(refresh, '%Y-%m-%dT%H:%M:%S%z') # Преобразования текстового значения в дату
        rdatetime_obj = rdatetime_obj.replace(tzinfo=None) # убираем таймзон
        publish_for_print = datetime_obj.strftime('%d.%m.%y %H:%M')
        refresh_for_print = rdatetime_obj.strftime('%d.%m.%y %H:%M')
        seller = seller.replace("'", ".")
        modification = modification.replace("'", ".")
        user_description = user_description.replace("'", ".")
                
        # Вызов функции дополнения vlk
        best_match = None
        if mtype != 'кастом':
            best_match = add_mvlk(brand, model, modification, year, cylcount, capacity, mtype, best_match)
        else:
            best_match = 'кастом'

        # Проверка необходимости установить exclude_flag
        exclude_flag = False
        if flag_on_order:
            exclude_flag = True
        if location in exclude_locations:
            exclude_flag = True
        
        # Запуск функции проверки дубликатов
        duplicate_html_block = f"""<p style="text-align: center;"><strong>Не найдено дубликатов</strong></p>""" # Формирование базового блока в хтмл "нет дублей"
        if (
            int(capacity) >= 299
            and cylcount > 1
            and brand not in exclude_brands
            and condition != 'новый'
        ):
            # Получение выгрузки потенциальных дубликатов и разбивка их на старые и новые
            dmc_results = duplicates_manual_check(brand, model, year, mtype, cylcount, capacity, seller, location, mileage)
            dmc_is_duplicate = []
            dmc_not_duplicate = []
            for i in dmc_results:
                if i[2] is True:
                    dmc_is_duplicate.append(i)
                else:
                    dmc_not_duplicate.append(i)
        else:
            dmc_is_duplicate = []
            dmc_not_duplicate = []
            
        # Скрипт для пгри
        parsequery = """
            INSERT INTO av_full(id, price, date, brand, model, model_misc, year, type, cylinders, drive, capacity, mileage, url, locations, status, status_date, model_vlk, seller, condition, exclude_flag, user_description, seller_id)
            VALUES (%s, %s, '%s', '%s', '%s', '%s', %s, '%s', %s, '%s', %s, %s, '%s', '%s', 'Актуально', null, '%s', '%s', '%s', %s, '%s', %s)
            ON CONFLICT (id) DO UPDATE 
            SET 
            price = excluded.price
            WHERE av_full.id = excluded.id;
        """ % (id, price, datetime_obj, brand, model, modification, year, mtype, cylcount, drivetype, capacity, mileage, url, location, best_match, seller, condition, exclude_flag, user_description, seller_id)
        
        # Работа курсора для пгри
        parsecursor.execute(parsequery)
        
        # Вынять влк и эксклюд флаш из базы
        vlkquery = " SELECT model_vlk, exclude_flag FROM av_full WHERE id = %s " % (id)
        parsecursor.execute(vlkquery)
        vlkfetch = parsecursor.fetchone()
        mvlk_actual = vlkfetch[0]
        exclude_flag_actual = vlkfetch[1]
        
        # Вынять средние цены по влк из базы
        prices_a_query = """
            SELECT AVG(price) as price_a, COUNT(*) as price_a_count 
            FROM av_full 
            WHERE model_vlk = '%s'
            AND status = 'Актуально'
            AND duplicate_flag is FALSE
            AND exclude_flag is FALSE""" % (mvlk_actual)
        prices_f_query = """
            SELECT AVG(price) as price_a, COUNT(*) as price_a_count 
            FROM av_full 
            WHERE model_vlk = '%s'
            AND duplicate_flag is FALSE
            AND exclude_flag is FALSE""" % (mvlk_actual)
        price_year_query = """
            SELECT AVG(price) as price_y
            FROM av_full
            WHERE model_vlk = '%s'
            AND year = %s
            AND duplicate_flag is FALSE
            AND exclude_flag is FALSE""" % (mvlk_actual, year)

        parsecursor.execute(prices_a_query)
        price_a_result = parsecursor.fetchone()

        parsecursor.execute(prices_f_query)
        price_f_result = parsecursor.fetchone()

        parsecursor.execute(price_year_query)
        price_year_result = parsecursor.fetchone()

        # вынимаем цены и количество вхождений по влк
        price_a = price_a_result[0] if price_a_result[0] is not None else None
        price_a_count = price_a_result[1] if price_a_result[1] is not None else None

        price_f = price_f_result[0] if price_f_result[0] is not None else None
        price_f_count = price_f_result[1] if price_f_result[1] is not None else None

        price_y = price_year_result[0] if price_f_result[0] is not None else None

        if price_a is not None and mvlk_actual not in (None, '', ' '):
            price_a = int(price_a)
            price_f = int(price_f)
            price_dif_fr_act = price - price_a
            price_dif_fr_full = price - price_f
            price_color_a = "#ff9900" if price_dif_fr_act > 0 else "#99cc00"
            price_color_f = "#ff9900" if price_dif_fr_full > 0 else "#99cc00"
        elif price_f is not None and mvlk_actual not in (None, '', ' '):
            price_a = '-'
            price_dif_fr_act = '-'
            price_f = int(price_f)
            price_dif_fr_full = price - price_f
            price_color_a = "#9e9e9e"
            price_color_f = "#ff9900" if price_dif_fr_full > 0 else "#99cc00"
        else:
            price_a = '-'
            price_f = '-'
            price_dif_fr_act = '-'
            price_dif_fr_full = '-'
            price_color_a = "#9e9e9e"
            price_color_f = "#9e9e9e"
        
        if price_y is not None and mvlk_actual not in (None, '', ' '):
            price_y = int(price_y)
            price_diff_y_act = price - price_y
            price_color_y = "#ff9900" if price_diff_y_act > 0 else "#99cc00"
        else:
            price_y = '-'
            price_color_y = "#9e9e9e"

        # Сверка даты
        if rdatetime_obj <= latest_ad_date: 
            stop_flag = True
            break
        processed_ads += 1

        # Дополнение хтмл отчета строчкой про дубликаты если они есть       
        if len(dmc_not_duplicate or dmc_is_duplicate) > 0:
            duplicate_html_block = f"""<p style="text-align: center;"><strong>Найдены дубликаты (<span style="color: #ff6600;">вероятные</span> / <span style="color: #008000;">старые</span>):</strong></p>"""
            # Блок дубликатов которые duplicate = False
            for idx, x in enumerate(dmc_not_duplicate):
                m_d_id = x[0]
                m_d_url = x[1]
                duplicate_html_block += f"""<a href="{m_d_url}" style="color: #ff6600;">{m_d_id}</a>"""
                # Добавляем запятую, если это не последний элемент из обоих списков
                if idx != len(dmc_not_duplicate) - 1 or len(dmc_is_duplicate) > 0:
                    duplicate_html_block += ", "

            # Блок дубликатов которые duplicate = True
            for idx, x in enumerate(dmc_is_duplicate):
                m_d_id = x[0]
                m_d_url = x[1]
                duplicate_html_block += f"""<a href="{m_d_url}" style="color: #008000;">{m_d_id}</a>"""
                # Добавляем запятую, если это не последний элемент
                if idx != len(dmc_is_duplicate) - 1:
                    duplicate_html_block += ", "

        # Принт объявы и дополнение HTML contents (для маленьких сокращенный)
        print(f"-----------------------------------------------------------------")
        if int(capacity) >= 299 and cylcount > 1 and brand not in exclude_brands:
            print (f"""
№ {processed_ads}, Price - {price}, ID - {id}
Publ. at {publish_for_print}, Refr. at {refresh_for_print}
Name - {brand} {model} {modification} ({year}, {capacity} ccm)
Actual mvlk - {mvlk_actual} (Best mvlk - {best_match})
Seller - {seller}
URL - {url}""")
        
            # Дополнение большим HTML каждого объявления
            html_mail_contents += f"""
<html>
        <body>
<table style="width: 950px; height: 160px;" border="1">
<tbody>
<tr style="height: 10px;">
<td style="width: 484.953px; height: 10px;">
<p style="text-align: left;"><strong>№ {processed_ads}</strong></p>
<p style="text-align: left;">{id}</p>
</td>
<td style="width: 108.281px; height: 10px;">
<p style="text-align: center;">{year} г.в.&nbsp;</p>
</td>
<td style="width: 173.438px; height: 10px; text-align: center;" colspan="3">
<p style="text-align: center;"><a href="{url}"> <strong>{brand} {model} {modification}</strong></a></p>
</td>
</tr>
<tr style="height: 35px;">
<td style="width: 484.953px; height: 78px;" rowspan="3"><img src="{img_src}" alt="" /></td>
<td style="width: 108.281px; height: 35px;">
<p style="text-align: left;"><strong>Актуальное влк</strong></p>
<p style="text-align: left;">{mvlk_actual}</p>
</td>
<td style="width: 173.438px; height: 35px;" colspan="3">
<p>{mtype}</p>
<p>{cylcount} цилиндров</p>
<p>{capacity} см3</p>
<p>{mileage} км.</p>
<p>{condition}</p>
<p>Флаг exclude - <strong>{exclude_flag_actual}</strong></p>
</td>
</tr>
<tr style="height: 33px;">
<td style="width: 108.281px; height: 33px;">
<p style="text-align: left;"><strong>Лучшее влк</strong></p>
<p style="text-align: left;">{best_match}</p>
</td>
<td style="width: 173.438px; height: 33px;" colspan="3">
<p style="text-align: center;"><strong>Продавец</strong></p>
<p style="text-align: center;">{seller}</p>
<p style="text-align: center;"><strong>Локация</strong></p>
<p style="text-align: center;">{location}</p>
{duplicate_html_block}
</td>
</tr>
<tr style="height: 10px;">
<td style="height: 10px; text-align: center; width: 287.719px;" colspan="4">
<blockquote><strong>Дата апдейта&nbsp;</strong>{refresh_for_print}</blockquote>
<strong>Дата подачи </strong>{publish_for_print}</td>
</tr>
<tr style="height: 62px;">
<td style="text-align: center; height: 62px; width: 484.953px;">
<strong>Ценовая статистика согласно актуальному vlk:</strong><br/>Средняя по актуальным = {price_a} ({price_a_count} шт.), разница = <span style="color: {price_color_a};">{price_dif_fr_act}</span><br/>Средняя за все время = {price_f} ({price_f_count} шт.), разница = <span style="color: {price_color_f};">{price_dif_fr_full}</span>
</td>
<td style="text-align: center; width: 108.281px;" colspan="4"><strong>{price} USD</strong><br />Цена по vlk с таким же годом: <span style="color: {price_color_y};"><br />{price_y} USD</span></td>
</tr>
</tbody>
</table>
<p>&nbsp;</p>
        </body>
        </html>
        """
        else: # сокращенный для мопедов и совкоциклов
            print (f"""
№ {processed_ads}, Price - {price}, ID - {id}
Name - {brand} {model} {modification} ({year}, {capacity} ccm)
URL - {url}""")
            # Дополнение сокращенным HTML каждого объявления
            html_mail_contents += f"""<table style="width: 950px; height: 80px;" border="1">
<tbody>
<tr style="height: 10px;">
<td style="height: 10px; width: 262px;">
<p style="text-align: left;"><strong>№ {processed_ads}</strong></p>
<p style="text-align: left;">{id}</p>
</td>
<td style="width: 137px;" colspan="2">
<p style="text-align: center;"><a href="{url}"><strong>{brand} {model} {modification}</strong></a></p>
</td>
<td style="width: 128px;">
<p style="text-align: center;"><strong>{year} г.в.&nbsp;</strong></p>
</td>
<td style="width: 96px;">
<p style="text-align: center;"><strong>{price} USD</strong></p>
</td>
</tr>
</tbody>
</table>
<div class="jfk-bubble gtx-bubble" style="visibility: visible; left: -88px; top: 158px; opacity: 1;">&nbsp;</div>"""
        
## Завершающий блок
parsecursor.close()
print(f"---- ВЫВОД ------------------------------------------------------")
print(f"Все хорошо! Прошел {page_counter} страниц, в старой базе было {old_rows_count} строк. Обработано {processed_ads} объявлений! Ошибок - {error_counter}")

# Дополнение HTML хвостом
html_mail_contents += f"""<hr />
<h2 style="text-align: center;">КОНЕЦ ОТЧЕТА</h2>
<h4 style="text-align: center;">Все хорошо! Прошел {page_counter} страниц, в старой базе было {old_rows_count} строк. Обработано {processed_ads} объявлений! Ошибок при открытии страницы - {error_counter}</h4>"""

# Параметры отправки на email
subject = 'Результат работы скриптов. №1 Парсинг и апдейт modelvlk'
for recipient in recipients:
    send_email(subject, html_mail_contents, recipient)

# Записать-закрыть курсор
conn.commit()
conn.close()