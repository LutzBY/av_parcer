import requests
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import datetime
from datetime import datetime
import json
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import psycopg2
from psycopg2.extras import RealDictCursor
import smtplib
from email.mime.text import MIMEText
from email.utils import COMMASPACE
from requests.exceptions import ChunkedEncodingError, RequestException
import time
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

company_cursor = conn.cursor()
cursor = conn.cursor()

def parse_new_organisations(seller_id_list):
    try:
        for id in seller_id_list:
            org_url = f"https://api.av.by/organizations/{id}"
            org_response = requests.get(org_url, headers=headers)
            
            # Проверка успешности
            if org_response.status_code == 200:
                item = org_response.json()  # Получаем джсон
                o_phone = []
                for i in item['infoPhones']:
                    o_phone.append(f'+{i['phone']['country']['code']}{i['phone']['number']}')  #375291112121

                o_creation = item['infoPhones'][0]['createdAt']
                o_creation = datetime.strptime(o_creation, '%Y-%m-%dT%H:%M:%S%z') # Преобразования текстового значения в дату
                o_creation = o_creation.replace(tzinfo=None) # убираем таймзон

                o_region = item['region']['label'] # Минская область
                o_city = item['city']['locationName'] # Минск
                o_legal_address = item['legalAddress'] # г. Минск, Долгиновский тракт, 186
                o_legal_address = o_legal_address.replace("'", ".")
                o_id = id
                o_title = item['title'] # ООО «ДрайвМоторс»
                o_title = o_title.replace("'", ".")
                o_legal_name = item['legalName'] # ООО «ДрайвМоторс»
                o_legal_name = o_legal_name.replace("'", ".")
                o_unp = item['unp'] # 191111259
                o_url = item.get('siteUrl', None)
                print(o_title, o_phone)
                # Кверя
                parsequery = """
                    INSERT INTO av_organizations(id, date_created, title, legal_name, unp_num, phone, region, city, legal_address, url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """
                # Работа курсора для пгри
                company_cursor.execute(parsequery, (o_id, o_creation, o_title, o_legal_name, o_unp, o_phone, o_region, o_city, o_legal_address, o_url))
                conn.commit()
                print(f'{o_id} записан')
    # Если страница открылась но она с домиком 404
    except (KeyError, json.JSONDecodeError, TypeError):
        print('Произошла ошибка открытия страницы')

# Проверка на новые юрлица
select_query = """
select distinct(seller_id)
from av_full af
left join av_organizations ao
on af.seller_id = ao.id
where seller_id is not null
and legal_name is null
"""
cursor.execute(select_query)

new_companies = cursor.fetchall()
if new_companies:
    new_companies_print = f'Найдено {len(new_companies)} новых юрлиц'
    seller_id_list = [int(c[0]) for c in new_companies]
    parse_new_organisations(seller_id_list)
else:
    new_companies_print = 'Нет новых юрлиц'

conn.close()