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
import decimal
##########################
id_to_check = 109534875
#########################
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

#Подключение к postgres
conn = psycopg2.connect(
    host = pgre_host,
    port = pgre_port,
    database = pgre_db,
    user = pgre_login,
    password = pgre_password
)
cursor = conn.cursor()
query = """
    SELECT brand, model, model_misc, year, cylinders, capacity, type
    FROM av_full
    WHERE id IN (%s);
""" % id_to_check
cursor.execute(query)
row = cursor.fetchall()

# Функция дополнения модели инф-ой из базы знаний по постре
brand, model, modification, year, cylcount, capacity, mtype = row[0]
if modification:
    model_concat = model + " " + modification
else:
    model_concat = model
print(model_concat)
displacement = float(capacity)

cursor.close()

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
enumerate = 0
for row in rows:
    model_found = row[0]
    mtype_found = row[1]
    vlk_id = row[2]

    if brand == 'BMW':
        model_comp = fuzz.token_set_ratio(model_concat.lower(), model_found.lower())
        match_ratio.append(model_comp)
        best_match_list.append(model_found)
        print (f"{enumerate}----------------")
        print (f"{row}, - model, ratio - {model_comp}")
        enumerate +=1
    else:
        if mtype_found == mtype:
            model_comp = fuzz.token_set_ratio(model_concat.lower(), model_found.lower())
            match_ratio.append(model_comp * 1.2)
            best_match_list.append(model_found)
            print (f"{enumerate}----------------")
            print (f"{row}, - model, ratio - {model_comp *1.2}")
            enumerate +=1
            
        else:
            model_comp = fuzz.token_set_ratio(model_concat.lower(), model_found.lower())
            match_ratio.append(model_comp)
            best_match_list.append(model_found)
            print (f"{enumerate}----------------")
            print (f"{row}, - model, ratio - {model_comp}")
            enumerate +=1
    model_ratio_list = list(zip(best_match_list, match_ratio))
    
    best_model, best_ratio = max(model_ratio_list, key=lambda x: x[1])
    best_match = best_model


print (f"----------------")    
print (f"{brand} {model} {modification}, {year}, d - {capacity}, c - {cylcount}, t - {mtype}, BM - {best_match}")
vlkcursor.close()

conn.close