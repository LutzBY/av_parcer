from urllib.parse import urlencode
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import psycopg2
from email.mime.text import MIMEText
from email.utils import COMMASPACE
import json

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
# Отбираем строки с пустыми влк
cursor = conn.cursor()
query = """
    SELECT brand, model, model_misc, year, cylinders, capacity, type, id, model_vlk
    FROM av_full
    where model_vlk = 'z 1000'
    and brand = 'Kawasaki';
"""
cursor.execute(query)
row = cursor.fetchall()
cursor.close()

# Функция дополнения модели инф-ой из базы знаний по постре
for brand, model, modification, year, cylcount, capacity, mtype, id, mvlk in row:
    if modification:
        model_concat = model + " " + modification
    else:
        model_concat = model

    print(brand, model, modification, year, cylcount, capacity, mtype, id, mvlk) 

    displacement = float(capacity)

    vlkcursor = conn.cursor()
    query = """
        SELECT model, mtype
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
        
        if mtype_found == mtype:
            model_comp = fuzz.WRatio(model_concat.lower(), model_found.lower())
            match_ratio.append(model_comp + 30)
            best_match_list.append(model_found)
            print (f"row {row}, ratio - {model_comp + 30}")
        else:
            model_comp = fuzz.WRatio(model_concat.lower(), model_found.lower())
            match_ratio.append(model_comp)
            best_match_list.append(model_found)
            print (f"row {row}, ratio - {model_comp}")
        model_ratio_list = list(zip(best_match_list, match_ratio))
        
        best_model, best_ratio = max(model_ratio_list, key=lambda x: x[1])
        best_match = best_model


        if best_match !="":
            insertcursor = conn.cursor()
            insertquery = """
                UPDATE av_full
                SET model_vlk = %s
                WHERE id = %s
            """
            insertcursor.execute(insertquery, (best_match, id))
            insertcursor.close()
        else:
            continue
           
    print(F"MC - {model_concat}")
    print(F"BM - {best_match}")
    print(f"-----------")
    vlkcursor.close()

conn.commit()
conn.close()