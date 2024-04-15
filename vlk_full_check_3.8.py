import requests
from urllib.parse import urlencode
import re
import pandas as pd
from bs4 import BeautifulSoup
import time
import datetime
from datetime import datetime
import json
import datetime
import os
import requests
from tqdm import tqdm
from requests.exceptions import SSLError
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import psycopg2

#Подключение к postgres
conn = psycopg2.connect(
    host="root.alxsaz.net",
    port = "443",
    database="testdb",
    user="user",
    password="KuAxX6zP4ZCC4Kt"
)

excel_file = "av_rq_full.xlsx"
df_actual = pd.read_excel(excel_file)

df = pd.DataFrame(columns=["!Model_VLK"])

for index, row in tqdm(df_actual.iterrows(), total=len(df_actual)):
    brand_row = row['Brand']
    brand_row_lower = str(brand_row).lower()
    year_row = row['Year']
    displacement_row = int(row['Capacity'])
    model_row = row['Model']
    cyl_row = row['Cylinders']
    mtype_row = row['Type']
    model_misc = row['Model misc']
    model_concat = model_row
    if model_misc and pd.notnull(model_misc): # Если model_misc пуста
        model_concat += " " + model_misc
    
    id_row = row['ID']
    mvlk_row = row['!Model_VLK']

    if pd.isna(mvlk_row):
        cursor = conn.cursor()
        query = """
            SELECT model, mtype
            FROM vlookup
            WHERE brand = %(brand)s
                AND year = %(year)s
                AND cylinders = %(cyl)s
                AND displacement_ccm BETWEEN %(min_displacement)s AND %(max_displacement)s
        """
        params = {
            'brand': brand_row_lower,
            'year': year_row,
            'cyl': cyl_row,
            'min_displacement': displacement_row - 53,
            'max_displacement': displacement_row + 49
        }
        cursor.execute(query, params)
        rows = cursor.fetchall()

        match_ratio = []
        best_match_list = []
        best_match = ""
        for row in rows:
            model_found = row[0]
            mtype_found = row[1]
                
            if mtype_found == mtype_row:
                model_comp = fuzz.WRatio(model_concat.lower(), model_found.lower())
                match_ratio.append(model_comp + 50)
                best_match_list.append(model_found)
            else:
                model_comp = fuzz.WRatio(model_concat.lower(), model_found.lower())
                match_ratio.append(model_comp)
                best_match_list.append(model_found)
            model_ratio_list = list(zip(best_match_list, match_ratio))
        
            best_model, best_ratio = max(model_ratio_list, key=lambda x: x[1])
            best_match = best_model
        df_actual.loc[index, "!Model_VLK"] = best_match
    else:
        continue
    cursor.close()
df_actual.to_excel(excel_file, index=False)
conn.close()
