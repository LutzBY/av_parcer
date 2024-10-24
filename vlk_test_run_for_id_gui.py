##########################
#id_to_check = 111355795

#########################

import json
from fuzzywuzzy import fuzz
import psycopg2
import pyperclip
import tkinter as tk
from tkinter import messagebox
import sys

# Функция для копирования текста в буфер обмена
def copy_to_clipboard(text):
    root.clipboard_clear()  # Очистка буфера обмена
    root.clipboard_append(text)  # Добавление текста в буфер обмена
    root.update()  # Необходимо для обновления буфера обмена
    # messagebox.showinfo("Информация", f"'{text}' скопировано в буфер обмена.")
    root.destroy()


id_to_check = pyperclip.paste()

#Проверка на айди
if not id_to_check.isdigit():
    print('В буфере не ID')
    sys.exit()

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
enumerat = 0
results = []

for row in rows:
    model_found = row[0]
    mtype_found = row[1]
    vlk_id = row[2]

    if brand == 'BMW':
        model_comp = fuzz.token_set_ratio(model_concat.lower(), model_found.lower())
        match_ratio.append(model_comp)
        best_match_list.append(model_found)
        enumerat +=1
    else:
        if mtype_found == mtype:
            model_comp = fuzz.token_set_ratio(model_concat.lower(), model_found.lower())
            match_ratio.append(model_comp * 1.2)
            best_match_list.append(model_found)
            enumerat +=1
            
        else:
            model_comp = fuzz.token_set_ratio(model_concat.lower(), model_found.lower())
            match_ratio.append(model_comp)
            best_match_list.append(model_found)
            enumerat +=1
    model_ratio_list = list(zip(best_match_list, match_ratio))

    # Пример данных (замени на свои реальные результаты поиска)
    results.append({"№": enumerat,"name": model_found, "type": mtype_found, "ratio": model_comp, "vlk_id": vlk_id})
    
    print(f"""
{enumerat} --------
{model_found}
{mtype_found}
ratio = {model_comp}, vlk_id = {vlk_id}""")
    
    best_model, best_ratio = max(model_ratio_list, key=lambda x: x[1])
    best_match = best_model


print (f"----------------")    
print (f"{brand} {model} {modification}, {year}, d - {capacity}, c - {cylcount}, t - {mtype}, BM - {best_match}")
vlkcursor.close()

conn.close

# Создаем основное окно
root = tk.Tk()
root.title(f"{brand} {model} {modification}, {year}, d - {capacity}, c - {cylcount}, t - {mtype}")

# Создаем текстовые метки и кнопки для каждого результата
for idx, result in enumerate(results, 1):
    # Отображаем текстовую информацию о каждом элементе
    text = f"{idx} --------\n{result['name']}\n{result['type']}\nratio = {result['ratio']}, vlk_id = {result['vlk_id']}\n"
    label = tk.Label(root, text=text, justify="left")
    label.pack(anchor="w")

    # Создаем кнопку для копирования названия в буфер обмена
    copy_button = tk.Button(root, text="Скопировать", command=lambda text=result['name']: copy_to_clipboard(text))
    copy_button.pack(anchor="w")

# Запуск основного цикла приложения
root.mainloop()