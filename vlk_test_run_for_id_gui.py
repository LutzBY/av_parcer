##########################
#id_to_check = 111355795 114225903
#########################

import json
from fuzzywuzzy import fuzz
import psycopg2
import pyperclip
import tkinter as tk
from tkinter import messagebox
import sys

# Функция кнопки для копирования текста в буфер обмена
def copy_to_clipboard(text):
    pyperclip.copy(text)  # Используем pyperclip для копирования
    #messagebox.showinfo("Информация", f"'{text}' скопировано в буфер обмена.")  # Показываем уведомление
    root.destroy()  # Закрываем главное окно
    sys.exit()  # Завершаем выполнение программы

# Функция кнопки для копирования текста и записи в базу
def copy_to_clipboard_and_write(text):
    vlk_to_write = text
    vlk_cursor = conn.cursor()
    organization_query = ("UPDATE av_full SET model_vlk = '%s' WHERE id = %s") % (vlk_to_write, id_to_check) 
    vlk_cursor.execute(organization_query)
    conn.commit()
    root.destroy()
    sys.exit()

# Функция кнопки для записи exclude_flag
def set_exclude_flag(id_to_check):
    vlk_cursor = conn.cursor()
    set_flag_query = ("UPDATE av_full SET exclude_flag = True WHERE id = %s") % (id_to_check)
    vlk_cursor.execute(set_flag_query)
    conn.commit()

# Функция кнопки для записи exclude_flag
def set_exclude_flag_and_reset_mvlk(id_to_check):
    vlk_cursor = conn.cursor()
    set_flag_query = ("UPDATE av_full SET exclude_flag = True, model_vlk = '' WHERE id = %s") % (id_to_check) 
    vlk_cursor.execute(set_flag_query)
    conn.commit()
    root.destroy()
    sys.exit()

### Основная функция
def vlk_process(id_to_check):
    global root

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

        # Запись найденного результата
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

    # Создание окна gui
    root = tk.Tk()
    root_title = (f"{brand} {model} {modification}, {year} г.в. \n{capacity} см3, {cylcount} цил.\nТип - {mtype}")
    label = tk.Label(root, text=root_title, justify="left", background='light grey')
    label.pack(anchor="w")

    # Его заполнение каждым результатом
    for idx, result in enumerate(results, 1):
        # Отображаем текстовую информацию о каждом элементе
        text = f"{idx} --------\n{result['name']}\n{result['type']}\nratio = {result['ratio']}, vlk_id = {result['vlk_id']}\n"
        if result['ratio'] >= 50:
            label = tk.Label(root, text=text, justify="left", background='light grey')
        else:
            label = tk.Label(root, text=text, justify="left")
        label.pack(anchor="w")

        # Создаем фрейм для кнопок
        button_frame = tk.Frame(root)
        button_frame.pack(anchor="w")
        # Создаем кнопку для копирования названия в буфер обмена
        copy_button = tk.Button(
            button_frame, 
            text="Скопировать",
            command=lambda text=result['name']: copy_to_clipboard(text))
        copy_button.pack(side="left")
        # Создаем кнопку для прямой записи влк в базу
        copy_and_write_button = tk.Button(
            button_frame, 
            text="Записать", 
            command=lambda text=result['name']: copy_to_clipboard_and_write(text))
        copy_and_write_button.pack(side="left")

    # Создаем еще один фрейм для нижних кнопок
    button_low_frame = tk.Frame(root, pady=10) #width=500, height=300
    button_low_frame.pack(anchor="w", fill="x")

    # Кнопка установить флаг
    set_exclude_flag_button = tk.Button(
        button_low_frame, 
        text="Установить exclude flag true",
        bg="orange",
        command=lambda: set_exclude_flag(id_to_check)
    )
    set_exclude_flag_button.pack(side="top", padx=10, pady=5)

    # Кнопка повторного запуска скрипта (while process_flag)
    restart_button = tk.Button(
        button_low_frame, 
        text="Перезапустить",
        bg="red",
        command=lambda: (root.destroy(), vlk_process(id_to_check))  # Закрываем окно и перезапускаем процесс
    )
    restart_button.pack(side="bottom", padx=10, pady=5)

    # Кнопка установить флаг и очистить влк
    set_exclude_flag_clear_vlk_button = tk.Button(
        button_low_frame, 
        text="Установить флаг и очистить vlk",
        bg="orange",
        command=lambda: set_exclude_flag_and_reset_mvlk(id_to_check)
    )
    set_exclude_flag_clear_vlk_button.pack(side="bottom", padx=10, pady=5)

    # Запуск основного цикла приложения
    root.mainloop()


# Сохранение айди из буфера обмена
id_to_check = pyperclip.paste()

#Проверка на айди
if not id_to_check.isdigit():
    messagebox.showinfo("Результат", "В буфере обмена не ID")
    sys.exit()

# Чтение json конфига
with open('config.json') as file:
    config = json.load(file)

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

# Запуск главной функции
vlk_process(id_to_check)

# Закрыть коннекшон
conn.close()