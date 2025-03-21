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
    set_flag_and_reset_query = ("UPDATE av_full SET exclude_flag = True, model_vlk = '' WHERE id = %s") % (id_to_check) 
    vlk_cursor.execute(set_flag_and_reset_query)
    conn.commit()
    root.destroy()
    sys.exit()

# Функция кнопки для удаления айди
def delete_id(id_to_check):
    vlk_cursor = conn.cursor()
    delete_id_query = ("DELETE FROM av_full WHERE id = %s") % (id_to_check) 
    vlk_cursor.execute(delete_id_query)
    conn.commit()
    root.destroy()
    sys.exit()

# Функция кнопки для пометки дубликатов
def mark_duplicates_and_set_oldest_date_in_(id_to_check):
    def on_enter(event=None):
        # Получаем текст из поля ввода
        dupl_id_list = entry.get()
        if not dupl_id_list.strip():
            messagebox.showwarning("Ошибка", "Поле не должно быть пустым!")
            return
        
        try: # Выполнение квери на замену дубликатов
            dupl_cursor = conn.cursor()

            # квери выставить флажок дубликата
            dupl_query1 = """SELECT date
            FROM public.av_full
            WHERE id in (%s)
            order by date asc""" % (dupl_id_list)
            dupl_cursor.execute(dupl_query1)
            dupl_earliest_date = dupl_cursor.fetchone()
            dupl_earliest_date = dupl_earliest_date[0]

            # квери для дубликатов - выставить флажок дубликата и записать duplicate_id
            dupl_query2 = """UPDATE public.av_full
            SET duplicate_flag = True, duplicate_id = %s
            WHERE id in (%s);""" % (id_to_check, dupl_id_list)
            dupl_cursor.execute(dupl_query2)

            # квери для искомого id - выставить более старую дату
            dupl_query3 = """UPDATE public.av_full
            SET date_corrected = '%s'
            WHERE id = %s;""" % (dupl_earliest_date, id_to_check)
            dupl_cursor.execute(dupl_query3)

            # Закрываем окно после успешного ввода
            entry_window.destroy()
            conn.commit()
            root.destroy()
            sys.exit()

        except Exception as e:
            messagebox.showerror("Ошибка", f"Произошла ошибка: {e}")
    
    # Окно для ввода
    entry_window = tk.Toplevel(root)
    entry_window.title("Дубликаты через запятую")
    
    # Поле ввода
    entry = tk.Entry(entry_window, width=50)
    entry.pack(pady=5)
    entry.bind("<Return>", on_enter)  # Нажатие Enter для вызова функции on_enter

    # Устанавливаем фокус на поле ввода 
    #entry.focus_set() # багует ctrl+c ctrl+v (нет, багует раскладка, это просто не нужно ща)

    # Запуск окна
    entry_window.mainloop()

# Функция изменения данных и перезапуска скрипта
def update_and_restart(id_to_check, capacity, cylcount, year, conn, root, vlk_process):
    def on_save_and_restart(): # Функция кнопки "Сохранить и перезапустить"
        # Через гет получаем новые значения которые вводятся в поля
        new_capacity = entry_capacity.get().strip()
        new_cylinders = entry_cylinders.get().strip()
        new_year = entry_year.get().strip()

        if not new_capacity or not new_cylinders or not new_year:
            messagebox.showwarning("Ошибка", "Все поля должны быть заполнены!")
            return
    
        try:
            # Выполняем UPDATE
            cursor = conn.cursor()
            query = f"""
                UPDATE public.av_full
                   SET capacity = '{new_capacity}',
                       cylinders = '{new_cylinders}',
                       year = '{new_year}'
                 WHERE id = {id_to_check};
            """
            cursor.execute(query)
            conn.commit()

            # Закрываем текущее окно редактирования
            edit_window.destroy()

            # Запускаем функцию заново по новым данным
            root.destroy()
            vlk_process(id_to_check)

        except Exception as e:
            messagebox.showerror("Ошибка", f"Произошла ошибка: {e}")
            return

    # Создаём Toplevel-окно
    edit_window = tk.Toplevel(root)
    edit_window.title("Редактирование")

    # Метки и поля для capacity
    lbl_capacity = tk.Label(edit_window, text="Объём (capacity):")
    lbl_capacity.pack(pady=(10, 0))
    entry_capacity = tk.Entry(edit_window, width=20)
    entry_capacity.pack(pady=5)
    entry_capacity.insert(0, str(capacity))  # Предзаполняем

    # Метки и поля для cylinders
    lbl_cylinders = tk.Label(edit_window, text="Число цилиндров (cylcount):")
    lbl_cylinders.pack(pady=(10, 0))
    entry_cylinders = tk.Entry(edit_window, width=20)
    entry_cylinders.pack(pady=5)
    entry_cylinders.insert(0, str(cylcount))  # Предзаполняем

    # Метки и поля для year
    lbl_year = tk.Label(edit_window, text="Год выпуска (year):")
    lbl_year.pack(pady=(10, 0))
    entry_year = tk.Entry(edit_window, width=20)
    entry_year.pack(pady=5)
    entry_year.insert(0, str(year))  # Предзаполняем

    # Кнопка "Сохранить и перезапустить"
    btn_save = tk.Button(edit_window, text="Сохранить и перезапустить", command=on_save_and_restart)
    btn_save.pack(pady=10)

    # Фокус на поле ввода
    entry_capacity.focus_set()

    # Запуск цикла сообщений для окна
    edit_window.mainloop()

### ОСНОВНАЯ ФУНКЦИЯ
def vlk_process(id_to_check):
    global root

    cursor = conn.cursor()
    query = """
        SELECT brand, model, model_misc, year, cylinders, capacity, type, model_vlk
        FROM av_full
        WHERE id IN (%s);
    """ % id_to_check
    cursor.execute(query)
    row = cursor.fetchall()

    # Функция дополнения модели инф-ой из базы знаний по постре
    brand, model, modification, year, cylcount, capacity, mtype, actual_vlk = row[0]
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
                match_ratio.append(model_comp * 1.25)
                best_match_list.append(model_found)
                enumerat +=1
                
            else:
                model_comp = fuzz.token_set_ratio(model_concat.lower(), model_found.lower())
                match_ratio.append(model_comp)
                best_match_list.append(model_found)
                enumerat +=1
        model_ratio_list = list(zip(best_match_list, match_ratio))

        # Подсчет сколько встречается найденное влк
        count_vlk_query = """
        select count(model_vlk)
        from av_full
        where model_vlk = '%s'
        """ % model_found
        vlkcursor.execute(count_vlk_query)
        count_vlk = vlkcursor.fetchone()[0]

        # Запись найденного результата
        results.append({"№": enumerat,"name": model_found, "type": mtype_found, "ratio": model_comp, "vlk_id": vlk_id, "vlk_sums": count_vlk},)

        print(f"""
    {enumerat} --------
    {model_found}
    {mtype_found}
    ratio = {model_comp}, vlk_id = {vlk_id}, в базе - {count_vlk} шт.""")
        
        best_model, best_ratio = max(model_ratio_list, key=lambda x: x[1])
        best_match = best_model

    print (f"----------------")    
    print (f"{brand} {model} {modification}, {year}, d - {capacity}, c - {cylcount}, t - {mtype}, BM - {best_match}")
    vlkcursor.close()

    # Сортируем список results для передачи его по убыванию ratio
    results = sorted(results, key=lambda x: x['ratio'], reverse=True)

    ## Блок окна tk inter
    # Создание самого объекта окна gui
    root = tk.Tk()
    root_title = (f"{brand} {model} {modification}, {year} г.в. \n{capacity} см3, {cylcount} цил.\nТип - {mtype},\nVLK - {actual_vlk}")
    label = tk.Label(root, text=root_title, justify="left", background='light grey')
    label.pack(anchor="w")
    root.minsize(420, 600)

    # Создаем в нем фрейм для скроллинга
    scrollable_frame = tk.Frame(root)
    scrollable_frame.pack(fill="both", expand=True)

    # Добавляем объект Canvas для прокрутки
    canvas = tk.Canvas(scrollable_frame)
    canvas.pack(side="left", fill="both", expand=True)

    # Добавляем объект Scrollbar и связываем с Canvas
    scrollbar = tk.Scrollbar(scrollable_frame, orient="vertical", command=canvas.yview)
    scrollbar.pack(side="right", fill="y")
    canvas.configure(yscrollcommand=scrollbar.set)

    # Создаем главный внутренний фрейм, который будет содержать все элементы
    inner_frame = tk.Frame(canvas)

    # Создаем окно, чтобы Canvas мог отображать содержимое внутреннего фрейма
    canvas.create_window((0, 0), window=inner_frame, anchor="nw")

    # Функция для обновления размеров Canvas при изменении размеров внутреннего фрейма
    def update_canvas(event):
        canvas.configure(scrollregion=canvas.bbox("all"))

    inner_frame.bind("<Configure>", update_canvas)

    # Заполнение окна каждым результатом

    for idx, result in enumerate(results, 1):
        # Отображаем текстовую информацию о каждом элементе
        text = f"{idx} ------\n{result['name']}\n{result['type']}\nratio = {result['ratio']}, vlk_id = {result['vlk_id']}, в базе - {result['vlk_sums']} шт.\n"
        label = tk.Label(inner_frame, text=text, justify="left")
        label.pack(anchor="w")

        # Создаем фрейм для кнопок
        button_frame = tk.Frame(inner_frame)
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
        copy_and_write_button.pack(side="left", padx=5)

    # Создаем первый фрейм для нижних кнопок
    button_low_frame1 = tk.Frame(inner_frame, pady=10) #width=500, height=300
    button_low_frame1.pack(anchor="w", fill="x")

    # Создаем второй фрейм для нижних кнопок
    button_low_frame2 = tk.Frame(inner_frame, pady=10)
    button_low_frame2.pack(anchor="w", fill="x")

    # Кнопка установить флаг
    set_exclude_flag_button = tk.Button(
        button_low_frame1, 
        text="Установить exclude flag true",
        bg="orange",
        command=lambda: set_exclude_flag(id_to_check)
    )
    set_exclude_flag_button.pack(side="left", padx=10, pady=5)

    # Кнопка повторного запуска скрипта с заменой значений
    restart_button = tk.Button(
        button_low_frame2, 
        text="Изменить данные",
        bg="green",
        command=lambda: (update_and_restart(id_to_check, capacity, cylcount, year, conn, root, vlk_process))  # Закрываем окно и перезапускаем процесс
    )
    restart_button.pack(side="left", padx=10, pady=5)

    # Кнопка удаления искомого айди из базы
    delete_button = tk.Button(
        button_low_frame2, 
        text="УДАЛИТЬ ЗАПИСЬ",
        bg="red",
        command=lambda: delete_id(id_to_check)
    )
    delete_button.pack(side="right", padx=10, pady=5)

    # Кнопка установить флаг и очистить влк
    set_exclude_flag_clear_vlk_button = tk.Button(
        button_low_frame1, 
        text="Установить флаг и очистить vlk",
        bg="orange",
        command=lambda: set_exclude_flag_and_reset_mvlk(id_to_check)
    )
    set_exclude_flag_clear_vlk_button.pack(side="right", padx=10, pady=5)

    # Кнопка запуска пометки дубликатов
    mark_duplicates_button = tk.Button(
        button_low_frame2, 
        text="Пометить дубликаты",
        bg="violet",
        command=lambda: mark_duplicates_and_set_oldest_date_in_(id_to_check)
    )
    mark_duplicates_button.pack(side="bottom", padx=10, pady=5)

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