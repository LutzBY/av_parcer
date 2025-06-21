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

# Сохранение исходных значений
class OldValuesKeeper:
    def __init__(self):
        # Атрибуты экземпляра для хранения старых значений
        # brand, model, modification, year, cylcount, capacity, mtype, actual_vlk, price, status, set_exclude_flag, duplicate_flag
        self.old_brand = None
        self.old_model = None
        self.old_model_misc = None
        self.old_year = None
        self.old_cylinders = None
        self.old_capacity = None
        self.old_type = None
        self.old_model_vlk = None
        
    def save_values(self, brand, model, model_misc, year, cylinders, capacity, type, model_vlk):
        # Проверяем, сохранены ли уже значения
        if (
            self.old_brand is None
            and self.old_model is None
            and self.old_model_misc is None
            and self.old_year is None
            and self.old_cylinders is None
            and self.old_capacity is None
            and self.old_type is None
            and self.old_model_vlk is None
            and self.old_capacity is None
            ):
            # Сохраняем значения только при первом вызове
            self.old_brand = brand
            self.old_model = model
            self.old_model_misc = model_misc
            self.old_year = year
            self.old_cylinders = cylinders
            self.old_capacity = capacity
            self.old_type = type
            self.old_model_vlk = model_vlk

    def get_old_values(self):
        # Метод для получения сохраненных значений
        return self.old_brand, self.old_model, self.old_model_misc, self.old_year, self.old_cylinders, self.old_capacity, self.old_type, self.old_model_vlk, self.old_capacity
    def clear_old_values (self):
        self.old_brand = None
        self.old_model = None
        self.old_model_misc = None
        self.old_year = None
        self.old_cylinders = None
        self.old_capacity = None
        self.old_type = None
        self.old_model_vlk = None

# Функция для перезапуска по новому айди
def get_id_from_clipboard(root, keeper):
    # Сохранение айди из буфера обмена
    id_to_check = pyperclip.paste()

    #Закрываем старое окно
    root.destroy()
    
    #Проверка на айди
    if not id_to_check.isdigit():
        messagebox.showinfo("Результат", "В буфере обмена не ID")
    
    # Создание экземпляра класса
    keeper.clear_old_values()

    main_app_window(id_to_check)

# Создание главного окна ткинтер
def main_app_window(id_to_check):
    # Загружаем данные из базы (без  выполнения поиска VLK)
    brand, model, modification, year, cylcount, capacity, mtype, actual_vlk, price, status, exclude_flag, duplicate_flag = load_data_from_db(id_to_check, keeper)
    
    # Создаем главное диалоговое окно
    root = tk.Tk()
    root.title("Главное меню")
    root.attributes('-topmost', True)
    info = (f"""{id_to_check}, {status}, VLK - {actual_vlk}
        {brand} {model} {modification}
        {year} г.в. {capacity} см3, {cylcount} цил.
        {mtype}
        {price} USD
    E.Flag - {exclude_flag}, D.Flag - {duplicate_flag}""")
    label = tk.Label(root, text=info, justify="left", background='light grey')
    label.pack(anchor="w", padx=10, pady=10)
    root.minsize(420, 200)
    root.geometry('%dx%d+%d+%d' % (400, 300, 1300, 250)) # размер ш.в. и положение (отступы) ш.в.

    # --- Кнопки ---

    # Создаем первый фрейм для верхних кнопок
    button_low_frame1 = tk.Frame(root, pady=10) #width=500, height=300
    button_low_frame1.pack(anchor="w", fill="x")

    # Создаем второй фрейм для средних кнопок
    button_low_frame2 = tk.Frame(root, pady=10)
    button_low_frame2.pack(anchor="w", fill="x")

    # Создаем второй фрейм для нижних кнопок
    button_low_frame3 = tk.Frame(root, pady=10)
    button_low_frame3.pack(anchor="w", fill="x")

    # Фрейм 1 - Кнопка запуска поиска VLK
    btn_start_vlk = tk.Button(
        button_low_frame1,
        text="Запуск VLK поиска",
        bg="green",
        command=lambda: vlk_search_process(id_to_check, brand, model, modification, year, cylcount, capacity, mtype, actual_vlk)
    )
    btn_start_vlk.pack(side="left", padx=10, pady=5)

    # Фрейм 1 - Кнопка запуска пометки дубликатов
    mark_duplicates_button = tk.Button(
        button_low_frame1, 
        text="Пометить дубликаты",
        bg="violet",
        command=lambda: mark_duplicates_and_set_oldest_date_in_(id_to_check, root)
    )
    mark_duplicates_button.pack(side="left", padx=10, pady=5)

    # Фрейм 1 - Кнопка изменить данные
    btn_edit = tk.Button(
        button_low_frame1,
        text="Изменить данные",
        bg="cyan",
        command=lambda: update_and_restart(
            id_to_check, capacity, cylcount, year, mtype, actual_vlk, conn, root,
            lambda new_id: main_app_window(new_id),
            keeper
        )
    )
    btn_edit.pack(side="right", padx=10, pady=5)

    # Фрейм 2 - Кнопка установить флаг
    set_exclude_flag_button = tk.Button(
        button_low_frame2, 
        text="Установить exclude flag true",
        bg="orange",
        command=lambda: set_exclude_flag(id_to_check)
    )
    set_exclude_flag_button.pack(side="left", padx=10, pady=5)

    # Фрейм 2 - Кнопка установить флаг и очистить влк
    set_exclude_flag_clear_vlk_button = tk.Button(
        button_low_frame2, 
        text="Установить vlk 'кастом'",
        bg="orange",
        command=lambda: set_exclude_mvlk_to_cutom(id_to_check, root)
    )
    set_exclude_flag_clear_vlk_button.pack(side="bottom", padx=10, pady=5)

    # Фрейм 3 - Кнопка удалить запись
    btn_delete = tk.Button(
        button_low_frame3,
        text="УДАЛИТЬ ЗАПИСЬ",
        bg="red",
        command=lambda: delete_id(id_to_check, root)
    )
    btn_delete.pack(side="right", padx=10, pady=5)

    # Фрейм 3 - Кнопка перезапустить с новым айди
    btn_restart = tk.Button(
        button_low_frame3,
        text="НОВЫЙ АЙДИ",
        bg="yellow",
        command=lambda: get_id_from_clipboard(root, keeper)
    )
    btn_restart.pack(side="left", padx=10, pady=5)

    root.mainloop()

# Функция получения данных из базы
def load_data_from_db(id_to_check, keeper):
    cursor = conn.cursor()
    query = """
        SELECT brand, model, model_misc, year, cylinders, capacity, type, model_vlk, price, status, exclude_flag, duplicate_flag
        FROM av_full
        WHERE id IN (%s);
    """ % id_to_check
    cursor.execute(query)
    row = cursor.fetchall()[0]
    cursor.close()
    # сохраняем значения
    keeper.save_values(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
    return row  # brand, model, modification, year, cylcount, capacity, mtype, actual_vlk, price, status, set_exclude_flag, duplicate_flag
# Функция mvlk
def vlk_search_process(id_to_check, brand, model, modification, year, cylcount, capacity, mtype, actual_vlk):
    global root

    if modification:
        model_concat = model + " " + modification
    else:
        model_concat = model
    print(model_concat)
    displacement = float(capacity)

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

    for row_ in rows:
        model_found = row_[0]
        mtype_found = row_[1]
        vlk_id = row_[2]
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
    root_title = (f"{id_to_check}\n{brand} {model} {modification}, {year} г.в. \n{capacity} см3, {cylcount} цил.\nТип - {mtype},\nVLK - {actual_vlk}")
    label = tk.Label(root, text=root_title, justify="left", background='light grey')
    label.pack(anchor="w")
    root.minsize(420, 600)
    root.attributes('-topmost', True)

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
            command=lambda text=result['name']: copy_to_clipboard_and_write(text, id_to_check))
        copy_and_write_button.pack(side="left", padx=5)

    # Запуск основного цикла приложения
    root.mainloop()

# Функция кнопки для копирования текста в буфер обмена
def copy_to_clipboard(text):
    pyperclip.copy(text)  # Используем pyperclip для копирования
    #messagebox.showinfo("Информация", f"'{text}' скопировано в буфер обмена.")  # Показываем уведомление
    root.destroy()  # Закрываем главное окно
    # sys.exit()  # Завершаем выполнение программы

# Функция кнопки для копирования текста и записи в базу
def copy_to_clipboard_and_write(text, id_to_check):
    vlk_to_write = text
    vlk_cursor = conn.cursor()
    organization_query = ("UPDATE av_full SET model_vlk = '%s' WHERE id = %s") % (vlk_to_write, id_to_check) 
    vlk_cursor.execute(organization_query)
    conn.commit()
    root.destroy()
    # sys.exit()

# Функция кнопки для записи exclude_flag
def set_exclude_flag(id_to_check):
    vlk_cursor = conn.cursor()
    set_flag_query = ("UPDATE av_full SET exclude_flag = True WHERE id = %s") % (id_to_check)
    vlk_cursor.execute(set_flag_query)
    conn.commit()

# легаси Функция кнопки для записи exclude_flag
def set_exclude_flag_and_reset_mvlk(id_to_check):
    vlk_cursor = conn.cursor()
    set_flag_and_reset_query = ("UPDATE av_full SET exclude_flag = True, model_vlk = '' WHERE id = %s") % (id_to_check) 
    vlk_cursor.execute(set_flag_and_reset_query)
    conn.commit()
    root.destroy()
    # sys.exit()

# Функция кнопки установить влк "кастом"
def set_exclude_mvlk_to_cutom(id_to_check, root):
    vlk_cursor = conn.cursor()
    set_flag_and_reset_query = ("UPDATE av_full SET model_vlk = 'кастом' WHERE id = %s") % (id_to_check) 
    vlk_cursor.execute(set_flag_and_reset_query)
    conn.commit()
    # root.destroy()
    # sys.exit()

# Функция кнопки для удаления айди
def delete_id(id_to_check, root):
    vlk_cursor = conn.cursor()
    delete_id_query = ("DELETE FROM av_full WHERE id = %s") % (id_to_check) 
    vlk_cursor.execute(delete_id_query)
    conn.commit()
    # root.destroy()
    # sys.exit()

# Функция кнопки для пометки дубликатов
def mark_duplicates_and_set_oldest_date_in_(id_to_check, root):
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
            # root.destroy()
            # sys.exit()

            # Выводим сообщение
            messagebox.showinfo(f'Искомый айди - {id_to_check} ', f'Записаны {dupl_id_list} c датой {dupl_earliest_date}')

        except Exception as e:
            messagebox.showerror("Ошибка", f"Произошла ошибка: {e}")
    
    # Окно для ввода
    entry_window = tk.Toplevel(root)
    entry_window.title("Дубликаты через запятую")
    entry_window.attributes('-topmost', True)
    
    # Поле ввода
    entry = tk.Entry(entry_window, width=50)
    entry.pack(pady=5)
    entry.bind("<Return>", on_enter)  # Нажатие Enter для вызова функции on_enter

    # Устанавливаем фокус на поле ввода 
    entry.focus_set() # не нужно ща

    # Запуск окна
    entry_window.mainloop()

# Функция изменения данных и перезапуска скрипта
def update_and_restart(id_to_check, capacity, cylcount, year, mtype, actual_vlk, conn, root, vlk_process, keeper):
    
    # Функция кнопки "Сохранить и перезапустить"
    def on_save_and_restart():
        # Через гет получаем новые значения которые вводятся в поля
        new_vlk = entry_vlk.get().strip()
        new_capacity = entry_capacity.get().strip()
        new_cylinders = entry_cylinders.get().strip()
        new_year = entry_year.get().strip()
        new_mtype = entry_mtype.get().strip()

        if not new_capacity or not new_cylinders or not new_year or not new_mtype:
            messagebox.showwarning("Ошибка", "Все поля должны быть заполнены!")
            return
        try:
            cursor = conn.cursor()
            query = f"""
                UPDATE public.av_full
                   SET capacity = '{new_capacity}',
                       cylinders = '{new_cylinders}',
                       year = '{new_year}',
                       model_vlk = '{new_vlk}',
                       type = '{new_mtype}'
                 WHERE id = {id_to_check};
            """
            cursor.execute(query)
            conn.commit()
            edit_window.destroy()

            # Подгружаем новые данные из базы (для точности — вдруг там триггер что-то сменил)
            row = load_data_from_db(id_to_check, keeper)
            vlk_search_process(id_to_check, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]) 
        except Exception as e:
            messagebox.showerror("Ошибка", f"Произошла ошибка: {e}")
            return

    # Функция кнопки "Сохранить и закрыть"
    def on_save_and_stop():
        # Через гет получаем новые значения которые вводятся в поля
        new_vlk = entry_vlk.get().strip()
        new_capacity = entry_capacity.get().strip()
        new_cylinders = entry_cylinders.get().strip()
        new_year = entry_year.get().strip()
        new_mtype = entry_mtype.get().strip()

        if not new_capacity or not new_cylinders or not new_year or not new_mtype:
            messagebox.showwarning("Ошибка", "Все поля должны быть заполнены!")
            return
        try:
            cursor = conn.cursor()
            query = f"""
                UPDATE public.av_full
                   SET capacity = '{new_capacity}',
                       cylinders = '{new_cylinders}',
                       year = '{new_year}',
                       model_vlk = '{new_vlk}',
                       type = '{new_mtype}'
                 WHERE id = {id_to_check};
            """
            cursor.execute(query)
            conn.commit()
            edit_window.destroy()
            # root.destroy()
            # sys.exit()
        except Exception as e:
            messagebox.showerror("Ошибка", f"Произошла ошибка: {e}")
            return
    
    # Функция кнопки "Подставить и перезапустить"
    def paste_and_restart():
        # Берём значения из полей и просто вызываем vlk_search_process
        new_vlk = entry_vlk.get().strip()
        new_capacity = entry_capacity.get().strip()
        new_cylinders = entry_cylinders.get().strip()
        new_year = entry_year.get().strip()
        new_mtype = entry_mtype.get().strip()

        if not new_capacity or not new_cylinders or not new_year or not new_mtype:
            messagebox.showwarning("Ошибка", "Все поля должны быть заполнены!")
            return
        try:
            edit_window.destroy()
            # Вызов поиска VLK по введённым значениям, не меняя базу
            # vlk_search_process(id_to_check, brand, model, modification, year, cylcount, capacity, mtype, actual_vlk)
            vlk_search_process(
                id_to_check,
                keeper.old_brand,
                keeper.old_model,
                keeper.old_model_misc,
                new_year,
                new_cylinders,
                new_capacity,
                new_mtype,
                actual_vlk
            )
        except Exception as e:
            messagebox.showerror("Ошибка", f"Произошла ошибка: {e}")
            return

    # Создаём Toplevel-окно
    edit_window = tk.Toplevel(root)
    edit_window.title("Редактирование")
    edit_window.attributes('-topmost', True)

    # Метки и поля для vlk
    lbl_vlk = tk.Label(edit_window, text="Актуальная VLK")
    lbl_vlk.pack(pady=(10, 0))
    entry_vlk = tk.Entry(edit_window, width=20)
    entry_vlk.pack(pady=5)
    entry_vlk.insert(0, str(actual_vlk))  # Предзаполняем

    # Метки и поля для capacity
    lbl_capacity = tk.Label(edit_window, text=f"Объём (исх - {keeper.old_capacity}):")
    lbl_capacity.pack(pady=(10, 0))
    entry_capacity = tk.Entry(edit_window, width=20)
    entry_capacity.pack(pady=5)
    entry_capacity.insert(0, str(capacity))  # Предзаполняем

    # Метки и поля для cylinders
    lbl_cylinders = tk.Label(edit_window, text=f"Цилиндров (исх - {keeper.old_cylinders}):")
    lbl_cylinders.pack(pady=(10, 0))
    entry_cylinders = tk.Entry(edit_window, width=20)
    entry_cylinders.pack(pady=5)
    entry_cylinders.insert(0, str(cylcount))  # Предзаполняем

    # Метки и поля для year
    lbl_year = tk.Label(edit_window, text=f"Год выпуска (исх - {keeper.old_year}):")
    lbl_year.pack(pady=(10, 0))
    entry_year = tk.Entry(edit_window, width=20)
    entry_year.pack(pady=5)
    entry_year.insert(0, str(year))  # Предзаполняем

    # Метки и поля для mtype
    lbl_mtype = tk.Label(edit_window, text=f"Год выпуска (исх - {keeper.old_type}):")
    lbl_mtype.pack(pady=(10, 0))
    entry_mtype = tk.Entry(edit_window, width=20)
    entry_mtype.pack(pady=5)
    entry_mtype.insert(0, str(mtype))  # Предзаполняем

    # Кнопка "Сохранить и перезапустить"
    btn_save = tk.Button(edit_window, text="Сохранить и перезапустить", command=on_save_and_restart, bg="green")
    btn_save.pack(pady=10)

    # Кнопка "Подставить и перезапустить"
    btn_paste_and_restart = tk.Button(edit_window, text="Подставить и перезапустить", command=paste_and_restart, bg="cyan")
    btn_paste_and_restart.pack(pady=10)

    # Кнопка "Сохранить и закрыть"
    btn_save = tk.Button(edit_window, text="Сохранить и закрыть", command=on_save_and_stop, bg="orange")
    btn_save.pack(pady=10)
    
    # Фокус на поле ввода
    entry_capacity.focus_set()

    # Запуск цикла сообщений для окна
    edit_window.mainloop()


#### ИСПОЛНЕНИЕ

# Сохранение айди из буфера обмена
id_to_check = pyperclip.paste()

# Создание экземпляра класса
keeper = OldValuesKeeper()

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
main_app_window(id_to_check)

# Закрыть коннекшон
conn.close()