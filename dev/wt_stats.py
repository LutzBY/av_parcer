import tkinter as tk
from tkinter import scrolledtext
import pyperclip
import re
import pandas as pd
import sys

# Куда
file_path = r"C:\Users\lutzb\Desktop\wt_stats\data.xlsx"

# Функция парсинга
def parse_battle_stats():
    imported_game_log = pyperclip.paste()
    if not imported_game_log.strip():
        print("❌ Буфер обмена пуст. Скопируй статистику боя и запусти скрипт снова.")
        return None

    # --- Результат: Победа / Поражение ---
    result_match = re.search(r'(Победа|Поражение) в миссии', imported_game_log)
    result = result_match.group(1) if result_match else "Неизвестно"

    # --- Название миссии ---
    mission_match = re.search(r'миссии\s+"([^"]+)"', imported_game_log)
    mission = mission_match.group(1) if mission_match else "Неизвестно"

    # --- Итого: СЛ, СОИ (FRP), ОИ (RP) — только последнее вхождение ---
    total_matches = re.findall(r'Итого:\s*(\d+)\s*СЛ,\s*(\d+)\s*СОИ,\s*(\d+)\s*ОИ', imported_game_log)
    if not total_matches:
        print("❌ Не удалось найти ни одного вхождения 'Итого'.")
        return None

    # Берём ПОСЛЕДНЕЕ вхождение (финальные итоги)
    last_match = total_matches[-1]
    total_sl = int(last_match[0])   # Silver Lions
    total_frp = int(last_match[1])  # Free Research Points
    total_rp = int(last_match[2])   # Research Points

    # --- Очки миссии ---
    mission_points = re.findall(r'(\d+)\s*очк(?:о|а|ов)\s*миссии', imported_game_log)
    total_mission_points = sum(int(x) for x in mission_points)

    # --- Сессия ---
    session_match = re.search(r'Сессия:\s*([a-f0-9]+)', imported_game_log)
    session_id = session_match.group(1) if session_match else None
    if not session_id:
        print("❌ Не удалось найти session_id.")
        return None

    # --- Активность (%) ---
    activity_match = re.search(r'Активность:\s*(\d+)%', imported_game_log)
    activity_percent = int(activity_match.group(1)) if activity_match else None

    # --- Использованная техника ---
    vehicles = set()

    # Паттерн 1: "Время активности" — ищем текст до "Цифры + (ПА)"
    pattern_active = r'^\s*(.+?)\s+\d+\s*\+\s*$$ПА$$'
    # Паттерн 2: "Время игры" — ищем текст до "95% ... 4:51"
    pattern_game = r'^\s*(.+?)\s+\d+%.*?\d+:\d+'

    active_time_matches = re.findall(pattern_active, imported_game_log, re.MULTILINE)
    game_time_matches = re.findall(pattern_game, imported_game_log, re.MULTILINE)
    
    all_vehicles = active_time_matches + game_time_matches

    for v in all_vehicles:
        cleaned = re.sub(r'\s+', ' ', v.strip())
        # Исключаем ложные срабатывания (например, "Заработано", "Итого")
        if cleaned and not re.match(r'^[0-9\[\]"]', cleaned) and len(cleaned) > 1:
            vehicles.add(cleaned)

    vehicles = ", ".join(sorted(vehicles)) if vehicles else "Неизвестно"

    return {
        'session_id': session_id,
        'vehicles': vehicles,
        'total_sl': total_sl,
        'total_frp': total_frp,
        'total_rp': total_rp,
        'total_mission_points': total_mission_points,
        'result': result,
        'mission': mission,
        'activity_percent': activity_percent,
    }

# Функция сохранения в эксель
def save_to_excel(data, file_path):
    
    columns = [
        'session_id', 'vehicles', 'total_sl', 'total_frp', 'total_rp',
        'total_mission_points', 'result', 'mission', 'activity_percent'
    ]

    try:
        df = pd.read_excel(file_path, engine='openpyxl')
    except (FileNotFoundError, ValueError):
        df = pd.DataFrame(columns=columns)

    # Удаляем строку с таким session_id, если есть
    df = df[df['session_id'] != data['session_id']]

    # Добавляем новую
    new_row = pd.DataFrame([data], columns=columns)
    df = pd.concat([df, new_row], ignore_index=True)

    # Сохраняем
    df.to_excel(file_path, index=False, engine='openpyxl')
    print(f"\n ✅ Обновлено: {data['session_id']}")

# Окно Tkinter
class WTApp:
    def __init__(self, root):
        self.root = root
        self.root.title("WT Parser")
        root.geometry('%dx%d+%d+%d' % (400, 325, 1500, 650)) # размер - ш, в, положение - ш, в (3520 + 1080 )
        self.root.resizable(True, True)
        self.root.attributes('-topmost', True) # поверх
        self.root.attributes('-alpha', 0.90) # прозрачность
        
        # Метка: последняя миссия
        self.last_mission_label = tk.Label(
            root,
            text="Последняя миссия: неизвестно",
            font=("Arial", 9),
            fg="gray",
            wraplength=330,
            anchor="w",
            justify="left"
        )
        self.last_mission_label.pack(pady=(10, 5), padx=10, fill='x')

        # Кнопка
        self.button = tk.Button(
            root,
            text="📝 Записать",
            font=("Arial", 12),
            command=self.on_button_click
        )
        self.button.pack(pady=10)

        # Текстовое поле с выводом
        self.text_area = scrolledtext.ScrolledText(
            root,
            wrap=tk.WORD,
            font=("Consolas", 10),
            state='disabled',
            bg="white",
            fg="black",
            padx=10,
            pady=10
        )
        self.text_area.pack(expand=True, fill='both', padx=10, pady=5)

        # Перенаправление print в текстовое поле
        sys.stdout = TextRedirector(self.text_area)

    def on_button_click(self):
        self.text_area.configure(state='normal')
        self.text_area.delete(1.0, tk.END)
        self.text_area.configure(state='disabled')
        print("🔄 Обработка буфера обмена...")
        
        data = parse_battle_stats()
        if data:
            print("\n📋 Извлечено:")

            # Обновляем заголовок
            mission = data['mission']
            self.last_mission_label.config(
            text=f"{mission}",
            fg="black"
            ) # Подставляем новый текст

            # Выводим распаршенные строки
            for k, v in data.items():
                print(f"\n{k}: {v}")
            
            # Вызываем запись в эксель
            save_to_excel(data, file_path)

        else:
            print("❌ Обработка не удалась.")

# Забираем текст из print()
class TextRedirector:
    def __init__(self, widget):
        self.widget = widget

    def write(self, text):
        if text.strip():  # чтобы не вставлять пустые строки
            self.widget.configure(state='normal')
            self.widget.insert(tk.END, text)
            self.widget.see(tk.END)
            self.widget.configure(state='disabled')
            self.widget.update_idletasks()  # обновление интерфейса

    def flush(self):
        pass  # требуется для совместимости с stdout

# === ЗАПУСК ===
if __name__ == "__main__":
    root = tk.Tk()
    app = WTApp(root)
    root.mainloop()