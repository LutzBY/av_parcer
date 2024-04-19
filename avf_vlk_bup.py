import datetime
from datetime import datetime
import time
import psycopg2
import csv


# Чтение конфига с пасвордами
with open('config.txt', 'r') as file:
    lines = file.readlines()

mail_login = lines[1].strip()
mail_password = lines[3].strip()
pgre_login = lines[5].strip()
pgre_password = lines[7].strip()
pgre_host = lines[9].strip()
pgre_port = lines[11].strip()
pgre_db = lines[13].strip()
backup_path = lines[15].strip()
    
#Подключение к postgres
conn = psycopg2.connect(
    host = pgre_host,
    port = pgre_port,
    database = pgre_db,
    user = pgre_login,
    password = pgre_password
)

current_time_start = datetime.now()
print(f"Привет! Текущая дата - {current_time_start}")

# Забираем всю таблицу av_full и делаем бекап
cursor = conn.cursor()
select_query = "SELECT * from av_full"
cursor.execute(select_query)
rows_avf = cursor.fetchall()
rows_avf_count = cursor.rowcount
print(f"Строк в базе: {rows_avf_count}")

# Создаем и сохраняем бекап в csv
backup_avf = f"{backup_path}av_full_backup_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
with open(backup_avf, 'w', newline='', encoding='utf-8 sig') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerows(rows_avf)
print(f"Бекап таблицы av_full сохранен в файле: {backup_avf}")

# Забираем всю таблицу av_full и делаем бекап
cursor = conn.cursor()
select_query = "SELECT * from vlookup"
cursor.execute(select_query)
rows_vlk = cursor.fetchall()
rows_vlk_count = cursor.rowcount
print(f"Строк в базе: {rows_vlk_count}")

# Создаем и сохраняем бекап в csv
backup_vlk = f"{backup_path}vlookup_backup_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
with open(backup_vlk, 'w', newline='', encoding='utf-8 sig') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerows(rows_vlk)
print(f"Бекап таблицы vlookup сохранен в файле: {backup_vlk}")


cursor.close()
conn.close()