import datetime
from datetime import datetime
import time
import psycopg2
import subprocess
import gzip

# Чтение конфига с пасвордами
with open('config.txt', 'r') as file:
    lines = file.readlines()

pgre_login = lines[5].strip()
pgre_password = lines[7].strip()
pgre_host = lines[9].strip()
pgre_port = lines[11].strip()
pgre_db = lines[13].strip()
backup_path = lines[15].strip()
pgdumpexe_path = lines[17].strip()

current_time_start = datetime.now()
print(f"Привет! Текущая дата - {current_time_start}")

# Для av_full
backup_path = f"{backup_path}av_full_backup_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.gz"

table_to_dump = 'public.av_full'

command = f"pg_dump -h {pgre_host} -p {pgre_port} -U {pgre_login} -W {pgre_password} -d {pgre_db} -t {table_to_dump} > {backup_path}"

# Вызываем pg_dump и сохраняем результат в сжатом формате gzip
with gzip.open(backup_path, 'wb') as f:
    subprocess.run(command, stdout=f, shell=True)
    #subprocess.run([pgdumpexe_path, '-h', pgre_host, '-p', pgre_port, '-U', pgre_login, '-W', pgre_password, '-d', pgre_db, '-t', table_to_dump], stdout=f)
print(f"Бекап базы av_full сохранен в файле: {backup_path}")