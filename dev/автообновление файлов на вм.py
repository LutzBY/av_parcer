import os
import hashlib
import shutil
from git import Repo

# Пути
repo_url = "https://github.com/LutzBY/av_parcer.git"  # URL репозитория
local_dir = "/path/to/local/repo"  # Путь к локальной папке для репозитория
vm_dir = "/path/to/local/vm"  # Путь к рабочей папке ВМ
needed_files = ["actcheck.py", "avparser.py", "onlparser.py", "exceptions.json"]  # Список нужных файлов для работы ВМ

# Функция сверки хэшей
def calculate_hash(file_path):
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            sha256.update(chunk)
    return sha256.hexdigest()

# Функция проверить и клонировать репозиторий, если он отсутствует
def check_and_clone_repo(repo_url, local_dir):
    if not os.path.exists(local_dir):
        print(f"Локальная папка {local_dir} не найдена. Клонируем репозиторий...")
        Repo.clone_from(repo_url, local_dir)
        print("Репозиторий успешно клонирован.")
    else:
        print(f"Локальная папка {local_dir} уже существует.")

# Функция git pull для обновления репозитория
def pull_repo(local_dir):
    repo = Repo(local_dir)
    if repo.is_dirty(untracked_files=True):
        print("Локальный репозиторий имеет несохранённые изменения. Проверьте вручную!")
    else:
        print("Обновляем локальный репозиторий...")
        repo.remotes.origin.pull()
        print("Репозиторий обновлён.")

# Функция сравнения хэшей файлов в локальной папке и рабочей папке ВМ
def compare_hashes(local_dir, vm_dir, needed_files):
    for file in needed_files:
        local_file = os.path.join(local_dir, file)
        vm_file = os.path.join(vm_dir, file)

        # Если файл отсутствует в одной из папок
        if not os.path.exists(local_file) or not os.path.exists(vm_file):
            print(f"Файл {file} отсутствует в одной из папок.")
            return False

        # Сравнение хэшей
        if calculate_hash(local_file) != calculate_hash(vm_file):
            print(f"Файл {file} отличается.")
            return False

    print("Все нужные файлы совпадают.")
    return True

# Функция скопировать нужные файлы из локальной папки в рабочую папку ВМ
def copy_files(local_dir, vm_dir, needed_files):
    for file in needed_files:
        src = os.path.join(local_dir, file)
        dst = os.path.join(vm_dir, file)
        shutil.copy2(src, dst)  # Копируем файл с сохранением метаданных
        print(f"Файл {file} скопирован из {src} в {dst}.")

## Основной цикл
def main():
    # 1. Проверка и клонирование репозитория
    check_and_clone_repo(repo_url, local_dir)
    # 2. Обновление репозитория
    pull_repo(local_dir)
    
    # 3. Проверка хэшей
    if not compare_hashes(local_dir, vm_dir, needed_files):
        # 4. Копирование нужных файлов
        copy_files(local_dir, vm_dir, needed_files)
    else:
        print("Обновление не требуется. Скрипты на ВМ актуальны.")

if __name__ == "__main__":
    main()