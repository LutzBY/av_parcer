import openai
import json
from openai import APIStatusError
import requests
import psycopg2

# Счетчики
token_usage = 0
llm_iter_counter = 0

# Чтение json конфига
with open('config.json', encoding='UTF-8') as file:
    config = json.load(file)

mail_login = config['sender login']
mail_password = config['sender password']
pgre_login = config['postgre login']
pgre_password = config['postgre password']
pgre_host = config['postgre host']
pgre_port = config['postgre port']
pgre_db = config['postgre database']
recipients = config['mail recipients']
poe_api_keys = config['api_keys']
av_key = config['av_x-api-key']

# Хэдеры
headers = {
    'accept': '*/*',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,be-BY;q=0.6,be;q=0.5',
    'cache-control': 'no-cache',
    'content-type': 'application/json',
    'origin': 'https://moto.av.by',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://moto.av.by/',
    'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Mobile Safari/537.36',
    'x-api-key': av_key,
    'x-user-group': '4e1bdcdb-a5c9-414f-bac8-a7e99e5a77db',
    'x-device-type': 'web.desktop'
}

# Функция подбора модели по llm
def add_mvlk_llm (brand, model, modification, year, cylcount, capacity, mtype):
    global token_usage
    global llm_iter_counter

    # Получаем rolling ключ
    api_key = poe_api_keys[llm_iter_counter % len(poe_api_keys)]
    
    # Инициализация клиента
    client = openai.OpenAI(
    api_key= api_key,
    base_url="https://api.poe.com/v1",
    )

    # Передаваемое сообщение
    messages = [
        {
            "role": "system",
            "content": "Определи наиболее вероятное поколение мотоцикла по: brand, model, modification, year, cylcount, capacity, mtype. "
                "Отвечай строго в формате: 'Марка Модель (период лет выпуска этой модели)'. Как 'Yamaha YZF-R1 (2007 - 2008) "
                "Если модели имеют разные названия для рынков или известны под разными именами — объедини через слеш: 'ZX-6R / ZX600G (1998–1999)', GSX650F / Bandit 650. "
                "НЕ используй год из данных как ответ — найди период выпуска поколения. Разница между указанным в данных годом и крайними значениями тем что ты возвращаешь не может быть более 2 лет"
                "Учитывай возможные ошибки во вводе. Если модель не определить, то возвращай только н.д., без объяснений"
                "В первую очередь руководствуйся информацией с https://bikeswiki.ru/ и википедии"
                ""
                "Примеры:"
                "Ввод: Suzuki, GSF, , 1996, 4, 650, стрит → 'Suzuki GSF650 Bandit (1995–2004)'"
                "Ввод: Kawasaki, Ninja, zx6, 1995, 4, 600, спорт → 'Kawasaki Ninja ZX-6R (1990–1997)'"
                "Ввод: Kawasaki, Ninja, , 2006, 2, 649, спорт → 'Kawasaki Ninja 650 / ER-6f (2006–2017)"
                "Ввод: Honda, CBR, , 1993, 4, 600, спорт → 'Honda CBR600F2 (1991–1994)'"
                "Ввод: Yamaha, YZF, р 6, 2006, 4, 599, спорт → 'Yamaha YZF-R6 (2006–2007)"
        },
        {
            "role": 'user', 'content': f'Попробуй - {brand}, {model}, {modification}, {year}, {cylcount}, {capacity}, {mtype}'
        } 
        ]

    # Создание чата и его атрибуты
    chat = client.chat.completions.create(
        model= 'Llama-3.3-70B', #'Llama-4-Scout-T', #'Llama-3.1-405B' возвращает ошибку 500
        messages=messages,
        temperature=0,
        #max_tokens=64
    )
    
    # Вывод и подсчет количества потраченных токенов
    print(f'ключ на итерации - {api_key}')
    print(f'promt_tokens = {chat.usage.prompt_tokens}')
    token_usage += chat.usage.total_tokens
    llm_iter_counter += 1

    # Получение первого ответа
    return chat.choices[0].message.content, chat.usage.prompt_tokens

# Функция получения и записи номера продавца
def phone_get_request(id_value, conn):
    phone_url = f"https://api.av.by/offers/{id_value}/phones" 
    phone_response = requests.get(phone_url, headers=headers)
    p_to_write = []

    # Проверка успешности
    if phone_response.status_code == 200:
        phone_data = phone_response.json()  # Получаем джсон
        for phone in phone_data:
            p_country = phone['country']['label']
            p_code = phone['country']['code']
            p_number = phone['number']
            p_full_number = f"+{p_code}{p_number}"
            p_to_write.append(p_full_number)
        # пишем
        phone_query = """
        UPDATE av_full
        SET seller_ph_nr = %s
        WHERE id = %s
        """
        cursor = conn.cursor()
        cursor.execute(phone_query, (p_to_write, id_value))
        conn.commit()
        print(f"Для ID {id_value} записан(ы) номер(а): {p_to_write}")
        return 1
    else:
        print(f"Ошибка {phone_response.status_code}: {phone_response.text}")
        return 0