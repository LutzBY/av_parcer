import openai
import json
from openai import APIStatusError

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
                "НЕ используй год из данных как ответ — найди период выпуска поколения."
                "Учитывай возможные ошибки во вводе. Если модель не определить, то возвращай 'н.д.'"
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
        model= 'Llama-3.1-405B',
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