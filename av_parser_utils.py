import openai
import json
from openai import APIStatusError

# Счетчики
token_usage = 0
llm_iter_counter = 0

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
                "Если модели имеют разные названия для рынков — объедини через слеш: 'ZX-6R / ZX600G (1998–1999)'. "
                "Учитывай возможные ошибки во вводе. Если модель не определилась ставь 'н.д.'"
        },
        {
            "role": 'user', 'content': f'Попробуй - {brand}, {model}, {modification}, {year}, {cylcount}, {capacity}, {mtype}'
        } 
        ]

    # Создание чата и его атрибуты
    chat = client.chat.completions.create(
        model="Llama-3.1-405B", # GPT-4o, Claude-Sonnet-4, Gemini-2.5-Pro, Grok-4, gpt-3.5-turbo
        messages=messages,
        temperature=0,
        #max_tokens=64
    )
    
    # Вывод и подсчет количества потраченных токенов
    print(f'ключ на итерации - {api_key}')
    print(f'completion_ = {chat.usage.completion_tokens}, prompt_ = {chat.usage.prompt_tokens}')
    token_usage += chat.usage.prompt_tokens
    llm_iter_counter += 1

    # Получение первого ответа
    return chat.choices[0].message.content