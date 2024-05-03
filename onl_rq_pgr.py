import requests
import json
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import re
import datetime
from datetime import datetime
import dateparser
import psycopg2
import smtplib
from email.mime.text import MIMEText
from email.utils import COMMASPACE

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

#Подключение к postgres
conn = psycopg2.connect(
    host = pgre_host,
    port = pgre_port,
    database = pgre_db,
    user = pgre_login,
    password = pgre_password
)

# Чтение текущей даты
current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"Привет! Текущая дата - {current_time_str}")

# Забираем последнюю дату из базы и количество стобцов
datecursor = conn.cursor()
datequery = "SELECT count(id), max(date) from mbonl_test"
datecursor.execute(datequery)
tmpfetch = datecursor.fetchone()
latest_ad_date = tmpfetch[1]
old_rows_count = tmpfetch[0]
print(f"Последняя дата объявления - {latest_ad_date.strftime('%Y-%m-%d %H:%M:%S')}")
datecursor.close()

# Функция отправки результата на email
def send_email(subject, body, recipient):
    sender = mail_login 
    password = mail_password
    smtp_server = 'smtp.yandex.ru'
    smtp_port = 465

    message = MIMEText(body)
    message['Subject'] = subject
    message['From'] = sender
    message['To'] = recipient

    try:
        server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        server.login(sender, password)
        server.sendmail(sender, recipient, message.as_string())
        server.quit()
        print(f'Email successfully sent to {recipient}')
    except Exception as e:
        print('Error sending email:', str(e))    

# Хэдеры для странцы search и для каждой страницы объяв (page)
headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,be-BY;q=0.6,be;q=0.5',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Length': '75',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Cookie': '_ym_uid=1544705841212306891; ab_tracking_id=sNzsrfFtRjVY6dML; a1_disabled=1; onl_session=dFFvWExHa3ZObHlxRHVEVlRaTnFBYmVuWElZSGF6YjlRYzJDOEt5eEFEMlV1QytER3R6YzRjeWNYNnUraTU0aCtpaHlzUnY5cGNIdXhRRFNpSWF6eis3R3VXZFc5UklWTjhyNWxLQzk0NUtSVHRwNEZhYXFabjY1cytXaEhNaGs1MlQ0TzBhWjQvS21MM29WZSs4bG5RPT0%3D; refresh_token=OEJVdHIydXlBMm9SNCt3dys0algzS29sUG9JT05Tb1lyN3EvcTdFVEVUN01NTmw3Qm1mbWIrSjl5aFVrMTZwNw==; fingerprint=c0da18b61a61b55ce01c0e7c69999bd5; PHPSESSID=1517652828dc319babbb96eed2b8c4d1; ouid=snyBDGCSizszUg9eEAJEAg==; __gads=ID=580015c7bd23aff0:T=1594702918:R:S=ALNI_MbWBy7hmSKj56imPd3Tw-dNZoSeYA; _ga_BT7DBB79XJ=GS1.1.1653043527.18.1.1653043568.0; tmr_lvid=5369ded0d05e6471d91a3adb69d8874d; tmr_lvidTS=1544705840436; delivery_boarding_showed=true; _ga_5ET8V1N9SR=GS1.1.1681904355.20.1.1681904409.0.0.0; _ga_32KPWHT0K8=GS1.1.1686039029.36.0.1686039029.0.0.0; _ga_64XDN24MMX=GS1.1.1687152413.1.1.1687152704.57.0.0; stid=a8984d1ed27e24e288a03cdaa11dedf2edc630d14657d29ca8d8f28c6ea22302; _ga=GA1.1.221970240.1544705841; fpestid=IISubuA1GTwjbnW-yhBXWXPqlTI3IurpAhDt1pMM68OcE0JOzuAdm1FnO2WIQYQADJYotw; _ym_d=1704690978; mindboxDeviceUUID=f5a70dd0-d0d6-4aa9-981f-090ee261b7c3; directCrm-session=%7B%22deviceGuid%22%3A%22f5a70dd0-d0d6-4aa9-981f-090ee261b7c3%22%7D; compare=%5B%22mpgb550gamingplu%22%2C%22b550apro%22%5D; _ga_KRWRWJPXZ2=GS1.1.1706768515.34.1.1706768627.0.0.0; _ga_9DJMZ0LZRD=GS1.1.1709182449.399.0.1709182459.50.0.0; _ga_9FSEQ8JYKN=GS1.1.1709183789.27.1.1709183998.0.0.0; _ga_KPSB9MHYED=GS1.1.1709183789.52.1.1709183998.60.0.0; _ga_5HNFCB8DR9=GS1.1.1713514070.587.1.1713514153.60.0.0; _gcl_au=1.1.1234277736.1713787471; _ga_4Y6NQKE48G=GS1.1.1713787471.592.1.1713787526.5.0.0; oss=eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjozMDY4NjEsInVzZXJfdHlwZSI6InVzZXIiLCJmaW5nZXJwcmludCI6ImQ1YzYyMzkyNWQ2YjNhOTcxYjE3MTJlMzExNzNmMDA1IiwiZXhwIjoyMDI5MzE0MTYyLCJpYXQiOjE3MTM5NTQxNjJ9.dZvtaBJ8hrQWKjk3c7ntGD38CiOdSBkKmccqPoEaVH5xSIevyqQq57uagy9tWK0woQyFUegiMFKHxRnr1uXPJQ; logged_in=1; _ga_SMLMFQCWFM=GS1.1.1713954154.80.0.1713954162.52.0.0; _ym_isad=1; _ym_visorc=b; _ga_NG54S9EFTD=GS1.1.1714638904.1212.1.1714639389.60.0.0; _ga_FT8E0R2RSY=GS1.1.1714638904.303.1.1714639389.60.0.0; ADC_REQ_2E94AF76E7=51E27AF2DA1B2526AC7475983E27021287D995908BE9E4E887781083080D0FD4007EDC0FF808E0B7',
    'Host': 'mb.onliner.by',
    'Origin': 'https://mb.onliner.by',
    'Pragma': 'no-cache',
    'Referer': 'https://mb.onliner.by/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"'
}

# Функция парсинга каждой страницы
def parse_page (url_page): 
    url_page = url_page
    response_page = requests.get(url_page, headers=headers_page)
    page = response_page.text
    soup = BeautifulSoup(page, 'lxml')
    
    # Извлечение нужных элементов
    # Марка и модель
    element = soup.find("span", class_="autoba-fastchars-ttl")
    brand = element.find("strong").string
    model_misc = element.find("strong").find_next_sibling(string=True)
    model_misc = model_misc.strip() if model_misc else ""
    # Год
    element = soup.find("span", class_="year")
    year = element.find("strong").string 
    year = int(year)
    # Пробег
    element = soup.find("span", class_="dist")
    mileage = element.find("strong") 
    if mileage.text == 'новый':
        mileage = 0
    else:
        mileage = element.find("strong").text
        mileage = mileage.replace(" ", "")
        mileage = int(mileage)
    # Цена
    price = soup.find("small", class_="other-costs")
    cleaned_string = re.sub('<.*?>', '', price.text) 
    cleaned_string = re.sub('[^\\d]', '', price.text)
    price = int(cleaned_string) 
    # Тип, объем, локация, тип двигла, цилиндры, привод из инфо
    element = soup.find("div", class_="content")
    element = soup.find_all("p", class_= None)
    result = []
    for tag in element:
        if tag.p:
            result.append(tag.p.text)
        else:
            result.extend(tag.text.split(', '))
    if isinstance(result[3], str):
        mtype = result[3]
    else:
        mtype = ""
    
    displacement = result[4]
    displacement = int(re.sub(r'\sсм³', '', displacement))
    location = result[8]
    eng_type = result[5]
    eng_cyl = result[6]
    eng_cyl = re.split(r'-цилиндровый', eng_cyl)
    eng_cyl = int(eng_cyl[0])
    drive = result[7]
    # Продавец
    seller = soup.find("span", class_="c-bl")
    seller = seller.find("strong").string
    seller = str(seller)
    # телефон
    element = soup.find('div', class_='col col-r second-col')
    phone = element.find("span", class_="c-bl").string
    # айди 
    element = soup.find('td', class_='bd numb').string
    id = re.split(r'№', element)
    id = int(id[1])
    # Дата
    element_date = soup.find('small', class_='msgpost-date')
    if element_date.span:
        # Если есть вложенный тег span, содержащий информацию о редактировании
        element_date_text = element_date.get_text(strip=True, separator=' ')
        element_date_text = re.sub(r'Редактировалось.+', '', element_date_text)  # Удаляем информацию о редактировании
    else:
        # Если нет вложенного тега span
        element_date_text = element_date.get_text(strip=True)

    date = dateparser.parse(element_date_text)
    date_str = date.strftime('%Y-%m-%d %H:%M:%S')

    print(f'ID - {id}, Дата - {date}, Модель - {brand} {model_misc}, Год - {year}, Пробег - {mileage}, Цена - {price}, Тип - {mtype}, Объем - {displacement}, Место - {location}, Тип двигла - {eng_type}, Цил - {eng_cyl}, Привод - {drive}, Продавец - {seller}, Телефон - {phone}, Ссылка - {url_page}')
    
    # Скрипт для записи в постгрес
    parsecursor = conn.cursor()
    parsequery = """
        INSERT INTO mbonl_test(id, date, brand, model_misc, year, mileage, price, mtype, displacement, location, eng_type, eng_cyl, drive, seller, phone, url)
        VALUES ( %s, '%s', '%s', '%s', %s, %s, %s, '%s', %s, '%s', '%s', %s, '%s', '%s', '%s', '%s')
        ON CONFLICT (id) DO UPDATE 
        SET 
        price = excluded.price
        WHERE mbonl_test.id = excluded.id;
    """ % (id, date, brand, model_misc, year, mileage, price, mtype, displacement, location, eng_type, eng_cyl, drive, seller, phone, url_page)
    parsecursor.execute(parsequery)
    return date # Вернуть дату объвяления для сверки

# Глобальные переменные и счетчики
page_counter = 1
ads_parced = 0

while True:
    # Получение ссылок на странице из джсона который в XHR search
    payload = {
        'min-price': '1000',
        'min-capacity': '400',
        'currency': 'USD',
        'sort[]': 'creation_date',
        'page': f'{page_counter}',
        'type[0][1]=': ''
    }
    headers_page = {
        'authority': 'moto.av.by',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'ru,en;q=0.9',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "YaBrowser";v="24.1", "Yowser";v="2.5"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 YaBrowser/24.1.0.0 Safari/537.36',
    }

    url_json = 'https://mb.onliner.by/search'
    response = requests.post(url_json, headers=headers, data=payload)
    data = response.json()

    content = response.json().get("result").get("content")
    pattern = r'<a href="/moto/(\d+)#'
    links = re.findall(pattern, content)

    # Цикл перехода по каждой объяве на странице с использованием функции parse_page
    print(f'Страница - {page_counter}')
    for link in links:
        url_page = 'https://mb.onliner.by/moto/' + link
        date = parse_page (url_page)
        ads_parced +=1
    conn.commit() # Записать страницу в пгре

    # Сверка дат
    if latest_ad_date > date:
        print ("Более старая дата найдена")
        break
    page_counter += 1

conn.close()

# Параметры отправки на email
mail_contents = (f"Привет!\nДата начала - {current_time_str}\nВ базе {old_rows_count} строк\nСамая старая дата - {latest_ad_date}\nПарсинг прошел успешно, обработано {ads_parced} штук.")
subject = 'Результат работы скриптов. №3 Парсинг онлайнера'
for recipient in recipients:
    send_email(subject, mail_contents, recipient)