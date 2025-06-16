from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
import time
import json
import datetime
from datetime import datetime
import dateparser
import pandas as pd
import psycopg2
import smtplib
from email.mime.text import MIMEText
from email.utils import COMMASPACE
import sys
import io

#Блок описания адресов
site = "https://mb.onliner.by/#min-price=1000&state[]=2&state[]=1&currency=USD&sort[]=creation_date&page=1"

table = "//body[1]/div[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[3]/div[1]/div[1]/div[2]/div[2]/table[1]"
table_urls = ".//td[3]/div[1]/p[1]/a[1]"
page_datetime = "//body/div[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[3]/div[1]/div[1]/ul[1]/li[1]/div[2]/div[1]/small[1]"
page_ids = "//body[1]/div[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[3]/div[1]/div[1]/div[1]/div[2]/table[1]/tbody[1]/tr[1]/td[1]"
page_titles = "//body[1]/div[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[3]/div[1]/div[1]/ul[1]/li[1]/div[2]/div[1]/div[1]/p[1]/span[1]/strong[1]"
page_years = "//body[1]/div[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[3]/div[1]/div[1]/ul[1]/li[1]/div[2]/div[1]/div[1]/p[1]/span[2]/span[1]/strong[1]"
page_odos = "//body[1]/div[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[3]/div[1]/div[1]/ul[1]/li[1]/div[2]/div[1]/div[1]/p[1]/span[2]/span[2]/strong[1]"
page_capacitys = "//body[1]/div[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[3]/div[1]/div[1]/ul[1]/li[1]/div[2]/div[1]/div[1]/p[2]/strong[1]"
page_prices = "//body[1]/div[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[2]/div[2]/span[1]/small[1]"
page_phones = "//body[1]/div[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[3]/div[1]/div[1]/ul[1]/li[1]/div[2]/div[1]/div[1]/div[2]/div[6]/div[2]/span[1]"
page_names = "//body[1]/div[1]/div[1]/div[1]/div[2]/div[2]/div[1]/div[3]/div[1]/div[1]/ul[1]/li[1]/div[2]/div[1]/div[1]/div[2]/div[6]/div[1]/span[1]/strong[1]"

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

current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"Привет! Текущая дата - {current_time_str}")

# Забираем последнюю дату и количество стобцов
datecursor = conn.cursor()
datequery = "SELECT count(id), max(date_page) from mbonl"
datecursor.execute(datequery)
tmpfetch = datecursor.fetchone()
latest_ad_date = tmpfetch[1]
old_rows_count = tmpfetch[0]
print(f"Последняя дата объявления - {latest_ad_date.strftime('%Y-%m-%d %H:%M:%S')}")
datecursor.close()


total_tuples = 0
page_number = 1


#Функция поиска по открытой страничке
def scrape_data_on_page(driver, page_datetime, page_ids, page_titles, page_years, page_odos, page_capacitys, page_prices, page_phones, page_names, table_link):
    
    global total_tuples
    #Команды найти
    date_times = driver.find_elements(By.XPATH, page_datetime)
    ids = driver.find_elements(By.XPATH, page_ids)
    titles = driver.find_elements(By.XPATH, page_titles)
    years = driver.find_elements(By.XPATH, page_years)
    odos = driver.find_elements(By.XPATH, page_odos)
    capacitys = driver.find_elements(By.XPATH, page_capacitys)
    prices = driver.find_elements(By.XPATH, page_prices)
    phones = driver.find_elements(By.XPATH, page_phones)
    names = driver.find_elements(By.XPATH, page_names)

    #формирование пустого хранилища данных
    data_page = []

    #упаковка кортежей
    for date_time, id, title, year, odo, capacity, price, phone, name, url in zip (date_times, ids, titles, years, odos, capacitys, prices, phones, names, table_link):
        
        dateclean = date_time.text.split(' Редактировалось')[0]
        datecleaned = dateparser.parse(dateclean)
        #dateclean = dateclean.strftime('%Y-%m-%d %H:%M:%S')
        priceclean = price.text.split(' $')[0]
        priceclean = priceclean.replace(" ", "")
        id = id.text
        title = title.text
        year = year.text
        odo = odo.text.replace(' ', '')
        capacity = capacity.text
        phone = phone.text
        name = name.text
        id = id.replace('№', '')
        if odo == 'новый':
            odo = 0

        data_tuple = (f"Datetime - {datecleaned} ID - {id}, Model -  {title}, Year -  {year}, Odo - {odo}, Cap - {capacity} Price - {priceclean}, Phone -  {phone}, Seller - {name}, URL - {table_link}")
                
        #наполнение дата пейдж содержимым кортежей
        data_page.append(data_tuple)
                
        total_tuples += 1

        # Скрипт для пгри
        parsecursor = conn.cursor()
        parsequery = """
            INSERT INTO mbonl(id, parse_date, date_page, model, year, odo, capacity, price, phone, seller, url)
            VALUES ( %s, '%s', '%s', '%s', %s, %s, %s, %s, '%s', '%s', '%s')
            ON CONFLICT (id) DO UPDATE 
            SET 
            price = excluded.price
            WHERE mbonl.id = excluded.id;
        """ % (id, current_time_str, datecleaned, title, year, odo, capacity, priceclean, phone, name, table_link)
        parsecursor.execute(parsequery)
    return data_page

# Инициализация драйвера и установка ему открытия без картинок
options = Options()
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--blink-settings=imagesEnabled=false')
driver = webdriver.Chrome(options=chrome_options)
driver.get(site)  

# Поиск таблицы, переход по ссылке
table_find = driver.find_element(By.XPATH, table)
url_buttons = driver.find_elements(By.XPATH, table_urls)
time.sleep(1)

#Поочередное открытие ссылок, проход по ним
def open_links_and_scrape_each_page(driver, url_buttons, latest_ad_date):
    for url_button in url_buttons:
        #Излекаем URL из гиперссылки
        table_link = url_button.get_attribute("href")
        driver.execute_script("window.open('{}', '_blank');".format(table_link))

        time.sleep(2)
        
        # Переключаемся на новую вкладку
        driver.switch_to.window(driver.window_handles[-1])

        current_page_date = dateparser.parse(driver.find_element(By.XPATH, page_datetime).text.split(' Редактировалось')[0])
        if current_page_date > latest_ad_date:    
            #Исполнение заранее заданной функции scrape
            data_page = scrape_data_on_page(driver, page_datetime, page_ids, page_titles, page_years, page_odos, page_capacitys, page_prices, page_phones, page_names,table_link)
            #Печать
            print(data_page)
            # Закрываем текущую вкладку
            driver.close()
            # Переключаемся обратно на исходную вкладку
            driver.switch_to.window(driver.window_handles[0])
        else:
            print("Повторяющееся объявление найдено")
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
            return

#Цикл перебора страниц
while True:
      # Использование драйвера для поиска таблицы по XPath
    table_find = driver.find_element(By.XPATH, table)
    url_buttons = driver.find_elements(By.XPATH, table_urls)
    
    # Сбор данных на текущей странице
    latest_ad_date = open_links_and_scrape_each_page(driver, url_buttons, latest_ad_date)

    if not latest_ad_date:
        print ("Все новые объявления обработаны")
        break

    # Попытка найти кнопку "Следующая страница"
    css_selector_next_button = "li.page-next"
    try:
        next_button = driver.find_element(By.CSS_SELECTOR, css_selector_next_button)
        next_button.click()
        time.sleep(4)  # Для ожидания загрузки страницы
        page_number += 1  # Увеличиваем номер страницы для следующей итерации
        print(f"Page - {page_number}, Adverts count: {total_tuples}")
    except NoSuchElementException:
        print("Последняя страница обработана. Программа завершает работу.")
        break  # Если кнопка "Следующая страница" не найдена, выходим из цикла

print(f"Выполнено. В базе было {old_rows_count} объявлений; Записано {total_tuples} новых")
conn.commit()
conn.close()