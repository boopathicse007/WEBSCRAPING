import re
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
from datetime import datetime
from urllib.parse import urlparse
import multiprocessing
from multiprocessing import Pool
import time

def get_region_from_url(url):
    parsed_url = urlparse(url)
    path_parts = parsed_url.path.split('/')
    if len(path_parts) > 1:
        return path_parts[1].upper()
    return None

def init_driver():
    options = webdriver.ChromeOptions()
    options.page_load_strategy = 'eager'
    options.add_argument("--headless")
    options.add_argument('--no-sandbox')
    options.add_argument('--window-size=1920x1080')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    driver = webdriver.Chrome(options=options)
    return driver

def get_book_details(isbn13):
    driver = init_driver()
    search_url = 'https://www.kobo.com/us/en/search?query=' + str(isbn13)
    driver.get(search_url)
    time.sleep(1)

    try:
        title_elements = driver.find_elements(By.CSS_SELECTOR, 'h1.title.product-field')
        if title_elements:
            title = title_elements[1].text
        else:
            title = None

    except:
        title = None


    try:
        author = driver.find_element(By.CLASS_NAME, 'contributor-name').text
    except:
        author = None

    try:
        price_meta = driver.find_element(By.CSS_SELECTOR, 'meta[property="og:price"]').get_attribute('content')
        currency_meta = driver.find_element(By.CSS_SELECTOR, 'meta[property="og:currency_code"]').get_attribute('content')
        price = re.sub(r'[^\d.]', '', price_meta)
        currency = currency_meta
    except:
        price, currency = None, None

    try:
        sales_rank = driver.find_element(By.CLASS_NAME, 'rank').text.replace('#', '')
    except:
        sales_rank = None
    
    try:
        rating = driver.find_element(By.CSS_SELECTOR, 'ul.stars.read-only').get_attribute('aria-label')
        rating = rating.split()[1]
    except:
        rating = None

    try:
        categories = [elem.text for elem in driver.find_elements(By.CSS_SELECTOR, '.category-rankings a')]
        categories = list(set(categories)) 
        categories = ", ".join(categories)
    except:
        categories = None

    try:
        widget = driver.find_element(By.ID, 'about-this-book-widget')
        pages = widget.find_elements(By.CSS_SELECTOR, '.stat-desc strong')[0].text
    except:
        pages = None

    try:
        hours_to_read = widget.find_elements(By.CSS_SELECTOR, '.stat-desc strong')[1].text
    except:
        hours_to_read = None

    try:
        total_words = widget.find_elements(By.CSS_SELECTOR, '.stat-desc strong')[2].text
    except:
        total_words = None

    try:
        metadata_widget = driver.find_element(By.CSS_SELECTOR, 'div[data-kobo-widget="BookItemDetailSecondaryMetadataWidget"]')

        ebook_details = metadata_widget.find_element(By.CSS_SELECTOR, 'div.bookitem-secondary-metadata')
        details = {}

        details['Publisher'] = ebook_details.find_elements(By.CSS_SELECTOR, 'ul > li')[0].text.strip()
        details['Release Date'] = ebook_details.find_elements(By.CSS_SELECTOR, 'ul > li')[1].find_element(By.CSS_SELECTOR, 'span').text.strip()
        details['Imprint'] = ebook_details.find_elements(By.CSS_SELECTOR, 'ul > li')[2].find_element(By.CSS_SELECTOR, 'span').text.strip()
        details['ISBN'] = ebook_details.find_elements(By.CSS_SELECTOR, 'ul > li')[3].find_element(By.CSS_SELECTOR, 'span').text.strip()
        details['Language'] = ebook_details.find_elements(By.CSS_SELECTOR, 'ul > li')[4].find_element(By.CSS_SELECTOR, 'span').text.strip()
        details['Download Options'] = ebook_details.find_elements(By.CSS_SELECTOR, 'ul > li')[5].text.split(":")[1].strip()

        supported_devices = metadata_widget.find_element(By.ID, 'readThisOn')
        supported_devices_list = [device.text.strip() for device in supported_devices.find_elements(By.CSS_SELECTOR, 'ul.supported-devices > li')]
        details['Supported Devices'] = ", ".join(supported_devices_list)

    except:
        details = {}

    region = get_region_from_url(driver.current_url)
    retailer = f'Kobo {region}'

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    book_details = {
        "region": region,
        "retailer": retailer,
        "isbn13": isbn13,
        "asin": None,
        "title_name": title,
        "authors": author,
        "sales_rank": sales_rank,
        "list_price": "",
        "print_list_price": "",
        "price": price,
        "currency": currency,
        "rating": rating,
        "categories": categories,
        "pages": pages,
        "hours_to_read": hours_to_read,
        "total_words": total_words,
        "publisher": details.get('Publisher'),
        "release_date": details.get('Release Date'),
        "imprint": details.get('Imprint'),
        "isbn": details.get('ISBN'),
        "language": details.get('Language'),
        "download_options": details.get('Download Options'),
        "supported_devices": details.get('Supported Devices'),
        "timestamp": timestamp,
        "url": driver.current_url
    }

    driver.quit()

    return book_details

def process_isbns_for_core(isbn_list):
    books_list = []
    driver = init_driver()
    for isbn in isbn_list:
        book_details = get_book_details(isbn)
        if book_details and book_details['title_name'] and book_details['title_name']!="" and len(book_details['title_name'])>0:
            books_list.append(book_details)
    driver.quit()
    return books_list

def process_isbn_list(isbn_list):
    total_cores = multiprocessing.cpu_count()
    cores_to_use = max(1, total_cores - 2)
    
    isbn_sublists = [isbn_list[i::cores_to_use] for i in range(cores_to_use)]
    pool = Pool(processes=cores_to_use)
    
    result_objects = [pool.apply_async(process_isbns_for_core, args=(isbn_sublists[i],)) for i in range(cores_to_use)]
    
    books_list = []
    for result in result_objects:
        books_list.extend(result.get())
    
    pool.close()
    pool.join()

    return books_list

start_time = time.time()
print(f"Start time: {time.strftime('%H:%M:%S', time.localtime(start_time))}")

isbn_df = pd.read_csv('isbn13_list.csv')
isbn_list = isbn_df['isbn13'].astype(str).tolist()

books_list = process_isbn_list(isbn_list)

books_list = [book for book in books_list if book]
df = pd.DataFrame(books_list)



df.set_index('isbn13', inplace=True)
df = df.reindex(isbn_list).reset_index()

df = df[df['title_name'].notna() & df['title_name'].str.strip().astype(bool)]

df.to_csv('book_result_data.csv', index=False)

end_time = time.time()
print(f"End time: {time.strftime('%H:%M:%S', time.localtime(end_time))}")

elapsed_time = end_time - start_time
minutes, seconds = divmod(elapsed_time, 60)

print(f"Elapsed time: {int(minutes)} minutes and {seconds:.2f} seconds")
