import atexit
from concurrent.futures import ThreadPoolExecutor, wait
import csv
import json
import math
import os
import random
from threading import Lock
import time
import phonenumbers
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from selenium.common.exceptions import WebDriverException
from fake_useragent import UserAgent
from seleniumwire import webdriver
import seleniumwire.thirdparty.mitmproxy.exceptions as mitmproxy_exceptions

# Global variables
price_range = 100  # Price range for filtering products
timeout = 0  # Timeout for requests (in seconds)
retry_timeout = 60  # Timeout for retrying failed requests (in seconds)
max_retries = 5  # Maximum number of retries for failed requests
max_workers = os.cpu_count()  # Maximum number of concurrent Chrome instances
proxy_filename = 'proxy.txt'  # Filename for storing proxies
csv_filename = 'phone_numbers.csv'  # Filename for storing phone numbers
progress_filename = 'progress.json'  # Filename for storing progress

def init_driver(headless=True, user_agent_rotation=False, advanced_stealth=False):
    options = webdriver.ChromeOptions()
    
    if headless:
        options.add_argument("--headless=new")

    # Disable images
    options.add_argument("--blink-settings=imagesEnabled=false")

    # Disable GPU
    options.add_argument("--disable-gpu")

    # Disable notifications
    options.add_argument("--disable-notifications")

    # Disable automatic downloads
    options.add_argument("--disable-downloads")

    # Disable infobars
    options.add_argument("--log-level=3")

    # Additional arguments to improve performance
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-features=NetworkService")
    options.add_argument("--window-size=1920,1080")

    options.add_experimental_option("excludeSwitches", ['enable-logging'])

    # User agent rotation (optional)
    if user_agent_rotation:
        ua = UserAgent()
        user_agent = ua.random
        options.add_argument(f"user-agent={user_agent}")

    # Advanced stealth (optional)
    if advanced_stealth:
        # Install required libraries (e.g., antidetect_selenium)
        driver = None  # Replace with 'edge' as needed
    else:
        driver = webdriver.Chrome(options=options)

    return driver

def create_driver_pool(max_workers):
    driver_pool = []
    lock = Lock()  # Create a lock for thread-safe driver access

    for _ in range(max_workers):
        driver = init_driver(headless=True, user_agent_rotation=True)
        print("Created driver instance without proxy")
        with lock:
            driver_pool.append(driver)

    return driver_pool, lock

def rotate_driver(driver_pool, lock):
    with lock:
        driver = driver_pool.pop(0)
        driver_pool.append(driver)
    return driver
    
def read_proxies_from_file(file_path):
    proxy_list = []
    with open(file_path, 'r') as file:
        for line in file:
            proxy = line.strip()
            if proxy:
                proxy_list.append(proxy)

    return proxy_list

def fetch_url_with_retry(url, driver):
    for _ in range(max_retries):
        try:
            time.sleep(timeout)
            driver.get(url)
            html_source = driver.page_source

            return html_source
        except WebDriverException as e:
            if "429" in str(e):
                print(f"Received 429 Too Many Requests error.")
                driver.proxy = { }
                continue 
            else:
                print(f"An error occurred while fetching URL {url}: {e}")
                time.sleep(retry_timeout)
                continue

    else:
        print(f"Failed to fetch URL {url} after {max_retries} retries.")
        return None, driver


def is_valid_url(url, driver):
    html_source = fetch_url_with_retry(url, driver)
    if html_source:
        return True
    else:
        return False
    
def get_category_name(url, driver):
    html_source = fetch_url_with_retry(url, driver)
    if not html_source:
        return ""

    soup = BeautifulSoup(html_source, 'html.parser')

    # Find the div with class "allegro.listing.top.grid.container.breadcrumb"
    category_name_div = soup.find('div', {'data-box-name': 'allegro.listing.top.grid.container.breadcrumb'})
    if category_name_div:
        # Find all li elements inside the div
        li_elements = category_name_div.find_all('li')

        # Skip the first li element and get the text values of the remaining li elements
        category_names = [li.text.strip() for li in li_elements[1:]]

        # Join the category names with a space
        return " ".join(category_names)
    return ""

def get_page_count(url, driver):
    html_source = fetch_url_with_retry(url, driver)
    if not html_source:
        return []
    
    soup = BeautifulSoup(html_source, 'html.parser')

    # Find the div with aria-label="paginacja"
    page_count_element = soup.find('div', {'aria-label': 'paginacja'})
    if page_count_element:
        last_link = page_count_element.find_all('a')[-1]
        page_count_text = last_link.text
        page_count = int(page_count_text)
        return page_count
    return 1

def get_offer_urls(url, driver):
    html_source = fetch_url_with_retry(url, driver)
    if not html_source:
        return []
    
    soup = BeautifulSoup(html_source, 'html.parser')

    # Find the div with data-role="rightItems"
    right_items_div = soup.find('div', {'data-role': 'rightItems'})
    if not right_items_div:
        return []

    # Find all article elements within the right_items_div
    articles = right_items_div.find_all('article', recursive=True)

    offer_urls = []

    # Iterate over each article
    for article in articles:
        article_link = article.find('a')
        if article_link:
            relative_href = article_link.get('href')
            full_url = f"{base_url}{relative_href}"
            offer_urls.append(full_url)

    return offer_urls

def extract_phone_numbers(url, driver):
    html_source = fetch_url_with_retry(url, driver)
    if not html_source:
        return []
    
    soup = BeautifulSoup(html_source, 'html.parser')

    # Find the div with class "description" (nested inside other divs)
    description_div = soup.find('div', recursive=True, attrs={'data-box-name': 'Description container'})
    if not description_div:
        return []

    description_text = description_div.get_text(strip=True, separator='\n')

    phone_numbers = find_phone_numbers(description_text)

    # Remove duplicates from the phone_numbers list
    unique_phone_numbers = list(set(phone_numbers))

    return unique_phone_numbers

def scrape_offer(offer_url, category_name, driver, proxy_list):
    for _ in range(max_retries):
        try:
            proxy_url = "none"

            if driver.proxy == { }:
                proxy = random.choice(proxy_list)
                proxy_parts = proxy.split(':')
                proxy_host = proxy_parts[0]
                proxy_port = proxy_parts[1]
                proxy_user = proxy_parts[2]
                proxy_pass = proxy_parts[3]
                proxy_url = f"http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port}"
                driver.proxy = {
                    "http": proxy_url,
                    "https": proxy_url
                }
            phone_numbers = extract_phone_numbers(offer_url, driver)
            print(f"Phone numbers found: {phone_numbers}, URL: {offer_url}, proxy: {proxy_url}")
            save_phone_numbers(category_name, offer_url, phone_numbers)
            driver.proxy = { }
        except mitmproxy_exceptions.TcpDisconnect as e:
            print(f"Proxy error, retrying with a different proxy.")
            driver.proxy = { }
            continue
        except:
            print(f"Error occurred while checking URL {offer_url}")
            break
    
def find_phone_numbers(text):
    phone_numbers = []
    for match in phonenumbers.PhoneNumberMatcher(text, "PL"):
        phone_number = match.number
        if phonenumbers.is_valid_number(phone_number):
            phone_numbers.append(phonenumbers.format_number(phone_number, phonenumbers.PhoneNumberFormat.INTERNATIONAL))
    return phone_numbers

def save_phone_numbers(category, offer, phone_numbers):
    with open(csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        row = [category, offer, ", ".join(phone_numbers)]
        writer.writerow(row)

def save_progress(data):
    with open(progress_filename, "w") as f:
        json.dump(data, f)

def load_progress():
    if os.path.exists(progress_filename):
        with open(progress_filename, "r") as f:
            return json.load(f)
    else:
        return None
    
def load_previous_progress(progress, category_name):
    if progress and category_name == progress.get("category_name"):
        start_price = progress.get("start_price", 0)
        start_page = progress.get("start_page", 1)
        print(f"Previous progress found. Start price: {start_price}, Start page: {start_page}, Category name: {category_name}")
        
        load_progress = input("Do you want to load the previous progress? (y/n) ").lower()
        if load_progress == "y":
            print("Loading previous progress.")
            return start_price, start_page
        else:
            print("Starting fresh.")
            try:
                os.remove(csv_filename)
            except FileNotFoundError:
                pass
            return 0, 1
    else:
        print("No previous progress found or category name does not match.")
        try:
            os.remove(csv_filename)
        except FileNotFoundError:
            pass
        return 0, 1

def exit_handler():
    if driver_pool:
        print("Closing open drivers...")
        for driver in driver_pool:
            driver.quit()
        print("All drivers closed.")

if __name__ == "__main__":
    # Register the exit handler
    atexit.register(exit_handler)

    base_category_url = input("Enter the base category URL: ")
    base_url = f"{urlparse(base_category_url).scheme}://{urlparse(base_category_url).netloc}"

    proxy_list = read_proxies_from_file(proxy_filename)
    print(f"Proxies loaded: {len(proxy_list)}")

    print(f"Creating driver pool with {max_workers} workers...")
    driver_pool, lock = create_driver_pool(max_workers)
    print("Driver pool created.")

    # Check if the base URL is valid
    if is_valid_url(base_category_url, rotate_driver(driver_pool=driver_pool, lock=lock)):
        # Get the category name
        category_name = get_category_name(base_category_url, rotate_driver(driver_pool=driver_pool, lock=lock))
        print(f"Category name: {category_name}")

        # Load previous progress
        progress = load_progress()
        start_price, start_page = load_previous_progress(progress, category_name)
        
    else:
        print(f"Base URL is invalid: {base_category_url}")
        exit()

    for start_price in range(start_price, 1000000000, price_range):
        # Calculate the end price
        end_price = start_price + price_range

        if start_price > 0:
            start_price += 0.01

        # Construct the category URL with price filter
        price_filter = f"&price_from={start_price:.2f}&price_to={end_price:.2f}"
        category_url = base_category_url + "?order=p" + price_filter

        # Check if the category URL is valid
        if is_valid_url(category_url, rotate_driver(driver_pool=driver_pool, lock=lock)):
            # Get the page count
            page_count = get_page_count(category_url, rotate_driver(driver_pool=driver_pool, lock=lock))
            print(f"Price range: {start_price:.2f} - {end_price:.2f}, Page count: {page_count}, URL: {category_url}")

            for page_num in range(start_page, page_count + 1):
                page_filter = f"&p={page_num}"
                full_url = category_url + page_filter

                # Save progress
                progress = {
                    "start_price": math.floor(start_price),
                    "start_page": start_page,
                    "category_name": category_name
                }
                save_progress(progress)
                print("Progress saved.")

                # Get the offer URLs
                offer_urls = get_offer_urls(full_url, rotate_driver(driver_pool=driver_pool, lock=lock))
                print(f"Page {page_num}: {len(offer_urls)} offers found.")

                # Scrape the offers
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    all_futures = []
                    for offer_url in offer_urls:
                        future = executor.submit(scrape_offer, offer_url, category_name, rotate_driver(driver_pool=driver_pool, lock=lock), proxy_list)
                        all_futures.append(future)

                    # Wait for all futures to complete
                    results = wait(all_futures)
        else:
            print(f"Invalid URL: {category_url}")

    exit()