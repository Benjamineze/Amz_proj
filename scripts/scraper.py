import time
from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.proxy import Proxy, ProxyType
from bs4 import BeautifulSoup
import re
import os

# Set up the Selenium WebDriver with anti-detection options
def setup_driver(proxy=None):
    chrome_options = Options()
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--incognito")
    chrome_options.add_argument("--headless")  # Optional: run in headless mode
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    )

    if proxy:
        proxy_obj = Proxy()
        proxy_obj.proxy_type = ProxyType.MANUAL
        proxy_obj.http_proxy = proxy
        proxy_obj.ssl_proxy = proxy
        proxy_obj.add_to_capabilities(webdriver.DesiredCapabilities.CHROME)
        driver = webdriver.Chrome(options=chrome_options, desired_capabilities=webdriver.DesiredCapabilities.CHROME)
    else:
        driver = webdriver.Chrome(options=chrome_options)

    return driver

# Define the scraping function for products
def scrape_products(driver, soup, coll_date):
    products = soup.find_all('div', {'data-component-type': 's-search-result'})
    product_list = []

    for product in products:
        product_data = {}

        # Extract Product Name
        try:
            product_data['Product Name'] = product.h2.text.strip()
        except AttributeError:
            product_data['Product Name'] = None

        # Extract Ratings
        try:
            rating_element = product.find('span', {'class': 'a-icon-alt'})
            product_data['Ratings'] = rating_element.text.strip() if rating_element else None
        except AttributeError:
            product_data['Ratings'] = None

        # Get link to the individual product page for more details
        try:
            product_link = 'https://www.amazon.com' + product.h2.a['href']
            driver.get(product_link)
            time.sleep(2)  # Add a delay to let the page load
            product_soup = BeautifulSoup(driver.page_source, "html.parser")

            # Extract Product Category
            try:
                category_element = product_soup.find('a', {'class': 'a-link-normal a-color-tertiary'})
                product_data['Product Category'] = category_element.text.strip() if category_element else None
            except Exception as e:
                print(f"Error extracting product category: {e}")
                product_data['Product Category'] = None

            # Go back to the main search result page after scraping
            driver.back()
            time.sleep(2)  # Allow page to reload
        except Exception as e:
            print(f"Error accessing product link: {e}")
            product_data['Product Category'] = None

        # Extract Price
        try:
            price_element = product.find('span', {'class': 'a-price'})
            if price_element:
                price_whole = price_element.find('span', {'class': 'a-price-whole'}).text.strip()
                price_fraction = price_element.find('span', {'class': 'a-price-fraction'}).text.strip()
                product_data['Price'] = price_whole + price_fraction
            else:
                product_data['Price'] = None
        except AttributeError:
            product_data['Price'] = None

        # Extract Quantity Sold in Last Month (look for the phrase "bought in past month")
        try:
            qty_sold_text = product.find(string=re.compile(r'bought in past month'))
            if qty_sold_text:
                qty_sold_match = re.search(r'([\d,]+[kK]?\+?) bought in past month', qty_sold_text)
                if qty_sold_match:
                    qty_sold = qty_sold_match.group(1)
                    product_data['Qty Sold'] = qty_sold
                else:
                    product_data['Qty Sold'] = None
        except (AttributeError, IndexError):
            product_data['Qty Sold'] = None

        # Collection date for each product
        product_data['coll_date'] = coll_date

        product_list.append(product_data)
        
    return product_list

# Function to check if a next page exists and click it
def go_to_next_page(driver, soup):
    try:
        next_page = soup.find('li', {'class': 'a-last'})
        if next_page and next_page.find('a'):
            next_url = 'https://www.amazon.com' + next_page.find('a')['href']
            driver.get(next_url)
            time.sleep(5)  # Add delay to avoid being blocked
            return True
        else:
            return False  # No next page
    except Exception as e:
        print(f"Error navigating to next page: {e}")
        return False

# Main scraping logic
def main_scrape(proxy=None):
    # Set up driver
    driver = setup_driver(proxy)

    # Define the URL for trending products search
    url = "https://www.amazon.com/s?k=trending+products"
    driver.get(url)
    time.sleep(5)  # Allow page to load

    product_list = []

    # Get today's date as the collection date
    coll_date = datetime.today().strftime('%Y-%m-%d')

    while True:
        # Parse the page source with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Scrape products from the current page
        scraped_products = scrape_products(driver, soup, coll_date)
        if scraped_products:
            product_list.extend(scraped_products)
            print(f"Found {len(scraped_products)} products on the current page.")
        else:
            print("No products found on the current page.")

        # Check for pagination and go to the next page if available
        if not go_to_next_page(driver, soup):
            print("No more pages.")
            break

    # Close the driver
    driver.quit()

    # Save the data to a DataFrame and then to a CSV file
    if product_list:
        new_data = pd.DataFrame(product_list, columns=['Product Name', 'Product Category', 'Ratings', 'Price', 'Qty Sold', 'coll_date'])
        new_data.to_csv('amazon_trending_products.csv', index=False)
        print("Scraping complete, data saved to amazon_trending_products.csv")
        return new_data
    else:
        print("No products scraped, so no data was saved.")

# Run the scraper
if __name__ == "__main__":
    proxy = None  # Set proxy if needed, or leave as None 
    new_data = main_scrape(proxy)

    if new_data is not None:
        print("DataFrame successfully created!")
        
    else:
        print("No data was scraped.")
