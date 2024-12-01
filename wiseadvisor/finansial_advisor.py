import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from datetime import datetime
import time 
from helpers.utils import save_to_csv

HOST_NAME = "https://www.wiseradvisor.com"

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s | %(message)s')
def scrape_advisor_from_directory() -> list:
    logging.info("Scraping city/state directory page...")
    ua = UserAgent()
    url = "https://www.wiseradvisor.com/financial-advisors.asp"
    
    try:
        # Send an HTTP request to the webpage
        response = requests.get(url, headers={'User-Agent': ua.random})
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx, 5xx)
        
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract the list of advisor names
        states = soup.find('div', {'id': 'city-state'})
        li_elements = states.find_all('li')
        links = []
        for li in li_elements:
            link = li.find('a', href=True)
            links.append(link.get('href'))

        logging.info(f"Found {len(links)} city/state links.")
        
        # Introduce a delay between requests
        logging.info("Sleeping for 2 seconds.")
        time.sleep(2)  # Delay of 2 seconds
        return links
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None


def scrape_advisor_by_state_and_city(path, delay=2) -> list:
    ua = UserAgent()
    url = f"{HOST_NAME}{path}"
    
    try:
        # Send an HTTP request to the webpage
        response = requests.get(url, headers={'User-Agent': ua.random})
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx, 5xx)
        links = []
        
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract the advisor's name
        data = soup.find('div', {'id': 'first-sec-data'}).find('tbody')
        rows = data.find_all('tr')
        for row in rows:
            firm_link = row.find('div', {'class': 'firm-advisor'}).find('a', href=True)
            if firm_link:
                href = firm_link['href']
                links.append(href)
        
        logging.info(f"Found {len(links)} advisor links for {path}.")
        
        # Introduce a delay between requests
        logging.info(f"Sleeping for {delay} seconds.")
        time.sleep(delay)  # Delay of 2 seconds
        return links
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed for {path}: {e}")
        return None


def scrape_advisor(path, delay=2) -> dict:
    ua = UserAgent()
    url = f"{HOST_NAME}{path}"
    
    try:
        # Send an HTTP request to the webpage
        response = requests.get(url, headers={'User-Agent': ua.random})
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx, 5xx)
        
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        sections = soup.find_all('section', {'class': 'city'})
        data = sections[1]
        detail = data.find('div', {'class': 'col-lg-8'})
        
        name = detail.find('h1').text
        first_name = name.split(' ')[0].strip()
        last_name = ' '.join(name.split(' ')[1:]).strip()
        
        address = detail.find('div', {'style': ' margin: 10px 0px 18px'})
        address_lines = list(address.stripped_strings)
        street = ', '.join(address_lines[1:3])  # Skip the phone number at index 0
        telephone = address.find('div').text.replace('Tel:', '').strip()
        city_state = address.find('span').text
        city = city_state.split(',')[0].strip()
        state = city_state.split(',')[1].strip()
        
        info = {
            'First': first_name,
            'Last': last_name,
            'Street': street,
            'City': city,
            'State': state,
            'Telephone': telephone
        }
        logging.info(f"Scraped advisor: {first_name} {last_name}")
    
        # Introduce a delay between requests
        logging.info(f"Sleeping for {delay} seconds.")
        time.sleep(delay)  # Delay of 2 seconds
        return info
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed for {path}: {e}")
        return None
    except Exception as e:
        logging.error(f"Error scraping advisor details for {path}: {e}")
        return None

def main():
    # Scrape the directory for city/state links
    cities = scrape_advisor_from_directory()
    
    if not cities:
        logging.error("No cities found.")
        return

    # Scrape the advisors from each state/city link in parallel
    all_advisor_data = []
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        # First, gather links for each city/state
        logging.info("Scraping advisor links for each city/state...")
        future_links = [executor.submit(scrape_advisor_by_state_and_city, city, 5) for city in cities]
        future_links = [future_links[0]] # For testing with a single city
        
        # Collect all the advisor links from the futures
        advisor_links = []
        for future in as_completed(future_links):
            links = future.result()
            if links:
                advisor_links.extend(links)
        
        # Now, scrape each advisor's details in parallel
        logging.info("Scraping advisor details...")
        future_advisors = [executor.submit(scrape_advisor, link, 5) for link in advisor_links]
        
        # Collect all advisor data
        for future in as_completed(future_advisors):
            advisor_data = future.result()
            if advisor_data:
                all_advisor_data.append(advisor_data)
    
    # Save the collected data to a CSV file
    date_name = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f'financial_advisors_{date_name}.csv'
    save_to_csv(all_advisor_data, filename=filename)
    logging.info("Scraping completed and data saved to financial_advisors.csv.")

# Usage
if __name__ == '__main__':
    main()
