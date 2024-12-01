import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import concurrent.futures
import logging
from typing import List, Dict
import time  # Import time module for adding delays
from helpers.utils import save_to_csv

HOST_NAME = "https://www.wiseradvisor.com"

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s | %(message)s')

def scrape_advisor_from_directory() -> list:
    logging.info("Scraping city/state directory page...")
    ua = UserAgent()
    url = "https://www.wiseradvisor.com/financial-advisors.asp"
    
    # Send an HTTP request to the webpage
    response = requests.get(url, headers={'User-Agent': ua.random})
    
    # Introduce a delay between requests
    time.sleep(2)  # Delay of 2 seconds
    
    # Check if the request was successful
    if response.status_code == 200:
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
        return links
    else:
        logging.error(f"Failed to retrieve the page. Status code: {response.status_code}")
        return None


def scrape_advisor_by_state_and_city(path, delay=2) -> list:
    ua = UserAgent()
    url = f"{HOST_NAME}{path}"
    
    # Send an HTTP request to the webpage
    response = requests.get(url, headers={'User-Agent': ua.random})
    links = []
    
    # Introduce a delay between requests
    time.sleep(delay)  # Delay of 2 seconds
    
    # Check if the request was successful
    if response.status_code == 200:
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
        return links
    else:
        logging.error(f"Failed to retrieve the page for {path}. Status code: {response.status_code}")
        return None


def scrape_advisor(path, delay=2) -> dict:
    ua = UserAgent()
    url = f"{HOST_NAME}{path}"
    
    # Send an HTTP request to the webpage
    response = requests.get(url, headers={'User-Agent': ua.random})
    
    # Introduce a delay between requests
    time.sleep(delay)  # Delay of 2 seconds
    
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        sections = soup.find_all('section', {'class': 'city'})
        data = sections[1]
        detail = data.find('div', {'class': 'col-lg-8'})
        
        name = detail.find('h1').text
        first_name = name.split(' ')[0].strip()
        last_name = name.split(' ')[1]
        
        address = detail.find('div', {'style': ' margin: 10px 0px 18px'})
        address_lines = list(address.stripped_strings)
        street = ', '.join(address_lines[1:3])  # Skip the phone number at index 0
        telephone = address.find('div').text.replace('Tel:', '').strip()
        city_state = address.find('span').text
        city = city_state.split(',')[0].strip()
        state = city_state.split(',')[1].strip()
        
        info = {
            'first': first_name,
            'last': last_name,
            'street': street,
            'city': city,
            'state': state,
            'telephone': telephone
            }
        logging.info(f"Scraped advisor: {first_name} {last_name}")
        return info
    else:
        logging.error(f"Failed to retrieve the page for {path}. Status code: {response.status_code}")
        return None


def main():
    # Scrape the directory for city/state links
    cities = scrape_advisor_from_directory()
    
    if not cities:
        logging.error("No cities found.")
        return

    # Scrape the advisors from each state/city link in parallel
    all_advisor_data = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # First, gather links for each city/state
        logging.info("Scraping advisor links for each city/state...")
        future_links = [executor.submit(scrape_advisor_by_state_and_city, city) for city in cities]
        
        # Collect all the advisor links from the futures
        advisor_links = []
        for future in concurrent.futures.as_completed(future_links):
            links = future.result()
            if links:
                advisor_links.extend(links)
        
        # Now, scrape each advisor's details in parallel
        logging.info("Scraping advisor details...")
        future_advisors = [executor.submit(scrape_advisor, link) for link in advisor_links]
        
        # Collect all advisor data
        for future in concurrent.futures.as_completed(future_advisors):
            advisor_data = future.result()
            if advisor_data:
                all_advisor_data.append(advisor_data)
    
    # Save the collected data to a CSV file
    save_to_csv(all_advisor_data, filename='financial_advisors.csv')
    logging.info("Scraping completed and data saved to financial_advisors.csv.")

# Usage
if __name__ == '__main__':
    main()
