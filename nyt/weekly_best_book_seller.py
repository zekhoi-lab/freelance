import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging
from itertools import chain
from helpers.utils import save_to_csv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s | %(message)s')

def generate_weekly_dates(start_date: str, end_date: str):
    """
    Generate a list of weekly dates between start_date and end_date, aligned to Sundays.
    The end date will be adjusted by adding 1 week.
    """
    start_date_obj = datetime.strptime(start_date, "%Y/%m/%d")
    end_date_obj = datetime.strptime(end_date, "%Y/%m/%d")
    
    # Adjust the end date by adding one week
    end_date_obj += timedelta(weeks=1)

    # Adjust the start date to the previous Sunday (if it's not already Sunday)
    days_to_previous_sunday = (start_date_obj.weekday() + 1) % 7
    start_date_obj -= timedelta(days=days_to_previous_sunday)

    weekly_dates = []
    current_date = start_date_obj
    while current_date <= end_date_obj:
        weekly_dates.append(current_date.strftime("%Y/%m/%d"))
        current_date += timedelta(weeks=1)

    return weekly_dates

def scrape_nyt_best_sellers(week_date_str: str, delay: int = 2, retry=0) -> List[Dict[str, str]]:
    """
    Scrape the New York Times Best Sellers list for the given week.
    """
    url = f"https://www.nytimes.com/books/best-sellers/{week_date_str}/combined-print-and-e-book-nonfiction/"
    try:
        logging.info(f"Scraping data for week: {week_date_str}")
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for any HTTP errors

        # Parse the page content with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the <ol> element containing the books list
        topic_list = soup.find('ol', {'data-testid': 'topic-list'})
        books = []

        for item in topic_list:
            title = item.find('h3', class_='css-5pe77f')
            author = item.find('p', class_='css-hjukut')
            description = item.find('p', class_='css-14lubdp')
            weeks_on_list = item.find('p', class_='css-1o26r9v')

            book_info = {
                'Date': week_date_str,
                'Book Name': title.get_text(strip=True) if title else 'N/A',
                'Book Author': author.get_text(strip=True) if author else 'N/A',
                'Book Description': description.get_text(strip=True) if description else 'N/A',
                'Weeks on the List/New This Week': weeks_on_list.get_text(strip=True) if weeks_on_list else 'N/A'
            }
            books.append(book_info)

        logging.info(f"Successfully scraped {len(books)} books for week {week_date_str}. Sleeping for {delay} seconds.")
        time.sleep(delay)  # Delay between requests
        return books
    except requests.RequestException as e:
        logging.error(f"Failed to scrape data for week {week_date_str}: {e}")
        if retry < 3:
            logging.info(f"Retrying for week {week_date_str}...")
            time.sleep(delay)
            return scrape_nyt_best_sellers(week_date_str, delay, retry + 1)
        else:
            return []

def scrape_all_best_sellers(start_date: str, end_date: str, delay: int = 2):
    """
    Scrapes the best sellers for each week in the given range of dates in parallel.
    Tracks the process and provides feedback.
    """
    logging.info(f"Starting scraping from {start_date} to {end_date} with a delay of {delay} seconds.")
    weekly_dates = generate_weekly_dates(start_date, end_date)

    results = []
    total_weeks = len(weekly_dates)
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(scrape_nyt_best_sellers, week_date, delay): week_date for week_date in weekly_dates}

        for idx, future in enumerate(as_completed(futures), 1):
            week_date = futures[future]
            logging.info(f"Scraping week {idx} of {total_weeks}: {week_date}")
            try:
                week_data = future.result()
                results.append(week_data)
            except Exception as e:
                logging.error(f"Error scraping {week_date}: {e}")
            
            logging.info(f"Finished scraping week {idx}. Sleeping for {delay} seconds.")
            time.sleep(delay)  # Delay after processing each week

    logging.info(f"Scraping completed. Collected data for {len(results)} weeks.")
    return results

# Usage
def main():
    DELAY = 30
    start_date = '2019/01/01'  # Set your start date here
    end_date = '2024/12/01'    # Set your end date here

    logging.info("Starting the scraping process.")
    all_best_sellers = scrape_all_best_sellers(start_date, end_date, delay=DELAY)

    logging.info("Saving the scraped data to a CSV file.")
    date_name = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f'nyt_best_sellers_{date_name}.csv'
    data = list(chain.from_iterable(all_best_sellers))  # Flatten the list of lists
    save_to_csv(data, filename=filename)

    logging.info("Process completed successfully.")

if __name__ == '__main__':
    main()
