import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import csv
import os

def generate_weekly_dates(start_date: str, end_date: str):
    """
    Generate a list of weekly dates between start_date and end_date, aligned to Sundays.
    The end date will be adjusted by adding 1 week.
    """
    # Convert string dates to datetime objects
    start_date_obj = datetime.strptime(start_date, "%Y/%m/%d")
    end_date_obj = datetime.strptime(end_date, "%Y/%m/%d")
    
    # Adjust the end date by adding one week
    end_date_obj += timedelta(weeks=1)

    # Adjust the start date to the previous Sunday (if it's not already Sunday)
    days_to_previous_sunday = (start_date_obj.weekday() + 1) % 7
    start_date_obj -= timedelta(days=days_to_previous_sunday)

    # List to store weekly dates (Sundays)
    weekly_dates = []

    # Generate weekly dates starting from the adjusted start date
    current_date = start_date_obj
    while current_date <= end_date_obj:
        weekly_dates.append(current_date.strftime("%Y/%m/%d"))
        current_date += timedelta(weeks=1)

    return weekly_dates

def scrape_nyt_best_sellers(week_date_str: str) -> List[Dict[str, str]]:
    """
    Scrape the New York Times Best Sellers list for the given week.
    """
    url = f"https://www.nytimes.com/books/best-sellers/{week_date_str}/combined-print-and-e-book-nonfiction/"
    try:
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

        return books
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return []

def scrape_all_best_sellers(start_date: str, end_date: str, delay: int = 2):
    """
    Scrapes the best sellers for each week in the given range of dates in parallel.
    Tracks the process and provides feedback.
    """
    weekly_dates = generate_weekly_dates(start_date, end_date)

    results = []
    total_weeks = len(weekly_dates)
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(scrape_nyt_best_sellers, week_date): week_date for week_date in weekly_dates}

        for idx, future in enumerate(as_completed(futures), 1):
            week_date = futures[future]
            print(f"Scraping week {idx} of {total_weeks}: {week_date}")
            try:
                week_data = future.result()
                results.append(week_data)
            except Exception as e:
                print(f"Error scraping {week_date}: {e}")
            
            print(f"Finished scraping week {idx}. Sleeping for {delay} seconds.")
            time.sleep(delay)

    return results

def save_to_csv(data: List[List[Dict[str, str]]], filename: str):
    """
    Saves the scraped data to a CSV file in the current directory.
    """
    # Get the current directory of the script
    current_folder = os.path.dirname(os.path.abspath(__file__))
    folder_path = os.path.join(current_folder, 'result')  # Save to 'result' folder

    # Ensure the folder exists
    os.makedirs(folder_path, exist_ok=True)

    # Define the file path
    file_path = os.path.join(folder_path, filename)

    # Open the CSV file for writing
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["Date", "Book Name", "Book Author", "Book Description", "Weeks on the List/New This Week"])
        writer.writeheader()

        # Write the data to the CSV file
        for week_data in data:
            for book in week_data:
                writer.writerow(book)

    print(f"Data has been saved to {file_path}")

# Usage
start_date = '2019/01/01'  # Set your start date here
end_date = '2024/12/01'    # Set your end date here

all_best_sellers = scrape_all_best_sellers(start_date, end_date, delay=30)

# Save the scraped data to a CSV file
save_to_csv(all_best_sellers, 'nyt_best_sellers.csv')
