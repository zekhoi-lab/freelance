import csv
import os
from typing import List, Dict

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