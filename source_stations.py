import argparse
import json
import logging
import os
import pathlib
from datetime import datetime
import requests
from utils.utils import valid_date

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/source_stations.log"),  # Log to a file
        logging.StreamHandler(),  # Log to the console
    ],
)

def fetch_stations():
    logging.info("Attempting to access URL")
    
    url = "https://api.raccoon.bike/stations"
    params = {
        "system": "bike_share_toronto"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        logging.info("Data fetched successfully.")
        return response
    except requests.exceptions.HTTPError as err:
        logging.error(f"HTTP error occurred: {err}")
        raise
    except Exception as err:
        logging.error(f"An error occurred: {err}")
        raise

def fetch_and_write_stations(path: pathlib.Path, date:datetime):
    data_dir = path 
    data_path = path / date.strftime("%Y-%m-%d-stations.json")

    os.makedirs(path, exist_ok=True)

    response = fetch_stations()

    if response.status_code == 200:
        data = response.json()

        logging.info(f"Writing data to {data_path}")
        with open(data_path, 'w') as data_file:
            json.dump(data, data_file, indent=4)
        logging.info("Data written successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and store bike share station data.")

    parser.add_argument(
        "--path", 
        type=str, 
        help="The directory path to store data.",
        default='stations'
    )

    parser.add_argument(
        "--date",
        type=valid_date,
        help="The start date in YYYY-MM-DD format.",
        default=datetime.today()
    )

    args = parser.parse_args()

    logging.info("Begin fetching stations.")
    fetch_and_write_stations(path=pathlib.Path(args.path), date=args.date)
    logging.info("End fetching stations")
