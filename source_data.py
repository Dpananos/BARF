import argparse
import json
import logging
import os
import pathlib
from datetime import datetime, timedelta

import requests

from utils.utils import file_exists_and_has_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("fetch_bike_data.log"),  # Log to a file
        logging.StreamHandler(),  # Log to the console
    ],
)


def fetch_data(date: datetime):
    start_date = date.strftime("%Y%m%d00")
    end_date = date.strftime("%Y%m%d23")
    url = "https://api.raccoon.bike/activity"
    params = {
        "system": "bike_share_toronto",
        "start": f"{start_date}",
        "end": f"{end_date}",
        "frequency": "h",
    }

    response = requests.get(url, params=params)

    return response


def fetch_and_write_data(
    path: pathlib.Path, date: datetime, overwrite=False, *args, **kwargs
):
    # Data is partitioned by Year/Month/Day
    # Where the data will be written
    data_dir = path / date.strftime("%Y/%m/%d")
    data_path = path / date.strftime("%Y/%m/%d/data.json")

    # Make a directory to store the data
    os.makedirs(data_dir, exist_ok=True)

    # Check if a file already exists in this location and has something in it
    if file_exists_and_has_data(file_path=data_path):
        skipped_date = date.strftime("%Y-%m-%d")
        logging.info(f"Data already exists for {skipped_date}. Skipping.")
        return None

    else:
        response = fetch_data(date=date)

        if response.status_code == 200:
            data = response.json()

            with open(data_path, "w") as data_file:
                json.dump(data, data_file, indent=4)

            succeeding_date = date.strftime("%Y-%m-%d")
            logging.info(f"Successfully fetched and wrote data for {succeeding_date}.")

        else:
            failing_date = date.strftime("%Y/%m/%d")
            logging.error(
                f"Call failed for date = {failing_date} with status code {response.status_code} and message: {response.text}"
            )

    return None

def valid_date(date_string:str):
    try:
        return datetime.strptime(date_string, "%Y-%m-%d")
    except ValueError:
        msg = f"Not a valid date: '{date_string}'. Expected format is YYYY-MM-DD."
        raise argparse.ArgumentTypeError(msg)

def main(path, date):

    logging.info(f'Running source_data with following arguments: path={path}, date={date.strftime("%Y-%m-%d")}')

    today = datetime.today()

    while date <= today:
        fetch_and_write_data(path=pathlib.Path(path), date=date)
        date += timedelta(days=1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and store bike share data.")

    parser.add_argument(
        "--path", 
        type=str, 
        help="The directory path to store data.",
        default='data'
    )

    parser.add_argument(
        "--date",
        type=valid_date,
        help="The start date in YYYY-MM-DD format.",
        default=datetime(2021, 1, 1)
    )

    args = parser.parse_args()

    main(path=args.path, date=args.date)
