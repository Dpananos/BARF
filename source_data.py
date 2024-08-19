import argparse
import json
import logging
import pathlib
from datetime import datetime, timedelta

import requests
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

from utils.utils import valid_date

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/source_data.log"),  # Log to a file
        logging.StreamHandler(),  # Log to the console
    ],
)

# Initialize S3 client
s3_client = boto3.client('s3')
BUCKET_NAME = 'barf-bike-data'

def file_exists_and_has_data(file_path: str) -> bool:
    '''
    Given a file path, checks if the file exists and contains data in S3.
    '''
    try:
        s3_client.head_object(Bucket=BUCKET_NAME, Key=file_path)
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_path)
        return response['ContentLength'] > 0
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            logging.error(f"Error checking S3 file: {e}")
            return False

def fetch_data(date: datetime):
    # Hard code the hours to be the start of the day
    start_date = date.strftime("%Y%m%d00")
    # Hard code the hours to be the end of the day
    end_date = date.strftime("%Y%m%d23")

    url = "https://api.raccoon.bike/activity"
    params = {
        "system": "bike_share_toronto",
        "start": f"{start_date}",
        "end": f"{end_date}",
        "frequency": "d",
    }

    response = requests.get(url, params=params)
    return response

def upload_to_s3(file_path: str, bucket_name: str, s3_key: str):
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        logging.info(f"Successfully uploaded {file_path} to s3://{bucket_name}/{s3_key}.")
    except FileNotFoundError:
        logging.error(f"The file {file_path} was not found.")
    except NoCredentialsError:
        logging.error("AWS credentials not available.")
    except PartialCredentialsError:
        logging.error("Incomplete AWS credentials provided.")
    except Exception as e:
        logging.error(f"Failed to upload {file_path} to S3: {str(e)}")

def fetch_and_write_data(path: pathlib.Path, date: datetime):
    # Data is partitioned by Year/Month/Day
    # Updated S3 path to include 'data' prefix
    s3_key = f"data/{date.strftime('%Y/%m/%d/data.json')}"

    # Check if a file already exists in this location and has something in it
    if file_exists_and_has_data(file_path=s3_key):
        skipped_date = date.strftime("%Y-%m-%d")
        logging.info(f"Data already exists for {skipped_date}. Skipping.")
        return None

    response = fetch_data(date=date)

    if response.status_code == 200:
        data = response.json()
        local_file_path = f"/tmp/{date.strftime('%Y-%m-%d')}-data.json"

        with open(local_file_path, "w") as data_file:
            json.dump(data, data_file, indent=4)

        # Upload to S3
        upload_to_s3(file_path=local_file_path, bucket_name=BUCKET_NAME, s3_key=s3_key)

        succeeding_date = date.strftime("%Y-%m-%d")
        logging.info(f"Successfully fetched and wrote data for {succeeding_date}.")

    else:
        failing_date = date.strftime("%Y/%m/%d")
        logging.error(
            f"Call failed for date = {failing_date} with status code {response.status_code} and message: {response.text}"
        )

def main(path: str, date: datetime):
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
