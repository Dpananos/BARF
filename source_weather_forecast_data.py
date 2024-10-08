import argparse
import json
import logging
import os
import pathlib
from datetime import datetime, timedelta
from time import sleep

import requests

from utils.utils import file_exists_and_has_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/source_data.log"),  # Log to a file
        logging.StreamHandler(),  # Log to the console
    ],
)


def fetch_weather_forecast_data(date: datetime):
    start_date = date.strftime("%Y-%m-%d")
    end_date = (date + timedelta(days=14)).strftime("%Y-%m-%d")

    weather_vars = [
            "temperature_2m",
            "relative_humidity_2m",
            "dew_point_2m",
            "apparent_temperature",
            "pressure_msl",
            "surface_pressure",
            "cloud_cover",
            "cloud_cover_low",
            "cloud_cover_mid",
            "cloud_cover_high",
            "wind_speed_10m",
            "wind_speed_80m",
            "wind_speed_120m",
            "wind_speed_180m",
            "wind_direction_10m",
            "wind_direction_80m",
            "wind_direction_120m",
            "wind_direction_180m",
            "wind_gusts_10m",
            "shortwave_radiation",
            "direct_radiation",
            "direct_normal_irradiance",
            "diffuse_radiation",
            "global_tilted_irradiance",
            "vapour_pressure_deficit",
            "cape",
            "evapotranspiration",
            "et0_fao_evapotranspiration",
            "precipitation",
            "snowfall",
            "precipitation_probability",
            "rain",
            "showers",
            "weather_code",
            "snow_depth",
            "freezing_level_height",
            "visibility",
            "soil_temperature_0cm",
            "soil_temperature_6cm",
            "soil_temperature_18cm",
            "soil_temperature_54cm",
            "soil_moisture_0_to_1cm",
            "soil_moisture_1_to_3cm",
            "soil_moisture_3_to_9cm",
            "soil_moisture_9_to_27cm",
            "soil_moisture_27_to_81cm",
            "is_day",
        ]
    
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 43.7001,
        "longitude": -79.4163,
        "start_date": start_date, 
        "end_date": end_date,
        "hourly": weather_vars,
        "current": weather_vars
    }

    response = requests.get(url, params=params)

    return response


def fetch_and_write_data(path: pathlib.Path, date: datetime, overwrite: bool = True):
    # Data is partitioned by Year/Month/Day
    data_dir = path / date.strftime("weather_forecasts")
    data_path = path / date.strftime("weather_forecasts/as-of-%Y-%m-%d-weather-forecast-data.json")

    # Make a directory to store the data
    os.makedirs(data_dir, exist_ok=True)

    # Check if a file already exists in this location and has something in it
    if file_exists_and_has_data(file_path=data_path) and not overwrite:
        skipped_date = date.strftime("%Y-%m-%d")
        logging.info(f"Data already exists for {skipped_date}. Skipping.")
        return None

    else:

        response = fetch_weather_forecast_data(date=date)

        if response.status_code == 200:
            data = response.json()
            data['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            with open(data_path, "w") as data_file:
                json.dump(data, data_file, indent=4)

            succeeding_date = date.strftime("%Y-%m-%d")
            logging.info(
                f"Successfully fetched and wrote data for {succeeding_date}."
            )

        else:
            failing_date = date.strftime("%Y/%m/%d")
            logging.error(
                f"Call failed for date = {failing_date} with status code {response.status_code} and message: {response.text}"
            )

    return None


def valid_date(date_string: str):
    try:
        return datetime.strptime(date_string, "%Y-%m-%d")
    except ValueError:
        msg = f"Not a valid date: '{date_string}'. Expected format is YYYY-MM-DD."
        raise argparse.ArgumentTypeError(msg)


def main(path, date, overwrite):
    logging.info(
        f'Running source_weather_forecast_data with following arguments: path={path}, date={date.strftime("%Y-%m-%d")}'
    )



    fetch_and_write_data(path=pathlib.Path(path), date=date, overwrite=overwrite)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and store weather data.")

    parser.add_argument(
        "--path", type=str, help="The directory path to store data.", default="data"
    )

    parser.add_argument(
        "--date",
        type=valid_date,
        help="The start date in YYYY-MM-DD format.",
        default=datetime.now().date(),
    )

    parser.add_argument(
        "--overwrite",
        type=bool,
        help="Overwrite any existing predictions",
        default=True
    )

    args = parser.parse_args()

    main(path=args.path, date=args.date,overwrite=args.overwrite)
