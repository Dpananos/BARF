import dotenv
import os
import json
import duckdb
import pandas as pd
import logging
import argparse

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("logs/ingest_data.log"), logging.StreamHandler()],
)


def ingest_weather_data(conn, data_dir: str):
    conn.execute("create database if not exists bike")
    conn.execute("use bike")
    conn.execute("create schema if not exists raw")
    conn.execute(
        f"create or replace table raw.raw_weather_data as (select *, now() as _etl_loaded_at from read_json('{data_dir}/*/*/*/weather-data.json'))"
    )

    conn.execute(
        f"create or replace table raw.raw_weather_forecast_Data as (select *, now() as _etc_loaded_at from read_json('{data_dir}/weather_forecasts/*.json'))"
    )

    conn.execute(
        f"create or replace table raw.raw_trip_data as  (select *, now() as _etl_loaded_at from read_json('{data_dir}/*/*/*/data.json', format='array'))"
    )
    conn.commit()


if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(
        description="Ingest JSON data into a DuckDB database."
    )
    parser.add_argument(
        "--root_dir",
        default="data",
        help="Path to the root directory containing JSON files. Default is 'data'.",
    )

    args = parser.parse_args()

    dotenv.load_dotenv(".env")
    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
    my_string = f"md:?motherduck_token={motherduck_token}"

    with duckdb.connect(my_string) as conn:
        ingest_weather_data(conn, args.root_dir)
        logging.info("Data ingestion process completed")
