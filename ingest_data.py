import os
import json
import duckdb
import pandas as pd
import logging
import argparse

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("data_ingestion.log"),
                              logging.StreamHandler()])


def ingest_data_fact_tips(conn, file: str):
    logging.info(f"Starting data ingestion for file: {file}")
    try:
        logging.info("Connected to the DuckDB database")

        conn.sql("CREATE TABLE IF NOT EXISTS staging_fact_trips (filename VARCHAR, datetime DATETIME, station_trips BIGINT)")

        df = pd.read_json(file)
        
        # File will serve as our unique identifier.
        df['filename'] = file
        # API returns "station trips" -- with a space.
        df.columns = [col_name.replace(' ', '_') for col_name in df.columns]
        df = df[['filename','datetime','station_trips']]

        result = conn.execute(f"SELECT COUNT(*) FROM staging_fact_trips WHERE filename == '{file}'").fetchall()[0][0]
        logging.info(f"Number of existing records for {file}: {result}")

        if result == 0:
            conn.sql("INSERT INTO staging_fact_trips SELECT * FROM df")
            logging.info("Data inserted into staging_fact_trips table")
            conn.commit()
        else:
            logging.info("Record already ingested, not inserting new data.")

    except Exception as e:
        logging.error(f"Error during data ingestion for file {file}: {e}")


def crawl_directory(directory, conn):
    """Crawl through the directory and process JSON files."""
    logging.info(f"Starting to crawl directory: {directory}")
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)
                logging.info(f"Processing file: {file_path}")
                ingest_data_fact_tips(conn=conn, file=file_path)


if __name__ == '__main__':
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Ingest JSON data into a DuckDB database.")
    parser.add_argument('--db', default='data.db', help="Path to the DuckDB database file. Default is 'data.db'.")
    parser.add_argument('--root_dir', default='data/', help="Path to the root directory containing JSON files. Default is 'data/'.")

    args = parser.parse_args()

    # Execute the main functionality
    with duckdb.connect(database=args.db, read_only=False) as conn:
        crawl_directory(args.root_dir, conn)
        logging.info("Data ingestion process completed")
