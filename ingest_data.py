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

        conn.sql("CREATE TABLE IF NOT EXISTS fact_trips (datetime DATETIME, trips BIGINT)")
        df = pd.read_json(file)

        max_date = df.datetime.max().strftime('%Y-%m-%d')
        logging.info(f"Max date in the file: {max_date}")

        result = conn.execute(f"SELECT COUNT(*) FROM fact_trips WHERE datetime > '{max_date}'").fetchall()[0][0]
        logging.info(f"Number of existing records after {max_date}: {result}")

        if result == 0:
            conn.sql("INSERT INTO fact_trips SELECT * FROM df")
            logging.info("Data inserted into fact_trips table")
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
