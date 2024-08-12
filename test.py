import os
import json
import duckdb
import pandas as pd

# Path to the root data directory
root_dir = 'data/'

# Initialize DuckDB connection and create a database
conn = duckdb.connect(database='data.db', read_only=False)
conn.sql("CREATE TABLE trips (datetime DATETIME, trips BIGINT)")


def process_file(file_path):
    """Process a single JSON file and insert its data into DuckDB."""
    df = pd.read_json(file_path)
    conn.sql("INSERT INTO trips SELECT * FROM df")



def crawl_directory(directory):
    """Crawl through the directory and process JSON files."""
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)
                process_file(file_path)

# Start processing files
crawl_directory(root_dir)

# Commit changes and close the connection
conn.commit()
conn.close()

print('Database creation and data insertion complete.')
