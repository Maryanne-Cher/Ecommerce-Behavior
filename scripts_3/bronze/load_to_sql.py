"""
Script: load_ecommerce_nov.py

Purpose:
- Load raw November ecommerce event data from CSV into SQL Server.
- Handles large files by reading in chunks.
- Converts timestamps and splits category codes for easier analysis.
- Ensures missing values (NaN/NaT) are inserted as SQL NULLs.

Usage:
- Update DATABASE_CONFIG and CSV_FILE paths as needed.
- Run the script to populate the 'bronze.ecommerce_nov' table.
"""

import pandas as pd
from sqlalchemy import create_engine

# -------------------------------
# Database configuration
# -------------------------------
DATABASE_CONFIG = {
    "server": "ATX11492",
    "database": "ecommerce_behavior",
    "driver": "ODBC Driver 17 for SQL Server"
}

# CSV file path
CSV_FILE = r"C:\Users\mmbesu\Desktop\_SQL projects\ecommerce behavior data project\November data\ecommerce_nov.csv"

# Chunk size for reading large CSV files
CHUNK_SIZE = 100000

# Target table and schema in SQL Server
TABLE_NAME = "ecommerce_nov"
SCHEMA_NAME = "bronze"

# -------------------------------
# Create SQLAlchemy engine
# -------------------------------
connection_string = (
    f"mssql+pyodbc://@{DATABASE_CONFIG['server']}/"
    f"{DATABASE_CONFIG['database']}?driver={DATABASE_CONFIG['driver'].replace(' ', '+')}"
    "&trusted_connection=yes"
)
engine = create_engine(connection_string)

# -------------------------------
# Load CSV into SQL Server in chunks
# -------------------------------
for i, chunk in enumerate(pd.read_csv(CSV_FILE, chunksize=CHUNK_SIZE)):
    
    # Convert event_time to datetime with UTC
    chunk['event_time'] = pd.to_datetime(chunk['event_time'], utc=True)

    # Extract date and time components for analysis
    chunk['event_date'] = chunk['event_time'].dt.date
    chunk['event_time_only'] = chunk['event_time'].dt.time

    # Split category_code into category and subcategory
    chunk[['category', 'subcategory']] = chunk['category_code'].str.split('.', n=1, expand=True)

    # Convert NaN/NaT to None to insert as SQL NULL
    chunk = chunk.where(pd.notnull(chunk), None)

    # Append chunk to SQL Server table
    chunk.to_sql(
        name=TABLE_NAME,
        schema=SCHEMA_NAME,
        con=engine,
        if_exists='append',
        index=False
    )
    
    # Log progress
    print(f"✅ Loaded chunk {i+1}")
