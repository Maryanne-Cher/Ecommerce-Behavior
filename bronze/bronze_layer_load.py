"""
================================================================================
Project: E-commerce Analytics
File: bronze_layer_load.py
Purpose: Loads raw e-commerce behavior data into the Bronze layer of the ETL 
         pipeline in test mode (10,000 rows). This is the first stage of data 
         ingestion before any transformations or aggregations.
Functions:
    - load_csv_to_bronze() : Reads CSV, cleans data, and appends to Bronze table.
    - bronze_flow()         : Prefect flow to orchestrate the Bronze layer task.
Notes:
    - 'event_time' is converted to datetime without timezone.
    - NaN values are replaced with None for SQL compatibility.
    - Append mode is used to avoid overwriting existing Bronze data.
================================================================================
"""

# =================================================
# Imports
# =================================================
import pandas as pd
from prefect import flow, task
from sqlalchemy import create_engine

# =================================================
# Configuration
# =================================================
CSV_FILE = r"C:\Users\mmbesu\Desktop\_SQL projects\ecommerce behavior data project\November data\2019-Nov.csv"

# Database engine (replace with your actual connection string)
engine = create_engine("mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server")


# =================================================
# Task: Load CSV to Bronze
# =================================================
@task
def load_csv_to_bronze():
    """
    Loads the first 10,000 rows into the Bronze layer for testing.
    Cleans and transforms data:
      - Converts 'event_time' to datetime (removes timezone)
      - Replaces NaN with None for SQL compatibility
    """
    # Read only the first 10,000 rows
    df = pd.read_csv(CSV_FILE, nrows=10_000)

    # Convert 'event_time' to datetime (UTC-aware) and remove timezone info
    df['event_time'] = pd.to_datetime(df['event_time'], utc=True).dt.tz_localize(None)

    # Replace NaN values with None so SQL can handle them
    df = df.where(pd.notnull(df), None)

    # Append data to Bronze schema
    df.to_sql(
        name="ecommerce_behavior",   # Target table
        schema="bronze",             # Schema
        con=engine,                  # Database connection
        if_exists="append",          # Append instead of replace
        index=False                  # Do not write DataFrame index
    )

    print("✅ Appended 10,000 rows to Bronze layer")
    return "Bronze layer load complete ✅ (10,000 rows)"


# =================================================
# Prefect Flow: Orchestrate Bronze Layer Task
# =================================================
@flow(name="bronze-layer-flow")
def bronze_flow():
    """
    Prefect flow to orchestrate Bronze layer loading (test mode).
    Executes the 'load_csv_to_bronze' task and logs progress.
    """
    print("⚡ Running Bronze Layer (Test Mode)...")
    result = load_csv_to_bronze()
    print(result)
    print("🎉 Bronze Layer test load completed successfully!")


# =================================================
# Run Bronze layer
# =================================================
if __name__ == "__main__":
    bronze_flow()
