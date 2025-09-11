"""
GitHub-ready script to load large CSV files into SQL Server in chunks.
"""

import pandas as pd
from sqlalchemy import create_engine, text

# ---------------------------
# Configurations (Update as needed)
# ---------------------------
DATABASE_CONFIG = {
    "server": "<YOUR_SERVER_NAME>",       # e.g., "localhost\\SQLEXPRESS"
    "database": "<YOUR_DATABASE_NAME>",   # e.g., "ecommerce_behavior_warehouse"
    "driver": "ODBC Driver 17 for SQL Server"
}

CSV_FILE = r"<PATH_TO_YOUR_CSV_FILE>"    # e.g., "data/ecommerce_oct.csv"
CHUNK_SIZE = 100000                     # Number of rows per chunk
TABLE_NAME = "ecommerce_oct"             # Destination SQL table
# ---------------------------

# Create SQLAlchemy engine
connection_string = (
    f"mssql+pyodbc://@{DATABASE_CONFIG['server']}/"
    f"{DATABASE_CONFIG['database']}?driver={DATABASE_CONFIG['driver'].replace(' ', '+')}"
    "&trusted_connection=yes"
)
engine = create_engine(connection_string)

# Load CSV in chunks
for i, chunk in enumerate(pd.read_csv(CSV_FILE, chunksize=CHUNK_SIZE)):
    # Clean datetime column
    if 'event_time' in chunk.columns:
        chunk['event_time'] = pd.to_datetime(
            chunk['event_time'].str.replace(' UTC', ''), errors='coerce'
        )

    # Write chunk to SQL Server
    chunk.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
    print(f"✅ Loaded chunk {i+1}")

# Verify total rows
with engine.connect() as conn:
    result = conn.execute(text(f"SELECT COUNT(*) FROM dbo.{TABLE_NAME}"))
    row_count = result.scalar()
    print(f"✅ Total rows in {TABLE_NAME}: {row_count}")

