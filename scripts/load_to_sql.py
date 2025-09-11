import pandas as pd
from sqlalchemy import create_engine, text

# --- CONFIG ---
server = "<YOUR_SERVER>"
database = "<YOUR_DATABASE>"
table_name = "ecommerce_oct"
csv_file = "sample_data.csv"   # use included sample, or replace with full dataset
chunksize = 100000

# --- CONNECTION ---
connection_string = (
    f"mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)
engine = create_engine(connection_string)

# --- LOAD DATA ---
for i, chunk in enumerate(pd.read_csv(csv_file, chunksize=chunksize)):
    if "event_time" in chunk.columns:
        chunk["event_time"] = pd.to_datetime(
            chunk["event_time"].str.replace(" UTC", ""), errors="coerce"
        )
    chunk.to_sql(table_name, engine, if_exists="append", index=False)
    print(f"✅ Loaded chunk {i+1}")

# --- VERIFY ---
with engine.connect() as conn:
    result = conn.execute(text(f"SELECT COUNT(*) FROM dbo.{table_name}"))
    row_count = result.scalar()
    print(f"✅ Total rows in {table_name}: {row_count}")

