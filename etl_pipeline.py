# --------------------------------------------------------------------------------------------------
# Medallion ETL Pipeline Orchestration (Prefect Flow)
# --------------------------------------------------------------------------------------------------
#
# This script defines a complete Medallion Data Architecture (Bronze -> Silver -> Gold)
# pipeline using Prefect for orchestration and Pandas/SQLAlchemy for data processing.
# The pipeline is designed to load, clean, transform, and validate e-commerce behavior data.
#
# The master flow, `medallion_pipeline_flow`, executes the following stages sequentially:
# 1. Bronze Load: Reads the CSV, cleans 10,000 rows, and appends them to `bronze.ecommerce_behavior`.
# 2. Bronze DQ: Performs data quality checks (nulls, invalid IDs) on the raw data.
# 3. Silver Load: Executes a SQL Stored Procedure to transform Bronze data into the Silver layer.
# 4. Silver DQ: Checks for nulls, unknowns, and consistency in the Silver layer.
# 5. Gold Load: Executes Stored Procedures to build `gold.dim_products` and `gold.fact_ecommerce`.
# 6. Gold DQ: Performs integrity checks (referential integrity, key duplicates) on the Gold layer.
#
# Prerequisites:
# 1. Python environment with 'prefect', 'pandas', and 'SQLAlchemy' installed.
# 2. A running SQL Server instance accessible via ODBC Driver 17.
# 3. The specified database ('ecommerce_behavior') must exist, along with the 'bronze',
#    'silver', and 'gold' schemas, and required stored procedures (SPs) for Silver/Gold loading.
# 4. The CSV_FILE path must be valid on the execution machine.
#
# Execution:
# To run locally: python medallion_pipeline.py
# To deploy with Prefect: prefect deploy flow-name medallion_pipeline.py:medallion_pipeline_flow
#
# --------------------------------------------------------------------------------------------------

from prefect import task, flow
import pandas as pd
from sqlalchemy import create_engine, text

# =================================================
# 1. Configuration and Database Setup
# =================================================

# Database Configuration
DATABASE_CONFIG = {
    "server": "",
    "database": "ecommerce_behavior",
    "driver": "ODBC Driver 17 for SQL Server"
}

# Path to CSV for Bronze Layer loading
# NOTE: Update this path to a valid location on your machine.
CSV_FILE = r"C:\Users\mmbesu\Desktop\_SQL projects\ecommerce behavior data project\November data\2019-Oct.csv"

# Construct connection string and engine
connection_string = (
    f"mssql+pyodbc://@{DATABASE_CONFIG['server']}/"
    f"{DATABASE_CONFIG['database']}?driver={DATABASE_CONFIG['driver'].replace(' ', '+')}"
    "&trusted_connection=yes"
)
engine = create_engine(connection_string)

# =================================================
# 2. Bronze Layer Tasks (Load & DQ)
# =================================================

@task(name="Load CSV to Bronze (Test Mode)")
def load_csv_to_bronze():
    """
    Loads only the first 10,000 rows into the Bronze layer for testing.
    Cleans and transforms data:
      - event_time → datetime (remove timezone)
      - Replace NaN with None for SQL compatibility
    """
    print(f"Loading data from: {CSV_FILE}")
    
    # Read only first 10,000 rows
    try:
        df = pd.read_csv(CSV_FILE, nrows=10_000)
    except FileNotFoundError:
        print(f"❌ ERROR: CSV file not found at {CSV_FILE}. Cannot load Bronze data.")
        return False
        
    # Convert 'event_time' to datetime (UTC-aware), then remove timezone info
    df['event_time'] = pd.to_datetime(df['event_time'], utc=True).dt.tz_localize(None)

    # Replace NaN values with None so SQL can handle them
    df = df.where(pd.notnull(df), None)

    # Append data to Bronze schema
    df.to_sql(
        name="ecommerce_behavior",    # target table
        schema="bronze",             # schema
        con=engine,                  # database connection
        if_exists="append",          # append instead of replace
        index=False                  # do not write DataFrame index
    )

    print("✅ Appended 10,000 rows to Bronze layer")
    return True

# --- Bronze DQ Tasks ---
@task(name="DQ: Check Invalid IDs (Bronze)")
def dq_invalid_ids():
    query = """
    SELECT *
    FROM bronze.ecommerce_behavior
    WHERE product_id <= 0 OR category_id <= 0;
    """
    with engine.begin() as conn:
        rows = conn.execute(text(query)).fetchall()
    print("\n--- 1. Bronze DQ: Invalid Product/Category IDs ---")
    if rows:
        print(f"❌ Found {len(rows)} invalid rows. First 5 shown:")
        for row in rows[:5]:
            print(row)
    else:
        print("✅ No invalid product_id or category_id found.")

@task(name="DQ: Check Nulls and Distinct Counts (Bronze)")
def dq_nulls_and_distincts():
    query = """
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN event_time IS NULL THEN 1 ELSE 0 END) AS null_event_time,
        SUM(CASE WHEN event_type IS NULL THEN 1 ELSE 0 END) AS null_event_type,
        SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_id,
        SUM(CASE WHEN category_id IS NULL THEN 1 ELSE 0 END) AS null_category_id,
        SUM(CASE WHEN category_code IS NULL THEN 1 ELSE 0 END) AS null_category_code,
        SUM(CASE WHEN brand IS NULL THEN 1 ELSE 0 END) AS null_brand,
        SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) AS null_price,
        SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS null_user_id,
        SUM(CASE WHEN user_session IS NULL THEN 1 ELSE 0 END) AS null_user_session,
        COUNT(DISTINCT category_code) AS total_distinct_category_code,
        COUNT(DISTINCT brand) AS total_distinct_brand,
        COUNT(DISTINCT product_id) AS total_distinct_product_id
    FROM bronze.ecommerce_behavior;
    """
    with engine.begin() as conn:
        result = conn.execute(text(query)).fetchone()
    stats = dict(result._mapping)
    print("\n--- 2. Bronze DQ: Null Counts and Distincts ---")
    for col, val in stats.items():
        print(f"    {col}: {val}")

@task(name="DQ: Check Duplicate Product IDs (Bronze)")
def dq_duplicate_products():
    query = """
    SELECT COUNT(*) AS total_duplicates
    FROM (
        SELECT product_id, COUNT(*) AS product_count
        FROM bronze.ecommerce_behavior
        GROUP BY product_id
        HAVING COUNT(*) > 1
    ) t;
    """
    with engine.begin() as conn:
        result = conn.execute(text(query)).scalar()
    print("\n--- 3. Bronze DQ: Duplicate Product IDs ---")
    print(f"    Total product_id duplicates (events): {result}")
    if result > 0:
        print("❌ Duplicates found (expected if a product has multiple events).")
    else:
        print("✅ No duplicate product_id values found.") # This check should be for duplicate events, not product IDs

# =================================================
# 3. Silver Layer Tasks (Load & DQ)
# =================================================

@task(name="Load Silver Layer via SP")
def load_silver():
    with engine.begin() as conn:
        conn.execute(text("EXEC silver.LoadEcommerceBehavior"))
    print("\n✅ Silver layer loaded via stored procedure.")
    return True

# --- Silver DQ Tasks ---
@task(name="DQ: Check Nulls and Distinct Counts (Silver)")
def dq_nulls_and_distincts_silver():
    query = """
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN event_time_only IS NULL THEN 1 ELSE 0 END) AS null_event_time,
        SUM(CASE WHEN event_date IS NULL THEN 1 ELSE 0 END) AS null_event_date,
        SUM(CASE WHEN event_type IS NULL THEN 1 ELSE 0 END) AS null_event_type,
        SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_id,
        SUM(CASE WHEN category_id IS NULL THEN 1 ELSE 0 END) AS null_category_id,
        SUM(CASE WHEN brand IS NULL THEN 1 ELSE 0 END) AS null_brand,
        SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) AS null_price,
        SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS null_user_id,
        SUM(CASE WHEN user_session IS NULL THEN 1 ELSE 0 END) AS null_user_session,
        COUNT(DISTINCT category) AS total_distinct_category,
        COUNT(DISTINCT subcategory) AS total_distinct_subcategory,
        COUNT(DISTINCT brand) AS total_distinct_brand,
        COUNT(DISTINCT product_id) AS total_distinct_product_id
    FROM silver.ecommerce_behavior;
    """
    with engine.begin() as conn:
        result = conn.execute(text(query)).fetchone()
    stats = dict(result._mapping)
    print("\n--- 1. Silver DQ: Null Counts and Distincts ---")
    for col, val in stats.items():
        print(f"    {col}: {val}")

@task(name="DQ: Check UNKNOWN Values (Silver)")
def dq_unknown_values_silver():
    query = """
    SELECT
        SUM(CASE WHEN category = 'UNKNOWN' THEN 1 ELSE 0 END) AS category_unknowns,
        SUM(CASE WHEN subcategory = 'UNKNOWN' THEN 1 ELSE 0 END) AS subcategory_unknowns,
        SUM(CASE WHEN brand = 'UNKNOWN' THEN 1 ELSE 0 END) AS brand_unknowns,
        SUM(CASE WHEN user_session = 'UNKNOWN' THEN 1 ELSE 0 END) AS user_session_unknowns
    FROM silver.ecommerce_behavior;
    """
    with engine.begin() as conn:
        result = conn.execute(text(query)).fetchone()
    stats = dict(result._mapping)
    print("\n--- 2. Silver DQ: UNKNOWN Value Counts ---")
    for col, val in stats.items():
        print(f"    {col}: {val}")

@task(name="DQ: Check Duplicate Product IDs (Silver)")
def dq_duplicate_products_silver():
    query = """
    SELECT COUNT(*) AS total_duplicates
    FROM (
        SELECT product_id, COUNT(*) AS product_count
        FROM silver.ecommerce_behavior
        GROUP BY product_id
        HAVING COUNT(*) > 1
    ) t;
    """
    with engine.begin() as conn:
        result = conn.execute(text(query)).scalar()
    print("\n--- 3. Silver DQ: Duplicate Product IDs ---")
    print(f"    Total product_id duplicates (events): {result}")
    if result > 0:
        print("❌ Duplicates found (expected if a product has multiple events).")
    else:
        print("✅ No duplicate product_id values found.")

# =================================================
# 4. Gold Layer Tasks (Load & DQ)
# =================================================

@task(name="Load Gold Fact Table via SP")
def load_gold_fact():
    with engine.begin() as conn:
        conn.execute(text("EXEC gold.LoadFactEcommerce"))
    print("\n✅ Gold Fact Ecommerce loaded via stored procedure.")
    return True

@task(name="Load Gold Dim Products Table via SP")
def load_gold_dim_products():
    with engine.begin() as conn:
        conn.execute(text("EXEC gold.LoadDimProducts"))
    print("✅ Gold Dim Products table loaded via stored procedure.")
    return True

# --- Gold DQ Tasks ---
@task(name="DQ: Check Duplicate Event Keys (Gold Fact)")
def check_event_key_duplicates():
    query = """
    SELECT event_key, COUNT(*) AS duplicate_count
    FROM gold.fact_ecommerce
    GROUP BY event_key
    HAVING COUNT(*) > 1;
    """
    with engine.begin() as conn:
        results = conn.execute(text(query)).fetchall()
    
    print("\n--- 1. Gold DQ: Duplicate Event Keys in Fact Table ---")
    if results:
        print(f"⚠️ Duplicate event_keys found: {len(results)} distinct duplicates")
        for row in results[:5]: # show only first 5
            print(f"     event_key: {row.event_key}, count: {row.duplicate_count}")
    else:
        print("✅ No duplicate event_keys found.")

@task(name="DQ: Check Nulls/Unknowns/Distincts (Gold Fact)")
def check_fact_nulls_unknowns():
    query = """
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN event_key IS NULL THEN 1 ELSE 0 END)        AS event_key_null_count,
        SUM(CASE WHEN event_date IS NULL THEN 1 ELSE 0 END)       AS event_date_null_count,
        SUM(CASE WHEN event_time_only IS NULL THEN 1 ELSE 0 END)  AS event_time_only_null_count,
        SUM(CASE WHEN event_type IS NULL THEN 1 ELSE 0 END)       AS event_type_null_count,
        SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END)       AS product_id_null_count,
        SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END)         AS category_null_count,
        SUM(CASE WHEN subcategory IS NULL THEN 1 ELSE 0 END)      AS subcategory_null_count,
        SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END)            AS price_null_count,
        SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END)          AS user_id_null_count,
        SUM(CASE WHEN user_session IS NULL THEN 1 ELSE 0 END)     AS user_session_null_count,
        SUM(CASE WHEN category = 'UNKNOWN' THEN 1 ELSE 0 END)     AS category_unknown_count,
        SUM(CASE WHEN subcategory = 'UNKNOWN' THEN 1 ELSE 0 END)  AS subcategory_unknown_count,
        COUNT(DISTINCT category)        AS total_distinct_category,
        COUNT(DISTINCT subcategory)     AS total_distinct_subcategory,
        COUNT(DISTINCT product_id)      AS total_distinct_product_id
    FROM gold.fact_ecommerce;
    """
    with engine.begin() as conn:
        result = conn.execute(text(query)).fetchone()
    
    print("\n--- 2. Gold DQ: Fact Ecommerce Summary ---")
    for key, value in result._mapping.items():
        if value > 0 and ('null' in key.lower() or 'unknown' in key.lower()):
            print(f"⚠️ {key}: {value}")
        else:
            print(f"    {key}: {value}")

@task(name="DQ: Check Fact to Dim Referential Integrity (Gold)")
def check_referential_integrity():
    query = """
    SELECT f.product_id
    FROM gold.fact_ecommerce f
    LEFT JOIN gold.dim_products p
        ON f.product_id = p.product_id
    WHERE p.product_id IS NULL;
    """
    with engine.begin() as conn:
        results = conn.execute(text(query)).fetchall()
    
    print("\n--- 3. Gold DQ: Referential Integrity (Fact -> Dim) ---")
    if results:
        print(f"⚠️ Found {len(results)} fact records without a matching product in dim_products.")
        for row in results[:5]:
            print(f"    Product ID missing in Dim: {row.product_id}")
    else:
        print("✅ All fact products exist in dim_products.")

@task(name="DQ: Check Brand/Category Consistency (Silver vs. Gold Dim)")
def check_brand_category_consistency():
    brand_query = """
    SELECT COUNT(DISTINCT f.brand) AS mismatched_brands
    FROM silver.ecommerce_behavior f
    JOIN gold.dim_products p
        ON f.product_id = p.product_id
    WHERE f.brand <> p.brand;
    """
    category_query = """
    SELECT COUNT(DISTINCT f.category_id) AS mismatched_categories
    FROM silver.ecommerce_behavior f
    JOIN gold.dim_products p
        ON f.product_id = p.product_id
    WHERE f.category_id <> p.category_id;
    """
    with engine.begin() as conn:
        brand_result = conn.execute(text(brand_query)).scalar()
        category_result = conn.execute(text(category_query)).scalar()
    
    print("\n--- 4. Gold DQ: Consistency Check (Silver Source vs. Gold Dim) ---")
    if brand_result > 0:
        print(f"⚠️ Brand mismatches found: {brand_result} distinct brands")
    else:
        print("✅ No brand mismatches found.")
    
    if category_result > 0:
        print(f"⚠️ Category mismatches found: {category_result} distinct categories")
    else:
        print("✅ No category mismatches found.")

# =================================================
# 5. Prefect Flow Definitions
# =================================================

@flow(name="Bronze Layer Load Flow")
def bronze_load_flow():
    """Orchestrates loading data into the Bronze layer."""
    print("\n===============================")
    print("⚡ Starting Bronze Layer Load...")
    print("===============================")
    load_csv_to_bronze()

@flow(name="Bronze Layer DQ Flow")
def bronze_dq_flow():
    """Orchestrates data quality checks on the Bronze layer."""
    print("\n===============================")
    print("⚡ Starting Bronze Layer DQ Checks...")
    print("===============================")
    dq_invalid_ids()
    dq_nulls_and_distincts()
    dq_duplicate_products()
    print("🏁 Bronze DQ checks completed.")

@flow(name="Silver Layer Load Flow")
def silver_load_flow():
    """Orchestrates loading data into the Silver layer via SP."""
    print("\n===============================")
    print("⚡ Starting Silver Layer Load...")
    print("===============================")
    load_silver()

@flow(name="Silver Layer DQ Flow")
def silver_dq_flow():
    """Orchestrates data quality checks on the Silver layer."""
    print("\n===============================")
    print("⚡ Starting Silver Layer DQ Checks...")
    print("===============================")
    dq_nulls_and_distincts_silver()
    dq_unknown_values_silver()
    dq_duplicate_products_silver()
    print("🏁 Silver DQ checks completed.")

@flow(name="Gold Layer Fact Load Flow")
def gold_fact_load_flow():
    """Orchestrates loading the Gold Fact table."""
    print("\n===============================")
    print("⚡ Starting Gold Fact Table Load...")
    print("===============================")
    load_gold_fact()

@flow(name="Gold Layer Dim Products Load Flow")
def gold_dim_products_load_flow():
    """Orchestrates loading the Gold Dim Products table."""
    print("\n===============================")
    print("⚡ Starting Gold Dim Products Table Load...")
    print("===============================")
    load_gold_dim_products()

@flow(name="Gold Layer DQ Flow")
def gold_dq_flow():
    """Orchestrates data quality and integrity checks on the Gold layer."""
    print("\n===============================")
    print("⚡ Starting Gold Layer DQ Checks...")
    print("===============================")
    check_event_key_duplicates()
    check_fact_nulls_unknowns()
    check_referential_integrity()
    check_brand_category_consistency()
    print("🏁 Gold DQ checks completed.")

# =================================================
# 6. Master Orchestration Flow
# =================================================

@flow(name="Medallion ETL Pipeline Master Flow")
def medallion_pipeline_flow():
    """
    The master flow that orchestrates the entire Bronze -> Silver -> Gold 
    pipeline with all embedded Data Quality checks.
    """
    print("\n========================================================")
    print("🚀 Starting Medallion ETL Pipeline: Bronze -> Silver -> Gold")
    print("========================================================")
    
    # 1. Bronze Load & DQ
    bronze_load_flow()
    bronze_dq_flow()
    
    # 2. Silver Load & DQ
    silver_load_flow()
    silver_dq_flow()
    
    # 3. Gold Load (Fact depends on Dim, but here we run them sequentially)
    gold_dim_products_load_flow()
    gold_fact_load_flow()
    
    # 4. Gold DQ
    gold_dq_flow()
    
    print("\n========================================================")
    print("🎉 Pipeline Execution Complete!")
    print("========================================================")

# =================================================
# 7. Main Execution
# =================================================

if __name__ == "__main__":
    # Execute the single master flow
    medallion_pipeline_flow()
