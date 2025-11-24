"""
================================================================================
File: silver_data_quality.py
Purpose: Performs data quality (DQ) checks on the Silver layer of the e-commerce 
         ETL pipeline using Prefect tasks. Checks include:
           1. Null counts and distinct values
           2. 'UNKNOWN' value counts
           3. Duplicate product_id values
================================================================================
"""

# =================================================
# Imports
# =================================================
from prefect import flow, task
from sqlalchemy import create_engine, text

# =================================================
# Database Engine (replace with your connection string)
# =================================================
engine = create_engine(
    "mssql+pyodbc://@ATX11492/ecommerce_behavior?"
    "driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)


# =================================================
# 1️⃣ Check for nulls and distinct counts
# =================================================
@task
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
    print("ℹ️ Null counts and distinct values in silver.ecommerce_behavior:")
    for col, val in stats.items():
        print(f"   {col}: {val}")


# =================================================
# 2️⃣ Check for UNKNOWN values
# =================================================
@task
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
    print("⚠️ Rows with UNKNOWN values in silver.ecommerce_behavior:")
    for col, val in stats.items():
        print(f"   {col}: {val}")


# =================================================
# 3️⃣ Check for duplicate product_id
# =================================================
@task
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
    print("⚠️ Total duplicate product_id rows in silver.ecommerce_behavior:")
    print(f"   {result}")
    if result > 0:
        print("❌ Duplicates found in product_id column.")
    else:
        print("✅ No duplicate product_id values found.")


# =================================================
# Prefect Flow: Orchestrate Silver DQ Tasks
# =================================================
@flow(name="silver-dq-flow")
def silver_dq_flow():
    dq_nulls_and_distincts_silver()
    dq_unknown_values_silver()
    dq_duplicate_products_silver()
    print("🏁 Silver DQ checks completed.")


# =================================================
# Main Execution
# =================================================
if __name__ == "__main__":
    silver_dq_flow()
