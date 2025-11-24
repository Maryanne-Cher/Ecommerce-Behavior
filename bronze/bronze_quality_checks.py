"""
================================================================================
File: bronze_data_quality.py
Purpose: Performs data quality (DQ) checks on the Bronze layer of the e-commerce 
         ETL pipeline using Prefect tasks. Checks include:
           1. Invalid product_id or category_id
           2. Null counts and distinct values
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
# 1️⃣ Check for invalid product_id or category_id
# =================================================
@task
def dq_invalid_ids():
    query = """
    SELECT *
    FROM bronze.ecommerce_behavior
    WHERE product_id <= 0 OR category_id <= 0;
    """
    with engine.begin() as conn:
        rows = conn.execute(text(query)).fetchall()

    print("⚠️ Rows with invalid product_id or category_id:")
    if rows:
        for row in rows[:100]:  # print first 100 for preview
            print(row)
        print(f"❌ Found {len(rows)} invalid rows.")
    else:
        print("✅ No invalid product_id or category_id found.")


# =================================================
# 2️⃣ Check for nulls and distinct counts
# =================================================
@task
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

    print("ℹ️ Null counts and distinct values:")
    for col, val in stats.items():
        print(f"   {col}: {val}")


# =================================================
# 3️⃣ Check for duplicate product_id
# =================================================
@task
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

    print("⚠️ Total duplicate product_id rows:")
    print(f"   {result}")
    if result > 0:
        print("❌ Duplicates found in product_id column.")
    else:
        print("✅ No duplicate product_id values found.")


# =================================================
# Prefect Flow: Orchestrate Bronze DQ Tasks
# =================================================
@flow(name="bronze-dq-flow")
def bronze_dq_flow():
    dq_invalid_ids()
    dq_nulls_and_distincts()
    dq_duplicate_products()
    print("🏁 Bronze DQ checks completed.")


# =================================================
# Main Execution
# =================================================
if __name__ == "__main__":
    bronze_dq_flow()
