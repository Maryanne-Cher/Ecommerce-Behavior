"""
================================================================================
File: gold_data_quality.py
Purpose: Performs data quality (DQ) checks on the Gold layer including:
    1. Duplicate event_key detection
    2. Nulls, UNKNOWNs, and distinct counts in fact_ecommerce
    3. Referential integrity between fact_ecommerce and dim_products
    4. Consistency of brand and category values between Silver and Gold layers
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
# Task: Check for duplicate event_key in fact_ecommerce
# =================================================
@task
def check_event_key_duplicates():
    """
    Identifies duplicate event_key values in gold.fact_ecommerce.
    """
    query = """
    SELECT event_key, COUNT(*) AS duplicate_count
    FROM gold.fact_ecommerce
    GROUP BY event_key
    HAVING COUNT(*) > 1;
    """
    with engine.begin() as conn:
        results = conn.execute(text(query)).fetchall()

    if results:
        print("⚠️ Duplicate event_keys found:")
        for row in results[:20]:  # show only first 20
            print(f"    event_key: {row.event_key}, count: {row.duplicate_count}")
        if len(results) > 20:
            print(f"    ...and {len(results)-20} more")
    else:
        print("✅ No duplicate event_keys found.")


# =================================================
# Task: Check for nulls, UNKNOWNs, and distinct counts
# =================================================
@task
def check_fact_nulls_unknowns():
    """
    Summarizes nulls, UNKNOWNs, and distinct counts in gold.fact_ecommerce.
    """
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
        COUNT(DISTINCT category)      AS total_distinct_category,
        COUNT(DISTINCT subcategory)   AS total_distinct_subcategory,
        COUNT(DISTINCT product_id)    AS total_distinct_product_id
    FROM gold.fact_ecommerce;
    """
    with engine.begin() as conn:
        result = conn.execute(text(query)).fetchone()

    print("🟡 Fact Ecommerce DQ Summary:")
    for key, value in result._mapping.items():
        if value > 0 and ('null' in key.lower() or 'unknown' in key.lower()):
            print(f"⚠️ {key}: {value}")
        else:
            print(f"    {key}: {value}")


# =================================================
# Task: Referential integrity: fact_ecommerce -> dim_products
# =================================================
@task
def check_referential_integrity():
    """
    Checks that all product_ids in fact_ecommerce exist in dim_products.
    """
    query = """
    SELECT f.product_id
    FROM gold.fact_ecommerce f
    LEFT JOIN gold.dim_products p
        ON f.product_id = p.product_id
    WHERE p.product_id IS NULL;
    """
    with engine.begin() as conn:
        results = conn.execute(text(query)).fetchall()

    if results:
        print("⚠️ Products in fact_ecommerce missing in dim_products:")
        for row in results[:20]:
            print(f"    product_id: {row.product_id}")
        if len(results) > 20:
            print(f"    ...and {len(results)-20} more")
    else:
        print("✅ All fact products exist in dim_products.")


# =================================================
# Task: Consistency check between Silver and Gold
# =================================================
@task
def check_brand_category_consistency():
    """
    Checks that brand and category values are consistent between Silver and Gold.
    """
    brand_query = """
    SELECT COUNT(DISTINCT p.brand) AS mismatched_brands
    FROM silver.ecommerce_behavior f
    JOIN gold.dim_products p
        ON f.product_id = p.product_id
    WHERE f.brand <> p.brand;
    """
    category_query = """
    SELECT COUNT(DISTINCT p.category_id) AS mismatched_categories
    FROM silver.ecommerce_behavior f
    JOIN gold.dim_products p
        ON f.product_id = p.product_id
    WHERE f.category_id <> p.category_id;
    """
    with engine.begin() as conn:
        brand_result = conn.execute(text(brand_query)).scalar()
        category_result = conn.execute(text(category_query)).scalar()

    if brand_result > 0:
        print(f"⚠️ Brand mismatches found: {brand_result} distinct brands")
    else:
        print("✅ No brand mismatches found.")

    if category_result > 0:
        print(f"⚠️ Category mismatches found: {category_result} distinct categories")
    else:
        print("✅ No category mismatches found.")


# =================================================
# Prefect Flow: Orchestrate Gold DQ Tasks
# =================================================
@flow(name="gold-dq-flow")
def gold_dq_flow():
    check_event_key_duplicates()
    check_fact_nulls_unknowns()
    check_referential_integrity()
    check_brand_category_consistency()
    print("🏁 Gold DQ checks completed.")


# =================================================
# Main Execution
# =================================================
if __name__ == "__main__":
    gold_dq_flow()
