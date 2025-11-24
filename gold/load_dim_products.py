"""
================================================================================
File: gold_dim_products_load.py
Purpose: Loads data into the Gold-layer dimension table 'dim_products' by 
         executing the stored procedure 'gold.LoadDimProducts' via Prefect tasks.
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
# Task: Load Gold Dim Products via Stored Procedure
# =================================================
@task
def load_gold_dim_products():
    """
    Executes the Gold-layer stored procedure to populate the dim_products table.
    """
    with engine.begin() as conn:
        conn.execute(text("EXEC gold.LoadDimProducts"))

    print("✅ Gold dim_products table loaded via stored procedure.")
    return "Gold dim_products loaded ✅"


# =================================================
# Prefect Flow: Orchestrate Gold Dim Products Load
# =================================================
@flow(name="gold-dim-products-load")
def gold_dim_products_flow():
    print("⚡ Running Gold Dim Products Layer...")
    result = load_gold_dim_products()
    print(result)
    print("🎉 Gold dim_products load completed!")


# =================================================
# Main Execution
# =================================================
if __name__ == "__main__":
    gold_dim_products_flow()
