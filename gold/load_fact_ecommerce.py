"""
================================================================================
File: gold_fact_ecommerce_load.py
Purpose: Loads data into the Gold-layer fact table 'fact_ecommerce' by executing 
         the stored procedure 'gold.LoadFactEcommerce' via Prefect tasks.
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
# Task: Load Gold Fact Table via Stored Procedure
# =================================================
@task
def load_gold_fact():
    """
    Executes the Gold-layer stored procedure to populate the fact_ecommerce table.
    """
    with engine.begin() as conn:
        conn.execute(text("EXEC gold.LoadFactEcommerce"))

    print("✅ Gold fact_ecommerce loaded via stored procedure.")
    return "Gold fact_ecommerce loaded ✅"


# =================================================
# Prefect Flow: Orchestrate Gold Fact Load
# =================================================
@flow(name="gold-fact-ecommerce-load")
def gold_fact_ecommerce_flow():
    print("⚡ Running Gold Fact Ecommerce Layer...")
    result = load_gold_fact()
    print(result)
    print("🎉 Gold fact_ecommerce load completed!")


# =================================================
# Main Execution
# =================================================
if __name__ == "__main__":
    gold_fact_ecommerce_flow()
