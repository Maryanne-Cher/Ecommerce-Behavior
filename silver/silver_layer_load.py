"""
================================================================================
File: silver_layer_load.py
Purpose: Loads data into the Silver layer by executing the stored procedure 
         'silver.LoadEcommerceBehavior' via Prefect tasks. This layer contains 
         cleaned and transformed data from the Bronze layer.
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
# Task: Load Silver Layer via Stored Procedure
# =================================================
@task
def load_silver():
    """
    Executes the Silver layer stored procedure to load and transform data 
    from Bronze to Silver.
    """
    with engine.begin() as conn:
        conn.execute(text("EXEC silver.LoadEcommerceBehavior"))

    print("✅ Silver layer loaded via stored procedure.")
    return "Silver layer loaded ✅"


# =================================================
# Prefect Flow: Orchestrate Silver Layer Load
# =================================================
@flow(name="silver-layer-flow")
def silver_flow():
    print("⚡ Running Silver Layer...")
    result = load_silver()
    print(result)
    print("🎉 Silver Layer completed!")


# =================================================
# Main Execution
# =================================================
if __name__ == "__main__":
    silver_flow()
