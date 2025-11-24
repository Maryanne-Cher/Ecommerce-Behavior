"""
================================================================================
File: db_connection.py
Purpose: Configures and creates a SQL Server database connection using SQLAlchemy 
         and ODBC for the e-commerce behavior project. This connection can be 
         reused in ETL tasks and Prefect flows.
================================================================================
"""

# =================================================
# Imports
# =================================================
from prefect import task, flow
import pandas as pd
from sqlalchemy import create_engine, text

# =================================================
# Database Configuration
# =================================================
DATABASE_CONFIG = {
    "server": "ATX11492",
    "database": "ecommerce_behavior",
    "driver": "ODBC Driver 17 for SQL Server",
}

# =================================================
# Create SQLAlchemy Engine
# =================================================
connection_string = (
    f"mssql+pyodbc://@{DATABASE_CONFIG['server']}/"
    f"{DATABASE_CONFIG['database']}?"
    f"driver={DATABASE_CONFIG['driver'].replace(' ', '+')}&trusted_connection=yes"
)

engine = create_engine(connection_string)

# =================================================
# Optional: Test Connection (Uncomment if needed)
# =================================================
# with engine.connect() as conn:
#     result = conn.execute(text("SELECT 1"))
#     print("✅ Database connection successful:", result.fetchone()[0])
