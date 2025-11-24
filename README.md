# Ecommerce Behavior ETL Project

## Project Overview
This project implements an **end-to-end ETL pipeline** for e-commerce behavioral data, transforming raw CSV files into analytics-ready tables across **Bronze**, **Silver**, and **Gold** layers.  

Data quality (DQ) checks are performed at each layer, and **Prefect** flows orchestrate ETL tasks. The goal is to provide clean, deduplicated, and enriched data for analytics and reporting.

---

## Dataset

This project uses the [Ecommerce Behavior Data from Multi-category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) dataset hosted on Kaggle.  

⚠️ Note: The dataset is too large to store directly in this repository.

**How to download and prepare:**

1. Go to the [dataset page on Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
2. Sign in with your Kaggle account
3. Download the CSV file(s)
4. Create the database and schemas on SQL Server
5. Run the Bronze DDL script
6. Update the `csv_file` path in `scripts/load_to_sql.py` with the location of your downloaded CSV

---

## Example User Journey

1. User viewed several iPhones
2. Purchased one iPhone with 1 click
3. Viewed 2 unknown products from brand Arena
4. Browsed Apple headphones and purchased one
5. Explored expensive products but did not purchase

---

## User Sessions

- A new session is created when a user visits the website for the first time
- Events are marked with the session ID during activity
- If inactivity exceeds 2 hours, a new session ID is generated for subsequent events

---

## Event Types

| Event Type      | Description                         |
|-----------------|-------------------------------------|
| View            | User viewed a product                |
| Cart            | User added a product to cart         |
| Remove from Cart| User removed a product from cart     |
| Purchase        | User purchased a product             |

---

## Architecture

### Layers

- **Bronze Layer**  
  Raw ingestion of CSV data with minimal transformation.  
  **Table:** `bronze.ecommerce_behavior`  

- **Silver Layer**  
  Cleaned and standardized intermediate layer.  
  **Table:** `silver.ecommerce_behavior`  

- **Gold Layer**  
  Analytics-ready fact and dimension tables for reporting.  
  **Tables:** `gold.dim_products`, `gold.fact_ecommerce`  

### Tools Used

| Tool         | Purpose                                    |
|--------------|--------------------------------------------|
| SQL Server   | Data storage and ETL with stored procedures |
| Python       | ETL scripting, data cleaning, and transformations |
| Prefect      | ETL orchestration and workflow management  |
| Pandas       | Data processing and CSV handling           |
| SQLAlchemy   | Database connectivity                      |
| pyodbc       | SQL Server ODBC connection                 |
| Kaggle       | Source of raw dataset                       |

![SQL Server Logo](https://img.icons8.com/color/48/000000/sql-server.png)  
![Python Logo](https://img.icons8.com/color/48/000000/python.png)  
![Prefect Logo](https://images.ctfassets.net/0nm5vlv2ad7a/3trZQOSXl8NqgGCu4s2XrT/2a0b83f4a056da688d25bb89df1b8a78/prefect-logo.png)  
![Pandas Logo](https://pandas.pydata.org/static/img/pandas_mark.svg)

---

## Prefect Flows

| Layer  | Flow Name                        | Description                            |
|--------|---------------------------------|----------------------------------------|
| Bronze | `bronze_flow()`                  | Load CSV into Bronze layer             |
| Bronze | `bronze_dq_flow()`               | Run data quality checks on Bronze     |
| Silver | `silver_flow()`                  | Load Silver table from Bronze         |
| Silver | `silver_dq_flow()`               | Run data quality checks on Silver     |
| Gold   | `gold_dim_products_flow()`       | Load Gold dimension table             |
| Gold   | `gold_fact_ecommerce_flow()`     | Load Gold fact table                  |
| Gold   | `gold_dq_flow()`                 | Run data quality checks on Gold       |

---

## Key Features

- **Data Quality Checks**:
  - Null and distinct counts
  - Duplicate detection
  - UNKNOWN value detection
  - Referential integrity and consistency checks
- **Data Transformations**:
  - Bronze: minimal cleaning and type conversions
  - Silver: split category codes, fill missing values
  - Gold: deduplicate products and enrich fact events

---

## Repository Structure
ecommerce_etl/
│
├── bronze/
│ ├── bronze_layer_load.py
│ ├── bronze_dq.py
│
├── silver/
│ ├── silver_layer_load.py
│ ├── silver_dq.py
│ └── stored_procedures/
│ └── LoadEcommerceBehavior.sql
│
├── gold/
│ ├── gold_dim_products_load.py
│ ├── gold_fact_ecommerce_load.py
│ ├── gold_data_quality.py
│ └── stored_procedures/
│ ├── LoadDimProducts.sql
│ └── LoadFactEcommerce.sql
│
├── README.md
└── requirements.txt







