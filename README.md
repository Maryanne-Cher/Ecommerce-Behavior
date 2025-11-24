## Ecommerce_Behavior

This project uses the [Ecommerce Behavior Data from Multi-category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) dataset hosted on Kaggle.  
### How to Download the Full Dataset
⚠️ Note: The full dataset is too large to be stored directly in this repository.  
1. Go to the [dataset page on Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store).  
2. Sign in with your Kaggle account.  
3. Download the CSV file(s) to your local machine.
4. Create database and schemas on SQL Server (add link) 
5. Create bronze DDL script on SQL Server (add link)
6. Update the `csv_file` path in [`scripts/load_to_sql.py`](scripts/load_to_sql.py) with the location of your downloaded file.
7. 
### Example of a User's Journey:
1. The user checked out several iPhones
2. Purchased one iPhone in 1 click
3. Viewed 2 unknown products at brand Arena
4. Visited some Apple's headphones and purchased one
5. After that visited more expensive products but decided not to buy it

### User Session:
1. If user visits website for the first time, a new session is created and the user's events are marked with that session.
2. While user views more products or pages, we continue to mark events with the session ID.
3. But if user didn't generate new events for more than 2 hours, session code will expire and a new session ID is generated.
4. And the next events of the same user will be marked with this new session.

### Event Types:
1. View - a user viewed a product
2. Cart - a user added a product to shopping cart
3. Remove from cart - a user removed a product from shopping cart
4. Purchase - a user purchased a product


# E-commerce Analytics ETL Project

## Project Overview
This project implements an end-to-end ETL pipeline for e-commerce behavioral data, transforming raw CSV files into analytics-ready tables across **Bronze**, **Silver**, and **Gold** layers. Data quality (DQ) checks are performed at each layer, and **Prefect** flows orchestrate ETL and DQ tasks.

---

## Architecture

- **Bronze Layer**: Raw ingestion of CSV data with minimal transformation for testing.
- **Silver Layer**: Cleaned and standardized data for analytics, with transformations like splitting category codes and replacing missing values.
- **Gold Layer**: Analytics-ready fact and dimension tables for reporting, including deduplicated products and enriched events.

---

## Prefect Flows

| Layer  | Flow Name                        | Description                            |
|--------|---------------------------------|----------------------------------------|
| Bronze | `bronze_flow()`                  | Load CSV into Bronze layer.             |
| Bronze | `bronze_dq_flow()`               | Run data quality checks on Bronze.     |
| Silver | `silver_flow()`                  | Load Silver table from Bronze.         |
| Silver | `silver_dq_flow()`               | Run data quality checks on Silver.     |
| Gold   | `gold_dim_products_flow()`       | Load Gold dimension table.             |
| Gold   | `gold_fact_ecommerce_flow()`     | Load Gold fact table.                  |
| Gold   | `gold_dq_flow()`                 | Run data quality checks on Gold.       |

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







