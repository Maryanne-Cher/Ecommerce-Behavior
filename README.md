## Ecommerce_Behavior

This project uses the [Ecommerce Behavior Data from Multi-category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) dataset hosted on Kaggle.  

⚠️ Note: The full dataset is too large to be stored directly in this repository.  

### How to Download the Full Dataset
1. Go to the [dataset page on Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store).  
2. Sign in with your Kaggle account.  
3. Download the CSV file(s) to your local machine.
4. Create database and schemas (add link)
5. Create bronze DDL script (add link)
6. Update the `csv_file` path in [`scripts/load_to_sql.py`](scripts/load_to_sql.py) with the location of your downloaded file.  

For example:  
```python
csv_file = r"C:\Users\<your-username>\Downloads\2019-Oct.csv"

## Load Data into SQL Server

To export the dataset into SQL Server, use the provided script: [`scripts/load_to_sql.py`](scripts/load_to_sql.py).  

