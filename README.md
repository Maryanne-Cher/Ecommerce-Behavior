## Ecommerce_Behavior

This project uses the [Ecommerce Behavior Data from Multi-category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) dataset hosted on Kaggle.  
### Example of a User's Journey:
1. The user checked out several iPhones
2. Purchased one iPhone in 1 click (Without cart event)
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

⚠️ Note: The full dataset is too large to be stored directly in this repository.  


### How to Download the Full Dataset
1. Go to the [dataset page on Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store).  
2. Sign in with your Kaggle account.  
3. Download the CSV file(s) to your local machine.
4. Create database and schemas on SQL Server (add link) 
5. Create bronze DDL script on SQL Server (add link)
6. Update the `csv_file` path in [`scripts/load_to_sql.py`](scripts/load_to_sql.py) with the location of your downloaded file.

## Load Data into SQL Server using Python

To export the dataset into SQL Server, use the provided script: [`scripts/load_to_sql.py`](scripts/load_to_sql.py).  

