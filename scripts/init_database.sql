/* 
Create Database and Schemas
==================================================
Script Purpose: This script will create a new database named 'ecommerce_behavior_warehouse' after checking if it already exists.
If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas within the database:
'bronze', 'silver', and 'gold'. 
===================================================
*/

USE Master;
GO

--drop and recreate the 'ecommerce_behavior_warehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'ecommerce_behavior_warehouse')
BEGIN
	ALTER DATABASE ecommerce_behavior_warehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE ecommerce_behavior_warehouse
END;
GO

--create the 'ecommerce_behavior_warehouse' database
CREATE DATABASE ecommerce_behavior_warehouse
GO

USE ecommerce_behavior_warehouse;
GO

--create schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
