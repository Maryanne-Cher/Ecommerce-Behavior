/*
===================
Create Database and Schemas
=====================
*/

USE Master;
GO

--drop and recreate the 'ecommerce_behavior' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'ecommerce_behavior')
BEGIN
	ALTER DATABASE ecommerce_behavior SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE ecommerce_behavior
END;
GO

--create the 'ecommerce_behavior' database
CREATE DATABASE ecommerce_behavior
GO

USE ecommerce_behavior;
GO

--create schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
