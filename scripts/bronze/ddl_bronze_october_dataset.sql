/* 
DDL SCRIPT: CREATE BRONZE TABLES
Purpose: This script creates tables in the 'bronze' schema, dropping existing tables 
if they already exist.
=====================================================
*/

-- ======================================
-- DDL for bronze.ecommerce_oct
-- ====================================== 

-- Drop the table if it already exists
IF OBJECT_ID('bronze.ecommerce_oct', 'U') IS NOT NULL
    DROP TABLE bronze.ecommerce_oct;
GO

-- Create the table
CREATE TABLE bronze.ecommerce_oct (
    event_time      DATETIME2(7) NULL,
    event_type      VARCHAR(20) NULL,
    product_id      BIGINT NULL,
    category_id     BIGINT NULL,
    category_code   NVARCHAR(255) NULL,
    brand           NVARCHAR(255) NULL,
    price           FLOAT NULL,
    user_id         BIGINT NULL,
    user_session    NVARCHAR(150) NULL
) ON [PRIMARY];
GO
