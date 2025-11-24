/*
================================================================================
Table: gold.dim_products
Purpose: Stores the product dimension for the Gold layer, containing unique 
         product identifiers, associated category IDs, and brand information.
================================================================================
*/

-- ==============================================
-- Step 0: Drop table if it exists
-- ==============================================
IF OBJECT_ID('gold.dim_products', 'U') IS NOT NULL
    DROP TABLE gold.dim_products;
GO

-- ==============================================
-- Step 1: Create table
-- ==============================================
CREATE TABLE gold.dim_products (
    product_id   BIGINT      NOT NULL,
    category_id  BIGINT      NULL,
    brand        VARCHAR(50) NULL
);
GO

