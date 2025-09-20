/*
========================================================================
Script:       vw_dim_products.sql
Purpose:      Create or alter the product dimension view 'gold.vw_dim_products'.
Description:  Aggregates product data from 'silver.ecommerce_october_data' 
              to create a dimension with unique products, their category, 
              and brand. Handles duplicates by taking the MAX value.
========================================================================
*/

CREATE OR ALTER VIEW gold.vw_dim_products AS
SELECT 
    product_id,
    MAX(category_id) AS category_id,  -- Pick one if duplicates exist
    MAX(brand) AS brand
FROM silver.ecommerce_october_data
GROUP BY product_id;
GO
