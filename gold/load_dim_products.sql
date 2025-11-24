/*
================================================================================
Procedure: gold.LoadDimProducts
Purpose: Loads the Gold-layer product dimension table by deduplicating data 
         from the Silver layer. Ensures each product_id appears only once with 
         the most relevant category_id and brand.
================================================================================
*/

CREATE OR ALTER PROCEDURE gold.LoadDimProducts
AS
BEGIN
    SET NOCOUNT ON;

    -- ==============================================
    -- Step 0: Clear Gold table
    -- ==============================================
    TRUNCATE TABLE gold.dim_products;

    -- ==============================================
    -- Step 1: Insert deduplicated data from Silver
    -- ==============================================
    INSERT INTO gold.dim_products (product_id, category_id, brand)
    SELECT
        product_id,
        MAX(category_id) AS category_id,
        MAX(brand) AS brand
    FROM silver.ecommerce_behavior
    GROUP BY product_id;
END;
GO
