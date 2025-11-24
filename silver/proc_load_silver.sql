/*
================================================================================
Procedure: silver.LoadEcommerceBehavior
Purpose: Loads and transforms data from the Bronze layer into the Silver layer.
         - Truncates Silver table before loading
         - Transforms 'event_time' into separate date and time columns
         - Splits 'category_code' into 'category' and 'subcategory'
         - Handles NULLs for 'brand' and 'user_session'
================================================================================
*/

CREATE OR ALTER PROCEDURE silver.LoadEcommerceBehavior
AS
BEGIN
    SET NOCOUNT ON;

    -- ==============================================
    -- Step 0: Empty Silver table
    -- ==============================================
    TRUNCATE TABLE silver.ecommerce_behavior;

    -- ==============================================
    -- Step 1: Load and transform data from Bronze
    -- ==============================================
    INSERT INTO silver.ecommerce_behavior (
        event_date,
        event_time_only,
        event_type,
        product_id,
        category_id,
        category,
        subcategory,
        brand,
        price,
        user_id,
        user_session
    )
    SELECT
        CAST(event_time AS DATE) AS event_date,
        CAST(event_time AS TIME(0)) AS event_time_only,
        event_type,
        product_id,
        category_id,
        CASE 
            WHEN CHARINDEX('.', category_code) > 0 
                THEN LEFT(category_code, CHARINDEX('.', category_code) - 1)
            ELSE 'UNKNOWN'
        END AS category,
        CASE 
            WHEN CHARINDEX('.', category_code) > 0 
                THEN SUBSTRING(category_code, CHARINDEX('.', category_code) + 1, LEN(category_code))
            ELSE 'UNKNOWN'
        END AS subcategory,
        ISNULL(brand, 'UNKNOWN') AS brand,
        price,
        user_id,
        ISNULL(user_session, 'UNKNOWN') AS user_session
    FROM bronze.ecommerce_behavior;
END;
GO
