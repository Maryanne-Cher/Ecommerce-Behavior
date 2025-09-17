-- =========================================================
-- Stored Procedure: silver.load_silver
-- Purpose:
--   - Load transformed data from bronze.ecommerce_oct_partitioned 
--     into silver.ecommerce_october_data
--   - Handle NULLs and split category_code into category & subcategory
-- =========================================================

IF OBJECT_ID('silver.load_silver', 'P') IS NOT NULL
    DROP PROCEDURE silver.load_silver;
GO

CREATE PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO silver.ecommerce_october_data (
        event_type,
        product_id,
        category_id,
        category,
        category_code,   -- stores subcategory
        brand,
        price,
        user_id,
        user_session,
        event_date,
        event_time
    )
    SELECT
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
        END AS category_code,  -- subcategory goes here
        ISNULL(brand, 'UNKNOWN') AS brand,
        price,
        user_id,
        ISNULL(user_session, 'UNKNOWN') AS user_session,
        event_date,
        event_time_only AS event_time
    FROM bronze.ecommerce_oct_partitioned;
END;
GO
