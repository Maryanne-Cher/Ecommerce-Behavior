/********************************************************************
-- Stored Procedure: silver.load_silver
-- Description: Loads data from bronze.ecommerce_oct_partitioned into
--              silver.ecommerce_october_data, applies necessary 
--              transformations, and creates a clustered columnstore 
--              index to enable partitioning and improve query performance.
-- Steps:
--   1. Insert data from bronze to silver with category/subcategory handling.
--   2. Create clustered columnstore index if it doesn't exist.
-- Note: The silver table must already be created on the 
--       partition scheme [SchemePartitionByDay] using event_date.
********************************************************************/

CREATE PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    ------------------------------------------------------------------
    -- 1. Insert data from bronze to silver
    ------------------------------------------------------------------
    INSERT INTO silver.ecommerce_october_data (
        event_type,
        product_id,
        category_id,
        category,
        category_code,
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
        END AS subcategory,
        ISNULL(brand, 'UNKNOWN') AS brand,
        price,
        user_id,
        ISNULL(user_session, 'UNKNOWN') AS user_session,
        event_date,
        event_time_only AS event_time
    FROM bronze.ecommerce_oct_partitioned;

    ------------------------------------------------------------------
    -- 2. Create clustered columnstore index if it doesn't exist
    ------------------------------------------------------------------
    IF NOT EXISTS (
        SELECT 1 
        FROM sys.indexes 
        WHERE object_id = OBJECT_ID('silver.ecommerce_october_data') 
          AND name = 'Idx_ecommerce_silver'
    )
    BEGIN
        CREATE CLUSTERED COLUMNSTORE INDEX Idx_ecommerce_silver
        ON silver.ecommerce_october_data;
    END

    PRINT '✅ Data loaded into silver.ecommerce_october_data and index created.';
END;
GO
