-- =========================================================
-- Create Table: silver.ecommerce_october_data
-- =========================================================

-- Drop table if it already exists
IF OBJECT_ID('silver.ecommerce_october_data', 'U') IS NOT NULL
    DROP TABLE silver.ecommerce_october_data;
GO

-- Create new table with partitioning by event_date
CREATE TABLE silver.ecommerce_october_data (
    event_type     VARCHAR(12)      NULL,
    product_id     BIGINT           NULL,
    category_id    BIGINT           NULL,
    category       NVARCHAR(100)    NULL,
    category_code  NVARCHAR(100)    NULL,
    brand          VARCHAR(60)      NULL,
    price          FLOAT            NULL,
    user_id        BIGINT           NULL,
    user_session   NVARCHAR(100)    NULL,
    event_date     DATE             NULL,
    event_time     TIME             NULL
)
ON [SchemePartitionByDay](event_date);
GO
