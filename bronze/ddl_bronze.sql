/*
===============================================================================
Table: bronze.ecommerce_behavior
Purpose: Stores raw user behavior data for e-commerce events. 
         This is the first stage (bronze) of the ETL pipeline, capturing 
         all events as-is from source systems before transformations.
Columns:
    - event_time     : Timestamp of the event
    - event_type     : Type of event (e.g., click, purchase)
    - product_id     : Unique product identifier
    - category_id    : Unique category identifier
    - category_code  : Category name/code
    - brand          : Product brand
    - price          : Product price
    - user_id        : Unique user identifier
    - user_session   : Session identifier
    - loaded_at      : Timestamp when the record was loaded into this table
================================================================================
*/

-- ==============================================
-- Drop table if it exists
-- ==============================================
IF OBJECT_ID('bronze.ecommerce_behavior', 'U') IS NOT NULL
    DROP TABLE bronze.ecommerce_behavior;
GO

-- ==============================================
-- Create table
-- ==============================================
CREATE TABLE bronze.ecommerce_behavior (
    event_time      DATETIME      NOT NULL,
    event_type      VARCHAR(10)   NULL,
    product_id      BIGINT        NULL,
    category_id     BIGINT        NULL,
    category_code   VARCHAR(100)  NULL,
    brand           VARCHAR(50)   NULL,
    price           DECIMAL(10,2) NULL,
    user_id         BIGINT        NULL,
    user_session    VARCHAR(36)   NULL,
    loaded_at       DATETIME      NOT NULL DEFAULT GETDATE()
);
GO

-- ==============================================
-- Create Clustered Columnstore Index
-- ==============================================
CREATE CLUSTERED COLUMNSTORE INDEX CCI_ecommerce_behavior
ON bronze.ecommerce_behavior;
GO

