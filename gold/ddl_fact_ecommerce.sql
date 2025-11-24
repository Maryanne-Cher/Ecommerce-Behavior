/*
================================================================================
Table: gold.fact_ecommerce
Purpose: Stores the fact table for e-commerce events in the Gold layer. 
         Designed for analytics and reporting with a clustered columnstore index.
Columns:
    - event_key       : Surrogate key (IDENTITY)
    - event_date      : Date of the event
    - event_time_only : Time of the event
    - event_type      : Type of event (click, purchase, etc.)
    - product_id      : Unique product identifier
    - category        : Product category
    - subcategory     : Product subcategory
    - price           : Product price
    - user_id         : Unique user identifier
    - user_session    : Session identifier
================================================================================
*/

-- ==============================================
-- Step 0: Drop table if it exists
-- ==============================================
IF OBJECT_ID('gold.fact_ecommerce', 'U') IS NOT NULL
    DROP TABLE gold.fact_ecommerce;
GO

-- ==============================================
-- Step 1: Create partitioned table with clustered columnstore
-- ==============================================
CREATE TABLE gold.fact_ecommerce (
    event_key       BIGINT IDENTITY(1,1) PRIMARY KEY,
    event_date      DATE        NOT NULL,
    event_time_only TIME(0)     NOT NULL,
    event_type      VARCHAR(10) NULL,
    product_id      BIGINT      NULL,
    category        VARCHAR(50) NULL,
    subcategory     VARCHAR(50) NULL,
    price           DECIMAL(10,2) NULL,
    user_id         BIGINT      NULL,
    user_session    VARCHAR(36) NULL
);
GO
