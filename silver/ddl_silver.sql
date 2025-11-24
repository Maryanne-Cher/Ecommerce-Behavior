/*
================================================================================
Table: silver.ecommerce_behavior
Purpose: Stores cleaned and transformed e-commerce behavior data in the Silver 
         layer. This table is designed for analytics and reporting, using a 
         partitioned and clustered columnstore index for performance.
Columns:
    - event_date       : Date of the event
    - event_time_only  : Time of the event
    - event_type       : Type of event (click, purchase, etc.)
    - product_id       : Unique product identifier
    - category_id      : Unique category identifier
    - category         : Category name
    - subcategory      : Subcategory name
    - brand            : Product brand
    - price            : Product price
    - user_id          : Unique user identifier
    - user_session     : Session identifier
================================================================================
*/

-- ==============================================
-- Step 0: Drop table if it exists
-- ==============================================
IF OBJECT_ID('silver.ecommerce_behavior', 'U') IS NOT NULL
    DROP TABLE silver.ecommerce_behavior;
GO

-- ==============================================
-- Step 1: Create partitioned table with clustered columnstore
-- ==============================================
CREATE TABLE silver.ecommerce_behavior (
    event_date       DATE        NOT NULL,
    event_time_only  TIME(0)     NOT NULL,
    event_type       VARCHAR(50) NULL,
    product_id       BIGINT      NULL,
    category_id      BIGINT      NULL,
    category         VARCHAR(50) NULL,
    subcategory      VARCHAR(50) NULL,
    brand            VARCHAR(50) NULL,
    price            DECIMAL(10,2) NULL,
    user_id          BIGINT      NULL,
    user_session     VARCHAR(36) NULL
);
GO

-- ==============================================
-- Step 2: Add Clustered Columnstore Index
-- ==============================================
CREATE CLUSTERED COLUMNSTORE INDEX CCI_ecommerce_behavior
ON silver.ecommerce_behavior;
GO

