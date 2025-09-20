/*
========================================================================
Script:       fact_october_sessions.sql
Purpose:      Create the fact table 'gold.fact_october_sessions' for 
              tracking ecommerce session events for October.
Description:  This table stores product and user session data, including
              category, subcategory, price, event date, and event time.
              The table is partitioned by event_date and uses a 
              clustered columnstore index for performance.
========================================================================
*/

-- Drop table if it exists
DROP TABLE IF EXISTS gold.fact_october_sessions;
GO

-- Create fact table
CREATE TABLE gold.fact_october_sessions
(
    product_key    BIGINT IDENTITY(1,1) NOT NULL, -- Surrogate key
    product_id     BIGINT NOT NULL,
    category       NVARCHAR(100) NULL,
    subcategory    NVARCHAR(100) NULL,           -- Formerly category_code
    price          DECIMAL(10,2) NULL,
    user_id        BIGINT NULL,
    user_session   NVARCHAR(100) NULL,
    event_date     DATE NOT NULL,
    event_time     TIME NULL
)
ON SchemePartitionByDay(event_date); -- Partitioned by date
GO

-- Add clustered columnstore index for performance
CREATE CLUSTERED COLUMNSTORE INDEX idx_fact_october_sessions
ON gold.fact_october_sessions
WITH (DROP_EXISTING = OFF);
GO
