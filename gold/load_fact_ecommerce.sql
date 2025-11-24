/*
================================================================================
Procedure: gold.LoadFactEcommerce
Purpose: Loads the Gold-layer fact table 'fact_ecommerce' from the Silver layer.
         Transfers all relevant event-level data including date, time, product,
         category, price, and user/session information.
================================================================================
*/

CREATE OR ALTER PROCEDURE gold.LoadFactEcommerce
AS
BEGIN
    SET NOCOUNT ON;

    -- ==============================================
    -- Step 1: Insert data from Silver layer
    -- ==============================================
    INSERT INTO gold.fact_ecommerce (
        event_date,
        event_time_only,
        event_type,
        product_id,
        category,
        subcategory,
        price,
        user_id,
        user_session
    )
    SELECT
        event_date,
        event_time_only,
        event_type,
        product_id,
        category,
        subcategory,
        price,
        user_id,
        user_session
    FROM silver.ecommerce_behavior;
END;
GO
