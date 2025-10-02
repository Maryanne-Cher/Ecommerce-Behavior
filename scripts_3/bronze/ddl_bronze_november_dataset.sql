/*
Bronze Layer Table: ecommerce_nov

Purpose:
- Stores raw ecommerce event data for November.
- Each row represents a single user event (e.g., page view, purchase, cart add).
- This is a 'bronze' table in a typical data lake/warehouse pipeline, meaning data is raw and minimally transformed.

Columns:
- event_time: Full timestamp of the event.
- event_type: Type of event (view, purchase, etc.).
- product_id, category_id, category_code, brand, price: Product-related details.
- user_id, user_session: User identifiers and session info.
- event_date, event_time_only: Date and time split for easier analysis.
- category, subcategory: Optional product categorization.
*/

DROP TABLE IF EXISTS [bronze].[ecommerce_nov];
GO

CREATE TABLE [bronze].[ecommerce_nov](
    [event_time] datetime2(3) NULL,
    [event_type] varchar(10) NULL,
    [product_id] bigint NULL,
    [category_id] bigint NULL,
    [category_code] varchar(100) NULL,
    [brand] varchar(50) NULL,
    [price] decimal(10,2) NULL,
    [user_id] bigint NOT NULL,
    [user_session] varchar(36) NOT NULL,
    [event_date] date NULL,
    [event_time_only] time(0) NULL,
    [category] varchar(50) NULL,
    [subcategory] varchar(50) NULL
) ON [PRIMARY];
GO

