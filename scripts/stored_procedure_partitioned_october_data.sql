/********************************************************************
-- Stored Procedure: sp_PartitionEcommerceOct
-- Description: Automates creation of partitioned table for 
--              bronze.ecommerce_oct, including splitting datetime, 
--              partitioning, filegroups, and adding clustered columnstore index.
********************************************************************/

CREATE PROCEDURE bronze.sp_PartitionEcommerceOct
AS
BEGIN
    SET NOCOUNT ON;

    ------------------------------------------------------------------
    -- 1. Split datetime column into date and time
    ------------------------------------------------------------------
    IF COL_LENGTH('bronze.ecommerce_oct', 'event_date') IS NULL
    BEGIN
        ALTER TABLE bronze.ecommerce_oct
        ADD event_date AS CAST(event_time AS DATE) PERSISTED,
            event_time_only AS CONVERT(TIME, event_time) PERSISTED;
    END

    ------------------------------------------------------------------
    -- 2. Create Partition Function
    ------------------------------------------------------------------
    IF NOT EXISTS (SELECT * FROM sys.partition_functions WHERE name = 'PartitionByDay')
    BEGIN
        CREATE PARTITION FUNCTION PartitionByDay (DATE)
        AS RANGE LEFT FOR VALUES (
            '2019-10-01','2019-10-02','2019-10-03','2019-10-04','2019-10-05',
            '2019-10-06','2019-10-07','2019-10-08','2019-10-09','2019-10-10',
            '2019-10-11','2019-10-12','2019-10-13','2019-10-14','2019-10-15',
            '2019-10-16','2019-10-17','2019-10-18','2019-10-19','2019-10-20',
            '2019-10-21','2019-10-22','2019-10-23','2019-10-24','2019-10-25',
            '2019-10-26','2019-10-27','2019-10-28','2019-10-29','2019-10-30',
            '2019-10-31'
        );
    END

    ------------------------------------------------------------------
    -- 3. Create Filegroup and File
    ------------------------------------------------------------------
    IF NOT EXISTS (SELECT * FROM sys.filegroups WHERE name = 'FG_2019')
    BEGIN
        ALTER DATABASE ecommerce_behavior ADD FILEGROUP FG_2019;
        ALTER DATABASE ecommerce_behavior ADD FILE (
            NAME = P_2019,
            FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\P_2019.ndf'
        ) TO FILEGROUP FG_2019;
    END

    ------------------------------------------------------------------
    -- 4. Create Partition Scheme
    ------------------------------------------------------------------
    IF NOT EXISTS (SELECT * FROM sys.partition_schemes WHERE name = 'SchemePartitionByDay')
    BEGIN
        CREATE PARTITION SCHEME SchemePartitionByDay
        AS PARTITION PartitionByDay ALL TO (FG_2019);
    END

    ------------------------------------------------------------------
    -- 5. Create Partitioned Table
    ------------------------------------------------------------------
    IF OBJECT_ID('bronze.ecommerce_oct_partitioned', 'U') IS NULL
    BEGIN
        CREATE TABLE bronze.ecommerce_oct_partitioned
        (
            event_type VARCHAR(20) NULL,
            product_id BIGINT NULL,
            category_id BIGINT NULL,
            category_code NVARCHAR(100) NULL,
            brand NVARCHAR(60) NULL,
            price FLOAT NULL,
            user_id BIGINT NULL,
            user_session NVARCHAR(100),
            event_date DATE NOT NULL,
            event_time_only TIME NULL
        ) ON SchemePartitionByDay (event_date);
    END

    ------------------------------------------------------------------
    -- 6. Insert Data into Partitioned Table
    ------------------------------------------------------------------
    INSERT INTO bronze.ecommerce_oct_partitioned (
        event_type, product_id, category_id, category_code, brand,
        price, user_id, user_session, event_date, event_time_only
    )
    SELECT event_type, product_id, category_id, category_code, brand,
           price, user_id, user_session, event_date, event_time_only
    FROM bronze.ecommerce_oct;

    ------------------------------------------------------------------
    -- 7. Create Clustered Columnstore Index
    ------------------------------------------------------------------
    IF NOT EXISTS (
        SELECT * FROM sys.indexes 
        WHERE name = 'Idx_ecommerce' 
          AND object_id = OBJECT_ID('bronze.ecommerce_oct_partitioned')
    )
    BEGIN
        CREATE CLUSTERED COLUMNSTORE INDEX Idx_ecommerce 
        ON bronze.ecommerce_oct_partitioned;
    END

    PRINT '✅ Partitioned table created and populated successfully.';
END;
GO
