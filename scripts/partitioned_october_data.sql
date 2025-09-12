/********************************************************************
-- Bronze Ecommerce October Partitioning Script
-- Description: Splits datetime column, creates partitions by day,
-- adds filegroups, and creates a partitioned table with a clustered
-- columnstore index.
********************************************************************/

------------------------------
-- 1. Split Date and Time
------------------------------
ALTER TABLE bronze.ecommerce_oct
ADD event_date AS CAST(event_time AS DATE) PERSISTED,
    event_time_only AS CONVERT(VARCHAR(8), event_time, 108) PERSISTED;

-- Verify the data
SELECT TOP 10000 * 
FROM bronze.ecommerce_oct;

-- Check distinct event dates
SELECT DISTINCT event_date 
FROM bronze.ecommerce_oct
ORDER BY event_date ASC;

------------------------------
-- 2. Create Partition Function
------------------------------
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

-- List all existing Partition Functions
SELECT name, function_id, type, type_desc, boundary_value_on_right
FROM sys.partition_functions;

------------------------------
-- 3. Create Filegroups
------------------------------
ALTER DATABASE ecommerce_behavior ADD FILEGROUP FG_2019;

-- Check existing filegroups
SELECT * 
FROM sys.filegroups
WHERE type = 'FG';

-- Add .ndf file to the new filegroup
ALTER DATABASE ecommerce_behavior 
ADD FILE (
    NAME = P_2019,           -- Logical name
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\P_2019.ndf'
) TO FILEGROUP FG_2019;

-- Verify filegroup metadata
SELECT
    fg.name AS FileGroupName,
    mf.name AS LogicalFileName,
    mf.physical_name AS PhysicalFilePath,
    mf.size / 128 AS SizeINMB
FROM sys.filegroups fg
JOIN sys.master_files mf ON fg.data_space_id = mf.data_space_id
WHERE mf.database_id = DB_ID('ecommerce_behavior');

------------------------------
-- 4. Create Partition Scheme
------------------------------
CREATE PARTITION SCHEME SchemePartitionByDay
AS PARTITION PartitionByDay
ALL TO (FG_2019);

-- Check Partition Scheme metadata
SELECT
    ps.name AS PartitionSchemeName,
    pf.name AS PartitionFunctionName,
    ds.destination_id AS PartitionNumber,
    fg.name AS FileGroupName
FROM sys.partition_schemes ps
JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
JOIN sys.destination_data_spaces ds ON ps.data_space_id = ds.partition_scheme_id
JOIN sys.filegroups fg ON ds.data_space_id = fg.data_space_id;

------------------------------
-- 5. Create Partitioned Table
------------------------------
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

-- Insert data into partitioned table
INSERT INTO bronze.ecommerce_oct_partitioned (
    event_type, product_id, category_id, category_code, brand, 
    price, user_id, user_session, event_date, event_time_only
)
SELECT 
    event_type, product_id, category_id, category_code, brand, 
    price, user_id, user_session, event_date, event_time_only
FROM bronze.ecommerce_oct;

-- Verify partitioned data
SELECT
    p.partition_number AS PartitionNumber,
    f.name AS PartitionFileGroup,
    p.rows AS NumberOfRows
FROM sys.partitions p
JOIN sys.destination_data_spaces dds ON p.partition_number = dds.destination_id
JOIN sys.filegroups f ON dds.data_space_id = f.data_space_id
WHERE OBJECT_NAME(p.object_id) = 'ecommerce_oct_partitioned';

------------------------------
-- 6. Create Clustered Columnstore Index
------------------------------
CREATE CLUSTERED COLUMNSTORE INDEX Idx_ecommerce 
ON bronze.ecommerce_oct_partitioned;

-- Verify execution
SELECT * 
FROM bronze.ecommerce_oct_partitioned 
WHERE event_date = '2019-10-01';
