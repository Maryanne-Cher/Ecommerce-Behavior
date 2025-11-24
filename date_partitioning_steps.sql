/********************************************************************
-- Bronze Ecommerce Monthly Partitioning Script
-- Description: 
--   1. Creates a partition function for monthly ranges
--   2. Adds filegroups and associated data files
--   3. Creates a partition scheme
--   4. Provides metadata checks at each step
********************************************************************/

------------------------------
-- Step 1: Create Partition Function
------------------------------
-- Partition by month for Nov 2019 to Apr 2020
CREATE PARTITION FUNCTION PartitionByMonth (DATE)
AS RANGE LEFT FOR VALUES (
    '2019-11-30',  -- November 2019
    '2019-12-31',  -- December 2019
    '2020-01-31',  -- January 2020
    '2020-02-29',  -- February 2020 (leap year)
    '2020-03-31',  -- March 2020
    '2020-04-30'   -- April 2020
);

-- List all existing Partition Functions
SELECT
    name,
    function_id,
    type,
    type_desc,
    boundary_value_on_right
FROM sys.partition_functions;

------------------------------
-- Step 2: Create FileGroups
------------------------------
-- Each filegroup will store a single month partition
ALTER DATABASE ecommerce_behavior ADD FILEGROUP FG_201911; -- November 2019
ALTER DATABASE ecommerce_behavior ADD FILEGROUP FG_201912; -- December 2019
ALTER DATABASE ecommerce_behavior ADD FILEGROUP FG_202001; -- January 2020
ALTER DATABASE ecommerce_behavior ADD FILEGROUP FG_202002; -- February 2020
ALTER DATABASE ecommerce_behavior ADD FILEGROUP FG_202003; -- March 2020
ALTER DATABASE ecommerce_behavior ADD FILEGROUP FG_202004; -- April 2020
ALTER DATABASE ecommerce_behavior ADD FILEGROUP FG_Extra;  -- Extra filegroup for additional partitions

-- Verify filegroups
SELECT *
FROM sys.filegroups
WHERE type = 'FG';

------------------------------
-- Step 3: Add .ndf Files to Each FileGroup
------------------------------
-- Add file for November 2019
ALTER DATABASE ecommerce_behavior ADD FILE
(
    NAME = P_201911, -- Logical Name
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\P_201911.ndf'
) TO FILEGROUP FG_201911;

-- Add file for December 2019
ALTER DATABASE ecommerce_behavior ADD FILE
(
    NAME = P_201912, -- Logical Name
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\P_201912.ndf'
) TO FILEGROUP FG_201912;

-- Add file for January 2020
ALTER DATABASE ecommerce_behavior ADD FILE
(
    NAME = P_202001, -- Logical Name
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\P_202001.ndf'
) TO FILEGROUP FG_202001;

-- Add file for February 2020
ALTER DATABASE ecommerce_behavior ADD FILE
(
    NAME = P_202002, -- Logical Name
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\P_202002.ndf'
) TO FILEGROUP FG_202002;

-- Add file for March 2020
ALTER DATABASE ecommerce_behavior ADD FILE
(
    NAME = P_202003, -- Logical Name
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\P_202003.ndf'
) TO FILEGROUP FG_202003;

-- Add file for April 2020
ALTER DATABASE ecommerce_behavior ADD FILE
(
    NAME = P_202004, -- Logical Name
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\P_202004.ndf'
) TO FILEGROUP FG_202004;

-- Add file for extra_data (required because we have 7 partitions)
ALTER DATABASE ecommerce_behavior ADD FILE
(
    NAME = P_Extra, -- Logical Name
    FILENAME = 'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA\P_Extra.ndf'
) TO FILEGROUP FG_Extra;

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
-- Step 4: Create Partition Scheme
------------------------------
-- Maps the partition function to the filegroups
CREATE PARTITION SCHEME SchemePartitionByMonth
AS PARTITION PartitionByMonth
TO (
    FG_201911, FG_201912, FG_202001, FG_202002, FG_202003, FG_202004, FG_Extra
);

-- Verify the partition scheme
SELECT
    ps.name AS PartitionSchemeName,
    pf.name AS PartitionFunctionName,
    ds.destination_id AS PartitionNumber,
    fg.name AS FileGroupName
FROM sys.partition_schemes ps
JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
JOIN sys.destination_data_spaces ds ON ps.data_space_id = ds.partition_scheme_id
JOIN sys.filegroups fg ON ds.data_space_id = fg.data_space_id;
