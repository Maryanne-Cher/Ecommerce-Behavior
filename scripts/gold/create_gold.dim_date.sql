/*
========================================================================
Script:       dim_date.sql
Purpose:      Create the DimDate table for date-based analysis in Power BI.
Description:  Generates a date dimension table with attributes such as 
              Year, Month, Day, Quarter, Season, WeekNum, Holidays, and more.
              Covers dates from 2019-10-01 to 2020-04-30.
========================================================================
*/

-- Drop table if it exists
DROP TABLE IF EXISTS gold.dim_date;
GO

-- Create DimDate table
CREATE TABLE gold.dim_date (
    DateKey         INT NOT NULL,         -- YYYYMMDD
    FullDate        DATE NOT NULL,
    YearNum         INT  NOT NULL,
    MonthNum        INT  NOT NULL,
    MonthName       VARCHAR(20) NOT NULL,
    DayNum          INT  NOT NULL,
    DayOfWeekName   VARCHAR(20) NOT NULL,
    DayOfWeekNum    INT  NOT NULL,       -- 1 = Monday, 7 = Sunday
    QuarterNum      INT  NOT NULL,
    WeekNum         INT  NOT NULL,       -- ISO week number
    Season          VARCHAR(10) NOT NULL,
    Holiday         VARCHAR(50) NULL,
    IsWeekend       BIT NOT NULL,
    YearQuarter     VARCHAR(10) NOT NULL,   -- e.g., "2019-Q4"
    YearQuarterSort INT NOT NULL,           -- e.g., 20194
    MonthYear       VARCHAR(20) NOT NULL,   -- e.g., "Oct-2019"
    MonthYearSort   INT NOT NULL            -- e.g., 201910
);
GO

-- Generate dates between Oct 1, 2019 and Apr 30, 2020
WITH Dates AS (
    SELECT CAST('2019-10-01' AS DATE) AS d
    UNION ALL
    SELECT DATEADD(DAY, 1, d)
    FROM Dates
    WHERE d < '2020-04-30'
)
INSERT INTO gold.dim_date
SELECT
    CONVERT(INT, CONVERT(CHAR(8), d, 112))
