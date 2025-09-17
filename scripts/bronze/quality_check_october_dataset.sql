-- =========================================================
-- Data Quality Checks & Transformations
-- Dataset: bronze.ecommerce_oct_partitioned
-- =========================================================

-- 1. Check which columns contain NULL values
--    - category_code and brand have NULLs
--    - user_session has 2 NULL rows
SELECT *
FROM bronze.ecommerce_oct_partitioned
WHERE product_id = 4100129;


-- 2. Check for unwanted spaces in product_id
SELECT product_id
FROM bronze.ecommerce_oct_partitioned
WHERE product_id != TRIM(product_id);


-- 3. Inspect distinct values in category_code
SELECT DISTINCT category_code
FROM bronze.ecommerce_oct_partitioned;


-- 4. Identify NULL user_session values (to be replaced with 'UNKNOWN')
SELECT *
FROM bronze.ecommerce_oct_partitioned
WHERE user_session IS NULL;


-- 5. Split category_code into category & subcategory
SELECT 
    category_code,
    ISNULL(LEFT(category_code, CHARINDEX('.', category_code) - 1), 'UNKNOWN') AS category,
    ISNULL(SUBSTRING(category_code, CHARINDEX('.', category_code) + 1, LEN(category_code)), 'UNKNOWN') AS subcategory
FROM bronze.ecommerce_oct_partitioned;
