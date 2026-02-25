/*
=======================================================================================================
                                        silver.crm_cust_info
=======================================================================================================
Script Purpose:
    This script contains SQL queries for data quality checks and prototyping the
    transformation logic for the silver.crm_cust_info table.
*/

-- Check for NULLs or duplicates in primary key
-- Expectation: No result
SELECT
    cst_id,
    COUNT(*) AS record_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted spaces
-- Expectation: No results
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);
SELECT cst_gndr FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- Transforming by trimming and data normalization
SELECT
    cst_id,
    cst_key,
    cst_create_date,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status,
    CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr
FROM (
    SELECT
        *,
        ROW_NUMBER()
            OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC)
            AS flag_last
    FROM bronze.crm_cust_info
) AS t
WHERE flag_last = 1;

-- Data standardization and consistency
SELECT DISTINCT cst_gndr FROM bronze.crm_cust_info;
SELECT DISTINCT cst_marital_status FROM bronze.crm_cust_info;

-- Checking results
SELECT * FROM silver.crm_cust_info;

/*
=======================================================================================================
                                        silver.crm_prd_info
=======================================================================================================
*/

-- Check for NULLs or duplicates in primary key
-- Expectation: No result
SELECT
    prd_id,
    COUNT(*) AS record_count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Extracting category id from product key in crm_prd_info table with erp_px_cat_g1v2 table id column
SELECT
    prd_id,
    prd_key,
    prd_name,
    prd_cost,
    prd_line,
    prd_start_date,
    prd_end_date,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
FROM bronze.crm_prd_info
WHERE
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN
    (SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2);

-- Transforming prd_key in crm_prd_info table to match sls_prd_key in crm_sales_details table
SELECT
    prd_id,
    prd_key,
    prd_name,
    prd_cost,
    prd_line,
    prd_start_date,
    prd_end_date,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
FROM bronze.crm_prd_info
WHERE
    SUBSTRING(prd_key, 7, LEN(prd_key)) IN
    (SELECT sls_prd_key FROM bronze.crm_sales_details);

-- Handling missing data in prd_cost column
SELECT
    prd_id,
    prd_key,
    prd_name,
    prd_line,
    prd_start_date,
    prd_end_date,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    ISNULL(prd_cost, 0) AS prd_cost
FROM bronze.crm_prd_info;

-- Check for unwanted spaces
-- Expectation: No results
SELECT prd_name FROM silver.crm_prd_info
WHERE prd_name != TRIM(prd_name);

-- Checking quality of numbers
-- Expectation: No results
SELECT prd_cost FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data standardization & consistency
SELECT DISTINCT prd_line FROM silver.crm_prd_info;

-- Check for invalid date orders
SELECT * FROM silver.crm_prd_info
WHERE prd_end_date < prd_start_date;

/*
   200 rows where this scenario was true.
   In order to clean data so that it projects the right picture, pick few rows
   paste in excel and try to solve it.
   Solution: use start date of next row, -1 it and put as end date.
   Get it approved and integrate in query.
*/

-- Managing date columns
SELECT
    prd_id,
    prd_key,
    prd_name,
    prd_start_date,
    prd_end_date,
    LEAD(prd_start_date) OVER (PARTITION BY prd_key ORDER BY prd_start_date)
    - 1 AS prd_end_date_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

/*
=======================================================================================================
                                        silver.crm_sales_details
=======================================================================================================
*/

-- Handle invalid sls_order_dt
SELECT NULLIF(sls_order_dt, 0) AS sls_order_dt FROM silver.crm_sales_details
WHERE
    sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 OR sls_order_dt > 20500101 OR sls_order_dt < 19000101;

-- Handle invalid sls_ship_dt
SELECT NULLIF(sls_ship_dt, 0) AS sls_ship_dt FROM bronze.crm_sales_details
WHERE
    sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 OR sls_ship_dt > 20500101 OR sls_ship_dt < 19000101;

-- Check for referential integrity (Customer ID)
SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- Check for referential integrity (Product Key)
SELECT *
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

-- Check for invalid date orders
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Check data consistency between sales, quantity and price
-- sales = quantity * price
-- Values must not be NULL, zero or negative
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE
    sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
    OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0;

-- Check all results
SELECT * FROM silver.crm_sales_details;

/*
=======================================================================================================
                                        silver.erp_cust_az12
=======================================================================================================
*/

-- Identify missing customer keys
SELECT
    bdate,
    gen,
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid
FROM bronze.erp_cust_az12
WHERE CASE
    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);

-- Identify out of range dates
SELECT DISTINCT bdate FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Data standardization & consistency
SELECT DISTINCT gen FROM silver.erp_cust_az12;

-- Check all results
SELECT * FROM silver.erp_cust_az12;
