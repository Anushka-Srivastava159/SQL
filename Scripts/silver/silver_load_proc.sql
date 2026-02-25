/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL process to clean and transform data 
    from the 'bronze' schema into the 'silver' schema. The transformations include:
    - Normalizing gender and marital status.
    - Trimming strings and handling nulls.
    - Datatype casting and date validation.
    - Business rule enforcement (e.g., re-calculating sales).

Usage Example:
    EXEC silver.silver_load;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.silver_load AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

        ---------------------------------------------------------------------------
        PRINT '>>> Loading CRM Tables';
        ---------------------------------------------------------------------------

        -- Loading silver.crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>> Inserting data into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id, 
            cst_key, 
            cst_firstname, 
            cst_lastname, 
            cst_marital_status, 
            cst_gndr, 
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                 ELSE 'n/a'
            END,
            CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                 ELSE 'n/a'
            END,
            cst_create_date
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
            FROM bronze.crm_cust_info
        ) t WHERE flag_last = 1 AND cst_id IS NOT NULL;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Loading silver.crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT '>> Inserting data into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_name,
            prd_cost,
            prd_line,
            prd_start_date,
            prd_end_date
        )
        SELECT 
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
            SUBSTRING(prd_key, 7, LEN(prd_key)),
            prd_name,
            ISNULL(prd_cost, 0),
            CASE UPPER(TRIM(prd_line)) 
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END,
            CAST(prd_start_date AS DATE),
            CAST(LEAD(prd_start_date) OVER (PARTITION BY prD_key ORDER BY prd_start_date) - 1 AS DATE)
        FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Loading silver.crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT '>> Inserting data into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END,
            CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * sls_price 
                      THEN sls_quantity * ABS(sls_price)
                 ELSE sls_sales
            END,
            sls_quantity,
            CASE WHEN sls_price IS NULL OR sls_price <= 0
                      THEN sls_sales / NULLIF(sls_quantity, 0)
                 ELSE sls_price
            END
        FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        ---------------------------------------------------------------------------
        PRINT '>>> Loading ERP Tables';
        ---------------------------------------------------------------------------

        -- Loading silver.erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT '>> Inserting data into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT 
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                 ELSE cid
            END,
            CASE WHEN bdate > GETDATE() THEN NULL
                 ELSE bdate
            END,
            CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                 ELSE 'n/a'
            END
        FROM bronze.erp_cust_az12;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Loading silver.erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT '>> Inserting data into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT 
            cid,
            CASE WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
                 WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
                 WHEN UPPER(TRIM(cntry)) = 'FR' THEN 'France'
                 WHEN UPPER(TRIM(cntry)) = 'UK' THEN 'United Kingdom'
                 WHEN UPPER(TRIM(cntry)) = 'CA' THEN 'Canada'
                 WHEN UPPER(TRIM(cntry)) = 'AU' THEN 'Australia'
                 ELSE 'n/a'
            END
        FROM bronze.erp_loc_a101;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Loading silver.erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT '>> Inserting data into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT id, cat, subcat, maintenance FROM bronze.erp_px_cat_g1v2;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        SET @batch_end_time = GETDATE();
        PRINT '================================================';
        PRINT 'Silver Layer Loading Complete';
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';
    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING SILVER LOADING';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number:  ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State:   ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';
    END CATCH
END
