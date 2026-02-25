/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It truncates the existing tables before performing a bulk insert.

Parameters:
    @base_path (nvarchar(MAX)) - The root directory containing the dataset subfolders.
                                 Defaults to the local project path.
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.bronze_load
    @base_path NVARCHAR(MAX) = 'C:\Users\Anushka\OneDrive\Documents\GitHub\sql-data-warehouse-project\datasets\'
AS 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    DECLARE @sql NVARCHAR(MAX);

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '================================================';
        
        ---------------------------------------------------------------------------
        PRINT '>>> Loading CRM Tables';
        ---------------------------------------------------------------------------

        -- Loading bronze.crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting data into: bronze.crm_cust_info';
        SET @sql = 'BULK INSERT bronze.crm_cust_info FROM ''' + @base_path + 'source_crm\cust_info.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR='','', TABLOCK)';
        EXEC sp_executesql @sql;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Loading bronze.crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting data into: bronze.crm_prd_info';
        SET @sql = 'BULK INSERT bronze.crm_prd_info FROM ''' + @base_path + 'source_crm\prd_info.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR='','', TABLOCK)';
        EXEC sp_executesql @sql;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Loading bronze.crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting data into: bronze.crm_sales_details';
        SET @sql = 'BULK INSERT bronze.crm_sales_details FROM ''' + @base_path + 'source_crm\sales_details.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR='','', TABLOCK)';
        EXEC sp_executesql @sql;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        ---------------------------------------------------------------------------
        PRINT '>>> Loading ERP Tables';
        ---------------------------------------------------------------------------
        
        -- Loading bronze.erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting data into: bronze.erp_cust_az12';
        SET @sql = 'BULK INSERT bronze.erp_cust_az12 FROM ''' + @base_path + 'source_erp\CUST_AZ12.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR='','', TABLOCK)';
        EXEC sp_executesql @sql;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Loading bronze.erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting data into: bronze.erp_loc_a101';
        SET @sql = 'BULK INSERT bronze.erp_loc_a101 FROM ''' + @base_path + 'source_erp\LOC_A101.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR='','', TABLOCK)';
        EXEC sp_executesql @sql;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Loading bronze.erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> Truncating table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting data into: bronze.erp_px_cat_g1v2';
        SET @sql = 'BULK INSERT bronze.erp_px_cat_g1v2 FROM ''' + @base_path + 'source_erp\PX_CAT_G1V2.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR='','', TABLOCK)';
        EXEC sp_executesql @sql;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    
        SET @batch_end_time = GETDATE();
        PRINT '================================================';
        PRINT 'Bronze Layer Loading Complete';
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';
    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'ERROR OCCURRED DURING BRONZE LOADING';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number:  ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State:   ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';
    END CATCH
END
