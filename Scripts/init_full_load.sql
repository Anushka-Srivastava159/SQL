/*
===============================================================================
Master Deployment & Load Script
===============================================================================
Script Purpose:
    This script orchestrates the entire deployment and loading process of the 
    Data Warehouse. It executes scripts in the correct order to ensure 
    dependencies are respected.

Order of Execution:
    1. Initialize Database & Schemas
    2. Create & Load Bronze Layer (Raw)
    3. Create & Load Silver Layer (Cleaned)
    4. Create Gold Layer (Analytics/Views)
===============================================================================
*/

PRINT '================================================';
PRINT 'Starting Full Data Warehouse Deployment';
PRINT '================================================';

-- 1. Initialize Database
PRINT '>>> 1. Initializing Database and Schemas';
:r "init_database.sql"
GO

-- 2. Bronze Layer
PRINT '>>> 2. Setting up Bronze Layer';
:r "bronze/ddl_bronze.sql"
GO
PRINT '>>> Loading Bronze Data';
:r "bronze/bronze_load_proc.sql"
GO
EXEC bronze.load_bronze;
GO

-- 3. Silver Layer
PRINT '>>> 3. Setting up Silver Layer';
:r "silver/ddl_silver.sql"
GO
PRINT '>>> Loading Silver Data';
:r "silver/silver_load_proc.sql"
GO
EXEC silver.silver_load;
GO

-- 4. Gold Layer
PRINT '>>> 4. Setting up Gold Layer';
:r "gold/ddl_gold.sql"
GO

PRINT '================================================';
PRINT 'Full Data Warehouse Deployment Complete';
PRINT '================================================';
