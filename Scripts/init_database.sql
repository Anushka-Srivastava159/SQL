/*
===============================================================================
Database Initialization Script: Create Database and Schemas
===============================================================================
Script Purpose:
    This script initializes the 'DataWarehouse' database. It first checks if the 
    database already exists and, if so, drops it to ensure a clean setup. 
    It then creates the database and defines the standard Medallion Architecture 
    schemas: 'bronze', 'silver', and 'gold'.

Note:
    The script requires administrative privileges to drop and recreate the 
    database. Use with caution in production environments.
===============================================================================
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create standard Medallion Architecture schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

PRINT '================================================';
PRINT 'Database Initialization Complete';
PRINT 'Database: DataWarehouse';
PRINT 'Schemas: bronze, silver, gold';
PRINT '================================================';
