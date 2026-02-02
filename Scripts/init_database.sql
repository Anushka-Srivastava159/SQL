/*
===============================================================
Create Databse and Schemas
===============================================================
Script Purpose:
  This script creates a new databse names 'DataWarehouse' after checking if it already exists.
  If the database already exists, it is dropped and recreated. Additionally, the script sets up three schemas whithin 
  the database: bronze, silver and gold.
*/

use master;
Go

--Drop and recreate the 'DataWarehouse' db
if exists (select 1 from sys.database where name = 'dataWarehouse')
begin
	alter database DataWarehouse set single user with rollback immediate;
	drop database DataWarehouse;
end;
Go

--Create "DataWarehouse' databse
create database DataWarehouse;
Go

use DataWarehouse;
Go

create schema bronze;
Go
create schema silver;
Go
create schema gold;
Go
Go
