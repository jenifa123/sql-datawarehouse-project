/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWH' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWH' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/


use master;
GO

--Drop and recreate 'DataWH' DB
IF EXISTS (select 1 from sys.databases where name ='DataWH')
BEGIN
	ALTER Database DataWH set SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP Database DataWH
END;
GO

--Create a database
create database DataWH;
GO
use DataWH;

GO
--Create schemas
create schema bronze;

GO
create schema silver;
GO 
create schema gold
