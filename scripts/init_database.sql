/*
create database and schemas
Script purpose:
This script creates a new database named 'Datawarehouse' after checking if it already exists.
If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas within the database: Bronze, Silver, and Gold. 

*/

use master;
go

if exists(select 1 from sys.databases wehre name = 'DataWarehouse')
begin 
     alter database DataWarehouse set single_user with rollback immediate;
     drop database DataWarehouse;
end;
go

-- create the 'DataWarehouse' Database
create database DataWarehouse 
go

use DataWarehouse;
go

-- create schemas
create schema bronze;
go

create schema silver;
go

create schema gold;
go
