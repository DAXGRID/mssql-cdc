CREATE DATABASE mssql_cdc_test
GO

USE mssql_cdc_test
GO

-- Employee TABLE
CREATE TABLE [dbo].[employee](
	[id] [uniqueidentifier] NOT NULL,
	[first_name] [varchar](50) NULL,
	[last_name] [varchar](50) NULL,
    PRIMARY KEY (id));
GO

-- Enable CDC on Database
EXEC sys.sp_cdc_enable_db;
GO

-- Enable CDC on employee table
EXECUTE sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'employee',
    @role_name = N'null';
GO
