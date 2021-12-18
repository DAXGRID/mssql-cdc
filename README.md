# Microsoft SQL - Change Data Capture (CDC)

NOTE: Still under development, API might change until stable release.

You can get the NuGet package [here.](https://www.nuget.org/packages/MsSqlCdc)

## Introduction

The MS-SQL change data capture library, simplifies using MS-SQL-CDC by providing a simplified API to interact with the SQL functions for MS-SQL-CDC. The API has intentionally been made very simplistic, because the use-cases of the consumers of the library can vary a lot.

## Setup CDC on MS-SQL Server

First make sure that you've enabled the MS-SQL agent, otherwise changes won't be captured in the CDC tables.

### Enable CDC on the database

Shows the simplest way to setup CDC on a MSSQL server for a single table named `MyTable`. You can read more [here](https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server?view=sql-server-ver15)

```sql
USE MyDB
GO
EXEC sys.sp_cdc_enable_db
GO
```


### Enable CDC on a table

```sql
USE MyDB
GO
EXEC sys.sp_cdc_enable_table
@source_schema = N'dbo',
@source_name   = N'MyTable',
@role_name     = N'MyRole',
@supports_net_changes = 1
GO
```
