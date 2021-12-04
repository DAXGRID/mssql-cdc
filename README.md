# Microsoft SQL - Change Data Capture (CDC)

NOTE: Should not be used yet, still under development.

The MS-SQL change data capture library, integrates with MSSQL through its change data capture (CDC) system. It checks a range of specified CDC groups that each contains multiple CDC tables and streams the changesets in order based on the sequence number from the database log. The library aims only to handle the CDC integration with the datasource and therefore not the destination of the event stream.

## ETL Streaming VS Batch ETL

Instead of using batch ETL(extract, transform, load) based on schedule the library aims to provide a simple API to do ETL streaming of database events aimed at real-time applications.

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
