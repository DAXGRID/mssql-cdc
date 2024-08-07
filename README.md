# Microsoft SQL - Change Data Capture (CDC)

You can get the NuGet package [here.](https://www.nuget.org/packages/MsSqlCdc)

## Introduction

This library streamlines the utilization of MS-SQL-CDC by offering a simplified API that facilitates interaction with the SQL functions specific to MS-SQL-CDC. The design of the API deliberately prioritizes simplicity, empowering users to develop their own abstractions if necessary.

### Examples

Usage examples can be found under the [example folder](https://github.com/DAXGRID/mssql-cdc/tree/master/examples).

## API

### Get min LSN

Get the start_lsn column value for the specified capture instance from the cdc.change_tables system table. This value represents the low endpoint of the validity interval for the capture instance.

```c#
using var connection = new SqlConnection("myConnectionString");
await connection.OpenAsync();
var minLsn = await Cdc.GetMinLsnAsync(connection, "dbo_Employee");
```

### Get max LSN

Get the maximum log sequence number (LSN) from the start_lsn column in the cdc.lsn_time_mapping system table. You can use this function to return the high endpoint of the change data capture timeline for any capture instance.

```c#
using var connection = new SqlConnection("myConnectionString");
await connection.OpenAsync();
var maxLsn = await Cdc.GetMaxLsnAsync(connection);
```

### Get previous LSN

Get the previous log sequence number (LSN) in the sequence based upon the specified LSN.

```c#
using var connection = new SqlConnection("myConnectionString");
await connection.OpenAsync();
var previousLsn = await Cdc.GetPreviousLsnAsync(connection, 120000);
```

### Get next LSN

Get the next log sequence number (LSN) in the sequence based upon the specified LSN.

```c#
using var connection = new SqlConnection("myConnectionString");
await connection.OpenAsync();
var nextLsn = await Cdc.GetNextLsnAsync(connection, 120000);
```

### Map time to LSN

Map the log sequence number (LSN) value from the start_lsn column in the cdc.lsn_time_mapping system table for the specified time.

```c#
using var connection = new SqlConnection("myConnectionString");
await connection.OpenAsync();
var lsn = await Cdc.MapTimeToLsnAsync(connection, DateTime.UtcNow, RelationalOperator.LargestLessThan);
```

### Map LSN to time

Map date and time value from the tran_end_time column in the cdc.lsn_time_mapping system table for the specified log sequence number (LSN). You can use this function to systematically map LSN ranges to date ranges in a change table.

```c#
using var connection = new SqlConnection("myConnectionString");
await connection.OpenAsync();
var time = await Cdc.MapLsnToTimeAsync(connection, 120000);
```

### Get all changes

Get one row for each change applied to the source table within the specified log sequence number (LSN) range. If a source row had multiple changes during the interval, each change is represented in the returned result set.

```c#
using var connection = new SqlConnection("myConnectionString");
await connection.OpenAsync();
var allChanges = await Cdc.GetAllChangesAsync(connection, "dbo_Employee", 120000, 120020);
```

### Get net changes

Get one net change row for each source row changed within the specified Log Sequence Numbers (LSN) range.

```c#
using var connection = new SqlConnection("myConnectionString");
await connection.OpenAsync();
var netChanges = await Cdc.GetNetChangesAsync(connection, "dbo_Employee", 120000, 120020);
```

### Get column ordinal

Get the column ordinal of the specified column as it appears in the change table associated with the specified capture instance.

```c#
using var connection = new SqlConnection("myConnectionString");
await connection.OpenAsync();
var columnOrdinal = await Cdc.GetColumnOrdinalAsync(connection, "dbo_Employee", "Salary");
```

### Has column changed

Identifies whether the update mask on the specified column has been updated in the associated change row.

```c#
var captureInstance = "dbo_Employee";

using var connection = new SqlConnection("myConnectionString");
await connection.OpenAsync();

var minLsn = await Cdc.GetMinLsnAsync(connection, captureInstance);
var maxLsn = await Cdc.GetMaxLsnAsync(connection);

var columnOrdinal = await Cdc.GetColumnOrdinalAsync(connection, captureInstance, columnName);
var changes = await Cdc.GetAllChangesAsync(
                      connection,
                      captureInstance,
                      minLsn,
                      maxLsn,
                      AllChangesRowFilterOption.AllUpdateOld);

// We just pick the first one here as an example.
var updateMask = changes.First().GetUpdateMask();

var hasColumnChanged = await Cdc.HasColumnChangedAsync(connection, captureInstance, columnName, updateMask);
```

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

## Testing

### Running all tests

```sh
dotnet test
```

### Running unit tests

```sh
dotnet test --filter Category=Unit
```

### Running integration tests

```sh
dotnet test --filter Category=Integration
```

To run the integration tests, a running MS-SQL database is required. You can use the docker command below to set up a local MS-SQL database to run the integration tests against.

```sh
docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=myAwesomePassword1" -e "MSSQL_AGENT_ENABLED=True"  -p 1433:1433 -d mcr.microsoft.com/mssql/server:2022-CU13-ubuntu-22.04
```
