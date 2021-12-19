using System;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;

namespace MsSqlCdc;

public enum RelationalOperator
{
    LargestLessThan,
    LargestLessThanOrEqual,
    SmallestGreaterThan,
    SmallestGreaterThanOrEqual
}

public static class Cdc
{
    /// <summary>
    /// Identifies whether the specified update mask indicates that the specified column
    /// has been updated in the associated change row.
    /// </summary>
    /// <param name="connection">An open connection to a MS-SQL database.</param>
    /// <param name="captureInstance">
    /// Is the name of the capture instance in which the specified column is identified as a captured column.
    /// </param>
    /// <param name="columnName">Is the column to report on.</param>
    /// <param name="updateMask">Is the mask identifying updated columns in any associated change row.</param>
    /// <returns>
    /// Returns whether the specified update mask indicates that the specified column
    /// has been updated in the associated change row.
    /// </returns>
    public static async Task<bool> HasColumnChanged(
        SqlConnection connection,
        string captureInstance,
        string columnName,
        string updateMask)
    {
        return await CdcDatabase.HasColumnChanged(connection, captureInstance, columnName, updateMask);
    }

    /// <summary>
    /// Get the column ordinal of the specified column as it appears in the change
    /// table associated with the specified capture instance.
    /// </summary>
    /// <param name="connection">An open connection to a MS-SQL database.</param>
    /// <param name="captureInstance">
    /// Is the name of the capture instance in which the specified column is identified as a captured column.
    /// </param>
    /// <param name="columnName">Is the column to report on.</param>
    /// <returns>
    /// Returns the column ordinal of the specified column as it appears in the change
    /// table associated with the specified capture instance.
    /// </returns>
    public static async Task<int> GetColumnOrdinal(
        SqlConnection connection,
        string captureInstance,
        string columnName)
    {
        return await CdcDatabase.GetColumnOrdinal(connection, captureInstance, columnName);
    }

    /// <summary>
    /// Map the log sequence number (LSN) value from the start_lsn column
    /// in the cdc.lsn_time_mapping system table for the specified time.
    /// </summary>
    /// <param name="connection">An open connection to a MS-SQL database.</param>
    /// <param name="trackingTime">The datetime value to match against. tracking_time is datetime.</param>
    /// <param name="relationalOperator">
    /// Used to identify a distinct LSN value in within the cdc.lsn_time_mapping table with an associated tran_end_time
    /// that satisfies the relation when compared to the tracking_time value.
    /// </param>
    /// <returns>
    /// Returns the log sequence number (LSN) value from the start_lsn column
    /// in the cdc.lsn_time_mapping system table for the specified time.
    /// </returns>
    public static async Task<long> MapTimeToLsn(
        SqlConnection connection,
        DateTime trackingTime,
        RelationalOperator relationalOperator)
    {
        var convertedRelationOperator = DataConvert.RelationOperatorToStringRepresentation(relationalOperator);
        var lsnBytes = await CdcDatabase.MapTimeToLsn(connection, trackingTime, convertedRelationOperator);
        return DataConvert.ConvertBinaryLsn(lsnBytes);
    }

    /// <summary>
    /// Map date and time value from the tran_end_time column in the cdc.lsn_time_mapping
    /// system table for the specified log sequence number (LSN).
    /// You can use this function to systematically map LSN ranges to date ranges in a change table.
    /// </summary>
    /// <param name="connection">An open connection to a MS-SQL database.</param>
    /// <param name="lsn">Is the LSN value to match against.</param>
    /// <returns>
    /// Returns the date and time value from the tran_end_time column in the cdc.lsn_time_mapping
    /// system table for the specified log sequence number (LSN).
    /// </returns>
    public static async Task<DateTime> MapLsnToTime(SqlConnection connection, long lsn)
    {
        return await CdcDatabase.MapLsnToTime(connection, lsn);
    }

    /// <summary>
    /// Get the start_lsn column value for the specified capture instance from the cdc.change_tables system table.
    /// This value represents the low endpoint of the validity interval for the capture instance.
    /// </summary>
    /// <param name="connection">An open connection to a MS-SQL database.</param>
    /// <param name="captureInstance">The name of the capture instance.</param>
    /// <returns>Return the low endpoint of the change data capture timeline for any capture instance.</returns>
    public static async Task<long> GetMinLsn(SqlConnection connection, string captureInstance)
    {
        var minLsnBytes = await CdcDatabase.GetMinLsn(connection, captureInstance);
        return DataConvert.ConvertBinaryLsn(minLsnBytes);
    }

    /// <summary>
    /// Get the maximum log sequence number (LSN) from the start_lsn column in the cdc.lsn_time_mapping system table.
    /// You can use this function to return the high endpoint of the change data capture timeline for any capture instance.
    /// </summary>
    /// <param name="connection">An open connection to a MS-SQL database.</param>
    /// <returns>Return the high endpoint of the change data capture timeline for any capture instance.</returns>
    public static async Task<long> GetMaxLsn(SqlConnection connection)
    {
        var maxLsnBytes = await CdcDatabase.GetMaxLsn(connection);
        return DataConvert.ConvertBinaryLsn(maxLsnBytes);
    }

    /// <summary>
    /// Get the previous log sequence number (LSN) in the sequence based upon the specified LSN.
    /// </summary>
    /// <param name="connection">An open connection to a MS-SQL database.</param>
    /// <param name="lsn">The LSN number that should be used as the point to get the previous LSN.</param>
    /// <returns>Return the high endpoint of the change data capture timeline for any capture instance.</returns>
    public static async Task<long> GetPreviousLsn(SqlConnection connection, long lsn)
    {
        var previousLsnBytes = await CdcDatabase.DecrementLsn(connection, lsn);
        return DataConvert.ConvertBinaryLsn(previousLsnBytes);
    }

    /// <summary>
    /// Get the next log sequence number (LSN) in the sequence based upon the specified LSN.
    /// </summary>
    /// <param name="connection">An open connection to a MS-SQL database.</param>
    /// <param name="lsn">The LSN number that should be used as the point to get the next LSN.</param>
    /// <returns>Get the next log sequence number (LSN) in the sequence based upon the specified LSN.</returns>
    public static async Task<long> GetNextLsn(SqlConnection connection, long lsn)
    {
        var nextLsnBytes = await CdcDatabase.IncrementLsn(connection, lsn);
        return DataConvert.ConvertBinaryLsn(nextLsnBytes);
    }

    /// <summary>
    /// Get one net change row for each source row changed within the specified Log Sequence Numbers (LSN) range.
    /// </summary>
    /// <param name="connection">An open connection to a MS-SQL database.</param>
    /// <param name="captureInstance">The name of the capture instance.</param>
    /// <param name="fromLsn">The LSN that represents the low endpoint of the LSN range to include in the result set.</param>
    /// <param name="toLsn">The LSN that represents the high endpoint of the LSN range to include in the result set.</param>
    /// <returns>
    /// Returns one net change row for each source row changed within the specified Log Sequence Numbers (LSN) range.
    /// </returns>
    public static async Task<IReadOnlyCollection<ChangeRow<dynamic>>> GetNetChanges(
        SqlConnection connection,
        string captureInstance,
        long fromLsn,
        long toLsn)
    {
        var cdcColumns = await CdcDatabase.GetNetChanges(connection, captureInstance, fromLsn, toLsn);
        return cdcColumns.Select(x => DataConvert.ConvertCdcColumn(x, captureInstance)).ToList();
    }

    /// <summary>
    /// Get one row for each change applied to the source table within the specified log sequence number (LSN) range.
    /// If a source row had multiple changes during the interval, each change is represented in the returned result set.
    /// </summary>
    /// <param name="connection">An open connection to a MS-SQL database.</param>
    /// <param name="captureInstance">The name of the capture instance.</param>
    /// <param name="fromLsn">The LSN that represents the low endpoint of the LSN range to include in the result set.</param>
    /// <param name="toLsn">The LSN that represents the high endpoint of the LSN range to include in the result set.</param>
    /// <returns>
    /// Returns one row for each change applied to the source table within the specified log sequence number (LSN) range.
    /// If a source row had multiple changes during the interval, each change is represented in the returned result set.
    /// </returns>
    public static async Task<IReadOnlyCollection<ChangeRow<dynamic>>> GetAllChanges(
        SqlConnection connection,
        string captureInstance,
        long beginLsn,
        long endLsn)
    {
        var cdcColumns = await CdcDatabase.GetAllChanges(connection, captureInstance, beginLsn, endLsn);
        return cdcColumns.Select(x => DataConvert.ConvertCdcColumn(x, captureInstance)).ToList();
    }
}
