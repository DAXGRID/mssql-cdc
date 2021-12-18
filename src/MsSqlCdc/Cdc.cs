using System.Threading.Tasks;
using System.Linq;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;

namespace MsSqlCdc;

public class Cdc
{
    /// <summary>
    /// Returns the start_lsn column value for the specified capture instance from the cdc.change_tables system table.
    /// This value represents the low endpoint of the validity interval for the capture instance.
    /// </summary>
    /// <param name="connection">An open connection to a MS-SQL database.</param>
    /// <param name="captureInstance">The name of the capture instance.</param>
    /// <returns>Returns the CDC column as a ChangeData record.</returns>
    public static async Task<long> GetMinLsn(SqlConnection connection, string captureInstance)
    {
        var minLsnBytes = await CdcDatabase.GetMinLsn(connection, captureInstance);
        return DataConvert.ConvertBinaryLsn(minLsnBytes);
    }

    /// <summary>
    /// Returns the maximum log sequence number (LSN) from the start_lsn column in the cdc.lsn_time_mapping system table.
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
    /// Returns one net change row for each source row changed within the specified Log Sequence Numbers (LSN) range.
    /// </summary>
    /// <param name="connection">An open connection to a MS-SQL database.</param>
    /// <param name="captureInstance">The name of the capture instance.</param>
    /// <param name="fromLsn">The LSN that represents the low endpoint of the LSN range to include in the result set.</param>
    /// <param name="toLsn">The LSN that represents the high endpoint of the LSN range to include in the result set.</param>
    /// <returns>
    /// Returns one net change row for each source row changed within the specified Log Sequence Numbers (LSN) range.
    /// </returns>
    public static async Task<IReadOnlyCollection<ChangeData<dynamic>>> GetNetChanges(
        SqlConnection connection,
        string captureInstance,
        long fromLsn,
        long toLsn)
    {
        var cdcColumns = await CdcDatabase.GetNetChanges(connection, captureInstance, fromLsn, toLsn);
        return cdcColumns.Select(x => DataConvert.ConvertCdcColumn(x, captureInstance)).ToList();
    }

    /// <summary>
    /// Returns one row for each change applied to the source table within the specified log sequence number (LSN) range.
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
    public static async Task<IReadOnlyCollection<ChangeData<dynamic>>> GetAllChanges(
        SqlConnection connection,
        string captureInstance,
        long beginLsn,
        long endLsn)
    {
        var cdcColumns = await CdcDatabase.GetAllChanges(connection, captureInstance, beginLsn, endLsn);
        return cdcColumns.Select(x => DataConvert.ConvertCdcColumn(x, captureInstance)).ToList();
    }
}
