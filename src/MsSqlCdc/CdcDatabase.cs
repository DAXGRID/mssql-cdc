using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MsSqlCdc;

internal static class CdcDatabase
{
    public static async Task<bool?> HasColumnChanged(
        SqlConnection connection,
        string captureInstance,
        string columnName,
        byte[] updateMask)
    {
        var sql = "SELECT sys.fn_cdc_has_column_changed(@capture_instance, @column_name, @update_mask)";

        using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.AddWithValue("@capture_instance", captureInstance);
        _ = command.Parameters.AddWithValue("@column_name", columnName);
        _ = command.Parameters.AddWithValue("@update_mask", updateMask);

        var hasColumnChanged = await command.ExecuteScalarAsync().ConfigureAwait(false);

        return hasColumnChanged != DBNull.Value ? (bool?)hasColumnChanged : null;
    }

    public static async Task<int?> GetColumnOrdinal(
        SqlConnection connection,
        string captureInstance,
        string columnName)
    {
        var sql = "SELECT sys.fn_cdc_get_column_ordinal(@capture_instance, @column_name)";

        using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.AddWithValue("@capture_instance", captureInstance);
        _ = command.Parameters.AddWithValue("@column_name", columnName);

        var columnOrdinal = await command.ExecuteScalarAsync().ConfigureAwait(false);

        return columnOrdinal != DBNull.Value ? (int?)columnOrdinal : null;
    }

    public static async Task<DateTime?> MapLsnToTime(SqlConnection connection, byte[] lsn)
    {
        var sql = "SELECT sys.fn_cdc_map_lsn_to_time(@lsn)";

        using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.AddWithValue("@lsn", lsn);

        var lsnTime = await command.ExecuteScalarAsync().ConfigureAwait(false);

        return lsnTime != DBNull.Value ? (DateTime?)lsnTime : null;
    }

    public static async Task<byte[]?> MapTimeToLsn(
        SqlConnection connection,
        DateTime trackingTime,
        string relationOperator)
    {
        var sql = "SELECT sys.fn_cdc_map_time_to_lsn(@relational_operator, @tracking_time)";

        using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.AddWithValue("@relational_operator", relationOperator);
        _ = command.Parameters.AddWithValue("@tracking_time", trackingTime);

        var lsnBasedOnTime = await command.ExecuteScalarAsync().ConfigureAwait(false);

        return lsnBasedOnTime != DBNull.Value ? (byte[]?)lsnBasedOnTime : null;
    }

    public static async Task<byte[]?> GetMinLsn(SqlConnection connection, string captureInstance)
    {
        var sql = "SELECT sys.fn_cdc_get_min_lsn(@capture_instance)";

        using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.AddWithValue("@capture_instance", captureInstance);

        var minLsn = await command.ExecuteScalarAsync().ConfigureAwait(false);

        return minLsn != DBNull.Value ? (byte[]?)minLsn : null;
    }

    public static async Task<byte[]?> GetMaxLsn(SqlConnection connection)
    {
        var sql = "SELECT sys.fn_cdc_get_max_lsn()";
        using var command = new SqlCommand(sql, connection);
        var maxLsn = await command.ExecuteScalarAsync().ConfigureAwait(false);

        return maxLsn != DBNull.Value ? (byte[]?)maxLsn : null;
    }

    public static async Task<byte[]?> DecrementLsn(SqlConnection connection, byte[] lsn)
    {
        var sql = "SELECT sys.fn_cdc_decrement_lsn(@lsn)";

        using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.AddWithValue("@lsn", lsn);

        var decrementedLsn = await command.ExecuteScalarAsync().ConfigureAwait(false);

        return decrementedLsn != DBNull.Value ? (byte[]?)decrementedLsn : null;
    }

    public static async Task<byte[]?> IncrementLsn(SqlConnection connection, byte[] lsn)
    {
        var sql = "SELECT sys.fn_cdc_increment_lsn(@lsn)";

        using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.AddWithValue("@lsn", lsn);

        var incrementedLsn = await command.ExecuteScalarAsync().ConfigureAwait(false);

        return incrementedLsn != DBNull.Value ? (byte[]?)incrementedLsn : null;
    }

    public static async Task<List<IReadOnlyDictionary<string, object>>> GetAllChanges(
        SqlConnection connection,
        string captureInstance,
        byte[] beginLsn,
        byte[] endLsn,
        string filterOption)
    {
        return await GetChanges(
            connection,
            "cdc.fn_cdc_get_all_changes",
            captureInstance,
            beginLsn,
            endLsn,
            filterOption).ConfigureAwait(false);
    }

    public static async Task<List<IReadOnlyDictionary<string, object>>> GetNetChanges(
        SqlConnection connection,
        string captureInstance,
        byte[] beginLsn,
        byte[] endLsn,
        string filterOption)
    {
        return await GetChanges(
            connection,
            "cdc.fn_cdc_get_net_changes",
            captureInstance,
            beginLsn,
            endLsn,
            filterOption).ConfigureAwait(false);
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage
    ("Security", "CA2100:Review SQL queries for security vulnerabilities",
     Justification = "No user input.")]
    private static async Task<List<IReadOnlyDictionary<string, object>>> GetChanges(
        SqlConnection connection,
        string cdcFunction,
        string captureInstance,
        byte[] beginLsn,
        byte[] endLsn,
        string filterOption)
    {
        var sql = $"SELECT * FROM {cdcFunction}_{captureInstance}(@begin_lsn, @end_lsn, @filter_option)";

        using var command = new SqlCommand(sql, connection);
        _ = command.Parameters.AddWithValue("@begin_lsn", beginLsn);
        _ = command.Parameters.AddWithValue("@end_lsn", endLsn);
        _ = command.Parameters.AddWithValue("@filter_option", filterOption);

        var columns = new List<IReadOnlyDictionary<string, object>>();
        using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            var column = new Dictionary<string, object>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                column.Add(reader.GetName(i), reader.GetValue(i));
            }

            columns.Add(column);
        }

        return columns;
    }
}
