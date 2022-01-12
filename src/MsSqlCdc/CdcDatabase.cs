using System;
using System.Collections.Generic;
using System.Numerics;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace MsSqlCdc;

internal static class CdcDatabase
{
    public static async Task<bool?> IsBitSet(SqlConnection connection, int position, string updateMask)
    {
        var sql = "sys.fn_cdc_is_bit_set(@position, @update_mask )";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@position", position);
        command.Parameters.AddWithValue("@updateMask", updateMask);

        return (bool?)(await command.ExecuteScalarAsync());
    }

    public static async Task<bool?> HasColumnChanged(
        SqlConnection connection,
        string captureInstance,
        string columnName,
        string updateMask)
    {
        var sql = "sys.fn_cdc_has_column_changed(@capture_instance, @column_name, @update_mask)";
        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@capture_instance", captureInstance);
        command.Parameters.AddWithValue("@column_name", columnName);
        command.Parameters.AddWithValue("@update_mask", updateMask);

        return (bool?)(await command.ExecuteScalarAsync());
    }

    public static async Task<int?> GetColumnOrdinal(
        SqlConnection connection,
        string captureInstance,
        string columnName)
    {
        var sql = "SELECT sys.fn_cdc_get_column_ordinal(@capture_instance, @column_name)";
        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@capture_instance", captureInstance);
        command.Parameters.AddWithValue("@column_name", columnName);

        return (int?)(await command.ExecuteScalarAsync());
    }

    public static async Task<DateTime?> MapLsnToTime(SqlConnection connection, long lsn)
    {
        var sql = "SELECT sys.fn_cdc_map_lsn_to_time(@lsn)";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@lsn", lsn);

        return (DateTime?)(await command.ExecuteScalarAsync());
    }

    public static async Task<byte[]?> MapTimeToLsn(
        SqlConnection connection,
        DateTime trackingTime,
        string relationOperator)
    {
        var sql = "SELECT sys.fn_cdc_map_time_to_lsn(@relational_operator, @tracking_time)";
        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@relational_operator", relationOperator);
        command.Parameters.AddWithValue("@tracking_time", trackingTime);

        return (byte[]?)(await command.ExecuteScalarAsync());
    }

    public static async Task<byte[]?> GetMinLsn(SqlConnection connection, string captureInstance)
    {
        var sql = "SELECT sys.fn_cdc_get_min_lsn(@capture_instance)";
        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@capture_instance", captureInstance);

        return (byte[]?)(await command.ExecuteScalarAsync());
    }

    public static async Task<byte[]?> GetMaxLsn(SqlConnection connection)
    {
        var sql = "SELECT sys.fn_cdc_get_max_lsn()";
        using var command = new SqlCommand(sql, connection);

        return (byte[]?)(await command.ExecuteScalarAsync());
    }

    public static async Task<byte[]?> DecrementLsn(SqlConnection connection, long lsn)
    {
        var sql = "SELECT sys.fn_cdc_decrement_lsn(@lsn)";
        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@lsn", lsn);

        return (byte[]?)(await command.ExecuteScalarAsync());
    }

    public static async Task<byte[]?> IncrementLsn(SqlConnection connection, long lsn)
    {
        var sql = "SELECT sys.fn_cdc_increment_lsn(@lsn)";
        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@lsn", lsn);

        return (byte[]?)(await command.ExecuteScalarAsync());
    }

    public static async Task<List<List<(string fieldName, object fieldValue)>>> GetAllChanges(
        SqlConnection connection,
        string captureInstance,
        BigInteger beginLsn,
        BigInteger endLsn,
        string filterOption)
    {
        return await GetChanges(
            connection,
            "cdc.fn_cdc_get_all_changes",
            captureInstance,
            beginLsn,
            endLsn,
            filterOption);
    }

    public static async Task<List<List<(string fieldName, object fieldValue)>>> GetNetChanges(
        SqlConnection connection,
        string captureInstance,
        BigInteger beginLsn,
        BigInteger endLsn,
        string filterOption)
    {
        return await GetChanges(
            connection,
            "cdc.fn_cdc_get_net_changes",
            captureInstance,
            beginLsn,
            endLsn,
            filterOption);
    }

    private static async Task<List<List<(string fieldName, object fieldValue)>>> GetChanges(
        SqlConnection connection,
        string cdcFunction,
        string captureInstance,
        BigInteger beginLsn,
        BigInteger endLsn,
        string filterOption)
    {
        var sql = $"SELECT * FROM {cdcFunction}_{captureInstance}(@begin_lsn, @end_lsn, @filter_option)";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@begin_lsn", beginLsn);
        command.Parameters.AddWithValue("@end_lsn", endLsn);
        command.Parameters.AddWithValue("@filter_option", filterOption);

        var changes = new List<List<(string name, object value)>>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var column = new List<(string fieldName, object fieldValue)>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                column.Add((reader.GetName(i), reader.GetValue(i)));
            }

            changes.Add(column);
        }

        return changes;
    }
}
