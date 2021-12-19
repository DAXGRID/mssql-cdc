using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace MsSqlCdc;

internal static class CdcDatabase
{
    public static async Task<int> GetColumnOrdinal(
        SqlConnection connection,
        string captureInstance,
        string columnName)
    {
        var sql = "SELECT sys.fn_cdc_get_column_ordinal(@capture_instance, @column_name) AS column_ordinal";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@capture_instance", captureInstance);
        command.Parameters.AddWithValue("@column_name", columnName);

        using var reader = await command.ExecuteReaderAsync();

        var columnOrdinal = -1;
        while (await reader.ReadAsync())
        {
            columnOrdinal = (int)reader["column_ordinal"];
        }

        return columnOrdinal;
    }

    public static async Task<DateTime> MapLsnToTime(SqlConnection connection, long lsn)
    {
        var sql = "SELECT sys.fn_cdc_map_lsn_to_time(@lsn) AS lsn_time";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@lsn", lsn);

        using var reader = await command.ExecuteReaderAsync();

        var lsnTime = default(DateTime);
        while (await reader.ReadAsync())
        {
            lsnTime = (DateTime)reader["lsn_time"];
        }

        if (lsnTime == default(DateTime))
            throw new Exception($"Could not convert LSN to time with LSN being '{lsn}'");

        return lsnTime;
    }

    public static async Task<byte[]> MapTimeToLsn(
        SqlConnection connection,
        DateTime trackingTime,
        string relationOperator)
    {
        var sql = "SELECT sys.fn_cdc_map_time_to_lsn(@relational_operator, @tracking_time) AS lsn";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@relational_operator", relationOperator);
        command.Parameters.AddWithValue("@tracking_time", trackingTime);

        using var reader = await command.ExecuteReaderAsync();

        var lsn = new byte[10];
        while (await reader.ReadAsync())
        {
            lsn = (byte[])reader["lsn"];
        }

        return lsn;
    }

    public static async Task<byte[]> GetMinLsn(SqlConnection connection, string captureInstance)
    {
        var sql = "SELECT sys.fn_cdc_get_min_lsn(@capture_instance) AS min_lsn";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@capture_instance", captureInstance);

        using var reader = await command.ExecuteReaderAsync();

        var minLsn = new byte[10];
        while (await reader.ReadAsync())
        {
            minLsn = (byte[])reader["min_lsn"];
        }

        return minLsn;
    }

    public static async Task<byte[]> GetMaxLsn(SqlConnection connection)
    {
        var sql = "SELECT sys.fn_cdc_get_max_lsn() AS max_lsn";

        using var command = new SqlCommand(sql, connection);
        using SqlDataReader reader = await command.ExecuteReaderAsync();

        var maxLsn = new byte[10];
        while (await reader.ReadAsync())
        {
            maxLsn = (byte[])reader["max_lsn"];
        }

        return maxLsn;
    }

    public static async Task<byte[]> DecrementLsn(SqlConnection connection, long lsn)
    {
        var sql = "SELECT sys.fn_cdc_decrement_lsn(@lsn) AS previous_lsn";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@lsn", lsn);

        using var reader = await command.ExecuteReaderAsync();

        var nextLsn = new byte[10];
        while (await reader.ReadAsync())
        {
            nextLsn = (byte[])reader["previous_lsn"];
        }

        return nextLsn;
    }

    public static async Task<byte[]> IncrementLsn(SqlConnection connection, long lsn)
    {
        var sql = "SELECT sys.fn_cdc_increment_lsn(@lsn) AS next_lsn";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@lsn", lsn);

        using var reader = await command.ExecuteReaderAsync();

        var nextLsn = new byte[10];
        while (await reader.ReadAsync())
        {
            nextLsn = (byte[])reader["next_lsn"];
        }

        return nextLsn;
    }

    public static async Task<List<List<(string fieldName, object fieldValue)>>> GetAllChanges(
        SqlConnection connection,
        string captureInstance,
        long beginLsn,
        long endLsn)
    {
        return await GetChanges(connection, "cdc.fn_cdc_get_all_changes", captureInstance, beginLsn, endLsn);
    }

    public static async Task<List<List<(string fieldName, object fieldValue)>>> GetNetChanges(
        SqlConnection connection,
        string captureInstance,
        long beginLsn,
        long endLsn)
    {
        return await GetChanges(connection, "cdc.fn_cdc_get_net_changes", captureInstance, beginLsn, endLsn);
    }

    private static async Task<List<List<(string fieldName, object fieldValue)>>> GetChanges(
        SqlConnection connection,
        string cdcFunction,
        string captureInstance,
        long beginLsn,
        long endLsn)
    {
        var builder = new SqlCommandBuilder();
        // We have to do this here, since we cannot pass the function as command parameter.
        var function = builder.UnquoteIdentifier($"{cdcFunction}_{captureInstance}");

        var sql = $"SELECT * FROM {function}(@begin_lsn, @end_lsn, 'all update old')";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@begin_lsn", beginLsn);
        command.Parameters.AddWithValue("@end_lsn", endLsn);

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
