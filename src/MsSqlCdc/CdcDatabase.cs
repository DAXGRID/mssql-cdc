using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using System;

namespace MsSqlCdc;

public static class CdcDatabase
{
    public static async Task<byte[]> GetMinLsn(SqlConnection connection, string captureInstance)
    {
        var sql = "SELECT sys.fn_cdc_get_min_lsn(@capture_instance) AS min_lsn";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@capture_instance", captureInstance);

        using SqlDataReader reader = await command.ExecuteReaderAsync();

        var minLsn = new byte[8];
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

        var maxLsn = new byte[8];
        while (await reader.ReadAsync())
        {
            maxLsn = (byte[])reader["max_lsn"];
        }

        return maxLsn;
    }

    public static async Task<byte[]> GetNextLsn(SqlConnection connection, long lsn)
    {
        var sql = "SELECT sys.fn_cdc_increment_lsn(@lsn) AS next_lsn";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@lsn", lsn);

        using SqlDataReader reader = await command.ExecuteReaderAsync();

        var nextLsn = new byte[8];
        while (await reader.ReadAsync())
        {
            nextLsn = (byte[])reader["next_lsn"];
        }

        return nextLsn;
    }

    public static async Task<List<List<Tuple<string, object>>>> GetAllChanges(
        SqlConnection connection,
        string tableName,
        long beginLsn,
        long endLsn)
    {
        return await GetChanges(connection, "cdc.fn_cdc_get_all_changes", tableName, beginLsn, endLsn);
    }

    public static async Task<List<List<Tuple<string, object>>>> GetNetChanges(
        SqlConnection connection,
        string tableName,
        long beginLsn,
        long endLsn)
    {
        return await GetChanges(connection, "cdc.fn_cdc_get_net_changes", tableName, beginLsn, endLsn);
    }

    private static async Task<List<List<Tuple<string, object>>>> GetChanges(
        SqlConnection connection,
        string cdcFunction,
        string tableName,
        long beginLsn,
        long endLsn)
    {
        var builder = new SqlCommandBuilder();
        // We have to do this here, since we cannot pass the function as command parameter.
        var function = builder.UnquoteIdentifier($"{cdcFunction}_{tableName}");

        var sql = $"SELECT * FROM {function}(@begin_lsn, @end_lsn, 'all update old')";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@begin_lsn", beginLsn);
        command.Parameters.AddWithValue("@end_lsn", endLsn);

        var changes = new List<List<Tuple<string, object>>>();
        using SqlDataReader reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var column = new List<Tuple<string, object>>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                column.Add(new Tuple<string, object>(reader.GetName(i), reader.GetValue(i)));
            }

            changes.Add(column);
        }

        return changes;
    }
}
