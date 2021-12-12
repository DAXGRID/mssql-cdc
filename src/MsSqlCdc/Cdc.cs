using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace MsSqlCdc;

public static class Cdc
{
    public static async Task<long> GetNextLsn(SqlConnection connection, long lsn)
    {
        var sql = "SELECT sys.fn_cdc_increment_lsn(@lsn) AS next_lsn";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@lsn", lsn);

        using SqlDataReader reader = await command.ExecuteReaderAsync();

        long minLsn = 0;
        while (await reader.ReadAsync())
        {
            minLsn = DataConvert.ConvertBinaryLsn((byte[])reader["next_lsn"]);
        }

        return minLsn;
    }

    public static async Task<long> GetMinLsn(SqlConnection connection, string captureInstanceName)
    {
        var sql = "SELECT sys.fn_cdc_get_min_lsn(@capture_instance) AS min_lsn";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@capture_instance", captureInstanceName);

        using SqlDataReader reader = await command.ExecuteReaderAsync();

        long minLsn = 0;
        while (await reader.ReadAsync())
        {
            minLsn = DataConvert.ConvertBinaryLsn((byte[])reader["min_lsn"]);
        }

        return minLsn;
    }

    public static async Task<long> GetMaxLsn(SqlConnection connection)
    {
        var sql = "SELECT sys.fn_cdc_get_max_lsn() AS max_lsn";

        using var command = new SqlCommand(sql, connection);
        using SqlDataReader reader = await command.ExecuteReaderAsync();

        long maxLsn = 0;
        while (await reader.ReadAsync())
        {
            maxLsn = DataConvert.ConvertBinaryLsn((byte[])reader["max_lsn"]);
        }

        return maxLsn;
    }


    public static async Task<IReadOnlyCollection<ChangeData<dynamic>>> GetAllChanges(
        SqlConnection connection,
        string tableName,
        long beginLsn,
        long endLsn)
    {
        var builder = new SqlCommandBuilder();
        // We have to do this here, since we cannot pass the function as command parameter.
        var cdcFunction = builder.UnquoteIdentifier($"cdc.fn_cdc_get_all_changes_{tableName}");
        var sql = $"SELECT * FROM {cdcFunction}(@begin_lsn, @end_lsn, 'all update old')";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@begin_lsn", beginLsn);
        command.Parameters.AddWithValue("@end_lsn", endLsn);

        return await GetChanges(command, tableName);
    }

    public static async Task<IReadOnlyCollection<ChangeData<dynamic>>> GetNetChanges(
        SqlConnection connection,
        string tableName,
        long beginLsn,
        long endLsn)
    {
        var builder = new SqlCommandBuilder();
        // We have to do this here, since we cannot pass the function as command parameter.
        var cdcFunction = builder.UnquoteIdentifier($"cdc.fn_cdc_get_net_changes_{tableName}");
        var sql = $"SELECT * FROM {cdcFunction}(@begin_lsn, @end_lsn, 'all update old')";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@begin_lsn", beginLsn);
        command.Parameters.AddWithValue("@end_lsn", endLsn);

        return await GetChanges(command, tableName);
    }

    private static async Task<IReadOnlyCollection<ChangeData<dynamic>>> GetChanges(SqlCommand command, string tableName)
    {
        var changes = new List<ChangeData<dynamic>>();
        using SqlDataReader reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var startLsn = DataConvert.ConvertBinaryLsn((byte[])reader["__$start_lsn"]);
            var seqVal = DataConvert.ConvertBinaryLsn((byte[])reader["__$seqval"]);
            var operation = DataConvert.ConvertIntOperation((int)reader["__$operation"]);
            var updateMask = Encoding.UTF8.GetString((byte[])reader["__$update_mask"]);

            // We use dynamic object here because in this configuration
            // the columns are not mapped to concrete types.
            var body = new ExpandoObject() as IDictionary<string, Object>;

            // We start from 4 since we already mapped the 4 first values and the rest of the values are dynamic.
            for (var i = 4; i < reader.FieldCount; i++)
            {
                body[reader.GetName(i)] = reader.GetValue(i);
            }

            changes.Add(new(startLsn, seqVal, operation, updateMask, tableName, body));
        }

        return changes;
    }
}
