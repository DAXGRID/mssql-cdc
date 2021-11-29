using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace MsSqlCdc;

public class CdcListener
{
    private readonly string _connectionString;

    public CdcListener()
    {
        SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
        builder.DataSource = "localhost";
        builder.UserID = "sa";
        builder.Password = "";
        builder.InitialCatalog = "TestDb";
        builder.Encrypt = false;
        _connectionString = builder.ConnectionString;
    }

    public async Task Start()
    {
        var minLsn = await GetMinLsn("dbo_Employee");
        var maxLsn = await GetMaxLsn();

        var nextMinLsn = await GetNextLsn(minLsn);

        System.Console.WriteLine($"Getting from min_lsn {nextMinLsn} to max_lsn {maxLsn}.");

        await GetlatestChangeSet(nextMinLsn, maxLsn);
    }

    private async Task<long> GetNextLsn(long lsn)
    {
        using var connection = new SqlConnection(_connectionString);
        var sql = "SELECT sys.fn_cdc_increment_lsn(@lsn) AS next_lsn";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@lsn", lsn);

        await connection.OpenAsync();
        using SqlDataReader reader = await command.ExecuteReaderAsync();

        long minLsn = 0;
        while (await reader.ReadAsync())
        {
            minLsn = DataConvert.ConvertBinaryLsnBigInt((byte[])reader["next_lsn"]);
        }

        return minLsn;
    }

    private async Task<long> GetMinLsn(string captureInstanceName)
    {
        using var connection = new SqlConnection(_connectionString);
        var sql = "SELECT sys.fn_cdc_get_min_lsn(@capture_instance) AS min_lsn";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@capture_instance", captureInstanceName);

        await connection.OpenAsync();
        using SqlDataReader reader = await command.ExecuteReaderAsync();

        long minLsn = 0;
        while (await reader.ReadAsync())
        {
            minLsn = DataConvert.ConvertBinaryLsnBigInt((byte[])reader["min_lsn"]);
        }

        return minLsn;
    }

    private async Task<long> GetMaxLsn()
    {
        using var connection = new SqlConnection(_connectionString);
        var sql = "SELECT sys.fn_cdc_get_max_lsn() AS max_lsn";

        using var command = new SqlCommand(sql, connection);

        await connection.OpenAsync();
        using SqlDataReader reader = await command.ExecuteReaderAsync();

        long maxLsn = 0;
        while (await reader.ReadAsync())
        {
            maxLsn = DataConvert.ConvertBinaryLsnBigInt((byte[])reader["max_lsn"]);
        }

        return maxLsn;
    }

    private async Task GetlatestChangeSet(long beginLsn, long endLsn)
    {
        using var connection = new SqlConnection(_connectionString);
        var sql = "SELECT * FROM cdc.fn_cdc_get_all_changes_dbo_Employee(@begin_lsn, @end_lsn, 'all update old')";

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@begin_lsn", beginLsn);
        command.Parameters.AddWithValue("@end_lsn", endLsn);

        await connection.OpenAsync();

        using SqlDataReader reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var startLsn = DataConvert.ConvertBinaryLsnBigInt((byte[])reader["__$start_lsn"]);
            var seqVal = (byte[])reader["__$seqval"];
            var operation = (int)reader["__$operation"];
            var updateMask = (byte[])reader["__$update_mask"];

            System.Console.WriteLine($"StartLsn {startLsn}");
        }
    }
}
