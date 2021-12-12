using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using MsSqlCdc;

namespace Example;

public class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Starting to listen");

        SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
        builder.DataSource = "localhost";
        builder.UserID = "sa";
        builder.Password = "myAwesomePassword1";
        builder.InitialCatalog = "TestDb";
        builder.Encrypt = false;

        var connectionString = builder.ConnectionString;
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var minLsn = await Cdc.GetMinLsn(connection, "dbo_Employee");
        var maxLsn = await Cdc.GetMaxLsn(connection);

        var options = new JsonSerializerOptions
        {
            WriteIndented = true,
            Converters =
            {
                new JsonStringEnumConverter()
            }
        };

        var changeSets = await Cdc.GetAllChanges(connection, "dbo_Employee", minLsn, maxLsn);

        var result = JsonSerializer.Serialize(changeSets, options);
        Console.WriteLine(result);
    }
}
