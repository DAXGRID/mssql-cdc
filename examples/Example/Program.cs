using System;
using System.Collections.Generic;
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
        builder.Password = "";
        builder.InitialCatalog = "TestDb";
        builder.Encrypt = false;
        var connectionString = builder.ConnectionString;

        // var config = new Config(connectionString, new List<string> { "dbo_Employee" });
        // await CdcListener.Start(config);

        // var options = new JsonSerializerOptions
        // {
        //     WriteIndented = true,
        //     Converters =
        //     {
        //         new JsonStringEnumConverter()
        //     }
        // };

        // var result = JsonSerializer.Serialize(changeSets, options);
        // Console.WriteLine(result);
    }
}
