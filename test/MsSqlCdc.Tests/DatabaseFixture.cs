using System;
using System.IO;
using System.Threading;
using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;

namespace MsSqlCdc.Tests;

public class DatabaseFixture
{
    private const string MasterDatabaseName = "master";
    private const string TestDatabaseName = "mssql_cdc_test";
    public string ConnectionString => CreateConnectionString(TestDatabaseName);

    public DatabaseFixture()
    {
        DeleteDatabase();
        SetupDatabase();

        // We do this because the setup process is quite intensive for the SQL database.
        // So before it can be used in tests, we want to make sure that the CDC tables are ready to be consumed.
        Thread.Sleep(3000);
    }

    private void SetupDatabase()
    {
        using var connection = new SqlConnection(CreateConnectionString(MasterDatabaseName));
        connection.Open();
        var setupSql = File.ReadAllText(GetRootPath("Scripts/SetupDB.sql"));
        var server = new Server(new ServerConnection(connection));
        server.ConnectionContext.ExecuteNonQuery(setupSql);
    }

    private void DeleteDatabase()
    {
        var deleteDatabaseSql = $@"
            IF DB_ID('{TestDatabaseName}') IS NOT NULL
              BEGIN
                ALTER DATABASE {TestDatabaseName} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE {TestDatabaseName};
            END;";

        using var connection = new SqlConnection(CreateConnectionString(MasterDatabaseName));
        connection.Open();
        using var cmd = new SqlCommand(deleteDatabaseSql, connection);
        cmd.ExecuteNonQuery();
    }

    private static string GetRootPath(string filePath)
    {
        var absolutePath = Path.IsPathRooted(filePath)
            ? filePath
            : Path.GetRelativePath(Directory.GetCurrentDirectory(), filePath);

        if (!File.Exists(absolutePath))
            throw new ArgumentException($"Could not find file at path: {absolutePath}");

        return absolutePath;
    }

    private static string CreateConnectionString(string initialCatalog)
    {
        var builder = new SqlConnectionStringBuilder();
        builder.DataSource = "localhost";
        builder.UserID = "sa";
        builder.Password = "myAwesomePassword1";
        builder.InitialCatalog = initialCatalog;
        builder.Encrypt = false;
        return builder.ConnectionString;
    }
}
