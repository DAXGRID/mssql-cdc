using System;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Data.SqlClient;
using Xunit;

namespace MsSqlCdc.Tests;

public class CdcTests : IClassFixture<DatabaseFixture>
{
    private readonly DatabaseFixture _databaseFixture;

    public CdcTests(DatabaseFixture databaseFixture)
    {
        _databaseFixture = databaseFixture;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Get_min_lsn()
    {
        using var connection = await CreateOpenSqlConnection();
        var captureInstance = "dbo_Employee";

        // We insert an Employee to be able to the min LSN for capture instance.
        await InsertEmployee(Guid.NewGuid(), "Rune", "Nielsen");

        var minLsn = await Cdc.GetMinLsn(connection, captureInstance);

        minLsn.Should().BeGreaterThan(0);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Get_max_lsn()
    {
        using var connection = await CreateOpenSqlConnection();

        var maxLsn = await Cdc.GetMaxLsn(connection);

        maxLsn.Should().BeGreaterThan(0);
    }

    private async Task<SqlConnection> CreateOpenSqlConnection()
    {
        var connection = new SqlConnection(_databaseFixture.ConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    private async Task InsertEmployee(Guid id, string firstName, string lastName)
    {
        using var connection = await CreateOpenSqlConnection();
        var sql = @"
            INSERT INTO [dbo].[employee] ([id], [first_name], [last_name])
            VALUES(@id, @first_name, @last_name)";

        using var cmd = new SqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@first_name", firstName);
        cmd.Parameters.AddWithValue("@last_name", lastName);

        await cmd.ExecuteNonQueryAsync();
    }
}
