using System;
using System.Numerics;
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

        minLsn.Should().NotBe(default(BigInteger));
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Get_max_lsn()
    {
        using var connection = await CreateOpenSqlConnection();

        var maxLsn = await Cdc.GetMaxLsn(connection);

        maxLsn.Should().NotBe(default(BigInteger));
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Get_previous_lsn()
    {
        using var connection = await CreateOpenSqlConnection();
        // We use the max LSN to get an realistic LSN number for testing.
        var maxLsn = await Cdc.GetMaxLsn(connection);

        var previousLsn = await Cdc.GetPreviousLsn(connection, maxLsn);

        previousLsn.Should()
            .BeLessThan(maxLsn).And
            .NotBe(default(BigInteger));
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Get_next_lsn()
    {
        using var connection = await CreateOpenSqlConnection();
        // We use the max LSN to get an realistic LSN number for testing.
        var maxLsn = await Cdc.GetMaxLsn(connection);

        var previousLsn = await Cdc.GetNextLsn(connection, maxLsn);

        previousLsn.Should()
            .BeGreaterThan(maxLsn).And
            .BeGreaterThan(default(BigInteger));
    }

    [Theory]
    [InlineData(RelationalOperator.LargestLessThan, 0)]
    [InlineData(RelationalOperator.LargestLessThanOrEqual, 0)]
    [InlineData(RelationalOperator.SmallestGreaterThan, -100)]
    [InlineData(RelationalOperator.SmallestGreaterThanOrEqual, -100)]
    [Trait("Category", "Integration")]
    public async Task Map_time_to_lsn(RelationalOperator relationalOperator, int secondsFromNow)
    {
        using var connection = await CreateOpenSqlConnection();
        var now = DateTime.UtcNow.AddSeconds(secondsFromNow);

        var lsn = await Cdc.MapTimeToLsn(connection, now, relationalOperator);

        lsn.Should().NotBe(default(BigInteger));
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Map_lsn_to_time()
    {
        using var connection = await CreateOpenSqlConnection();
        // We use the max LSN to get an realistic LSN number for testing.
        var maxLsn = await Cdc.GetMaxLsn(connection);

        var time = await Cdc.MapLsnToTime(connection, maxLsn);

        time.ToUniversalTime().Should()
            .NotBe(default(DateTime)).And
            .BeBefore(DateTime.UtcNow);
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
