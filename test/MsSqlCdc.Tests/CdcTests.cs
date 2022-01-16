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
        var captureInstance = "dbo_Employee";
        using var connection = await CreateOpenSqlConnection();

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

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Get_all_changes_rowfilter_all()
    {
        var captureInstance = "dbo_Employee";
        using var connection = await CreateOpenSqlConnection();

        var minLsn = await Cdc.GetMinLsn(connection, captureInstance);
        var maxLsn = await Cdc.GetMaxLsn(connection);

        var allChanges = await Cdc.GetAllChanges(
            connection,
            captureInstance,
            minLsn,
            maxLsn,
            AllChangesRowFilterOption.All);

        allChanges
            .Should()
            .HaveCount(2).And
            .SatisfyRespectively(
                insert =>
                {
                    insert.CaptureInstance.Should().Be(captureInstance);
                    insert.StartLineSequenceNumber.Should().BeGreaterThan(default(BigInteger));
                    insert.SequenceValue.Should().BeGreaterThan(default(BigInteger));
                    insert.Operation.Should().Be(AllChangeOperation.Insert);
                    insert.Fields["id"].Should().NotBeNull();
                    insert.Fields["first_name"].Should().Be("Rune");
                    insert.Fields["last_name"].Should().Be("Nielsen");
                },
                afterUpdate =>
                {
                    afterUpdate.CaptureInstance.Should().Be(captureInstance);
                    afterUpdate.StartLineSequenceNumber.Should().BeGreaterThan(default(BigInteger));
                    afterUpdate.SequenceValue.Should().BeGreaterThan(default(BigInteger));
                    afterUpdate.Operation.Should().Be(AllChangeOperation.AfterUpdate);
                    afterUpdate.Fields["id"].Should().NotBeNull();
                    afterUpdate.Fields["first_name"].Should().Be("Rune");
                    afterUpdate.Fields["last_name"].Should().Be("Jensen");
                });
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Get_all_changes_rowfilter_all_update_old()
    {
        var captureInstance = "dbo_Employee";
        using var connection = await CreateOpenSqlConnection();

        var minLsn = await Cdc.GetMinLsn(connection, captureInstance);
        var maxLsn = await Cdc.GetMaxLsn(connection);

        var allChanges = await Cdc.GetAllChanges(
            connection,
            captureInstance,
            minLsn,
            maxLsn,
            AllChangesRowFilterOption.AllUpdateOld);

        allChanges
            .Should()
            .HaveCount(3).And
            .SatisfyRespectively(
                insert =>
                {
                    insert.CaptureInstance.Should().Be(captureInstance);
                    insert.StartLineSequenceNumber.Should().BeGreaterThan(default(BigInteger));
                    insert.SequenceValue.Should().BeGreaterThan(default(BigInteger));
                    insert.Operation.Should().Be(AllChangeOperation.Insert);
                    insert.Fields["id"].Should().NotBeNull();
                    insert.Fields["first_name"].Should().Be("Rune");
                    insert.Fields["last_name"].Should().Be("Nielsen");
                },
                beforeUpdate =>
                {
                    beforeUpdate.CaptureInstance.Should().Be(captureInstance);
                    beforeUpdate.StartLineSequenceNumber.Should().BeGreaterThan(default(BigInteger));
                    beforeUpdate.SequenceValue.Should().BeGreaterThan(default(BigInteger));
                    beforeUpdate.Operation.Should().Be(AllChangeOperation.BeforeUpdate);
                    beforeUpdate.Fields["id"].Should().NotBeNull();
                    beforeUpdate.Fields["first_name"].Should().Be("Rune");
                    beforeUpdate.Fields["last_name"].Should().Be("Nielsen");
                },
                afterUpdate =>
                {
                    afterUpdate.CaptureInstance.Should().Be(captureInstance);
                    afterUpdate.StartLineSequenceNumber.Should().BeGreaterThan(default(BigInteger));
                    afterUpdate.SequenceValue.Should().BeGreaterThan(default(BigInteger));
                    afterUpdate.Operation.Should().Be(AllChangeOperation.AfterUpdate);
                    afterUpdate.Fields["id"].Should().NotBeNull();
                    afterUpdate.Fields["first_name"].Should().Be("Rune");
                    afterUpdate.Fields["last_name"].Should().Be("Jensen");
                });
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Get_net_changes_all()
    {
        var captureInstance = "dbo_Employee";
        using var connection = await CreateOpenSqlConnection();
        var minLsn = await Cdc.GetMinLsn(connection, captureInstance);
        var maxLsn = await Cdc.GetMaxLsn(connection);

        var netChanges = await Cdc.GetNetChanges(
            connection,
            "dbo_Employee",
            minLsn,
            maxLsn,
            NetChangesRowFilterOption.All);

        netChanges
            .Should()
            .HaveCount(1).And
            .SatisfyRespectively(
                netChange =>
                {
                    netChange.CaptureInstance.Should().Be(captureInstance);
                    netChange.StartLineSequenceNumber.Should().BeGreaterThan(default(BigInteger));
                    netChange.UpdateMask.Should().BeNull();
                    netChange.Operation.Should().Be(NetChangeOperation.Insert);
                    netChange.Fields["first_name"].Should().Be("Rune");
                    netChange.Fields["last_name"].Should().Be("Jensen");
                });
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Get_net_changes_all_with_mask()
    {
        var captureInstance = "dbo_Employee";
        using var connection = await CreateOpenSqlConnection();
        var minLsn = await Cdc.GetMinLsn(connection, captureInstance);
        var maxLsn = await Cdc.GetMaxLsn(connection);

        var netChanges = await Cdc.GetNetChanges(
            connection,
            "dbo_Employee",
            minLsn,
            maxLsn,
            NetChangesRowFilterOption.AllWithMask);

        netChanges
            .Should()
            .HaveCount(1).And
            .SatisfyRespectively(
                netChange =>
                {
                    netChange.CaptureInstance.Should().Be(captureInstance);
                    netChange.StartLineSequenceNumber.Should().BeGreaterThan(default(BigInteger));
                    netChange.UpdateMask.Should().NotBeEmpty();
                    netChange.Operation.Should().Be(NetChangeOperation.Insert);
                    netChange.Fields["first_name"].Should().Be("Rune");
                    netChange.Fields["last_name"].Should().Be("Jensen");
                });
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Get_net_changes_all_with_merge()
    {
        var captureInstance = "dbo_Employee";
        using var connection = await CreateOpenSqlConnection();
        var minLsn = await Cdc.GetMinLsn(connection, captureInstance);
        var maxLsn = await Cdc.GetMaxLsn(connection);

        var netChanges = await Cdc.GetNetChanges(
            connection,
            "dbo_Employee",
            minLsn,
            maxLsn,
            NetChangesRowFilterOption.AllWithMerge);

        netChanges
            .Should()
            .HaveCount(1).And
            .SatisfyRespectively(
                netChange =>
                {
                    netChange.CaptureInstance.Should().Be(captureInstance);
                    netChange.StartLineSequenceNumber.Should().BeGreaterThan(default(BigInteger));
                    netChange.UpdateMask.Should().NotBeEmpty();
                    netChange.Operation.Should().Be(NetChangeOperation.InsertOrUpdate);
                    netChange.Fields["first_name"].Should().Be("Rune");
                    netChange.Fields["last_name"].Should().Be("Jensen");
                });
    }

    private async Task<SqlConnection> CreateOpenSqlConnection()
    {
        var connection = new SqlConnection(_databaseFixture.ConnectionString);
        await connection.OpenAsync();
        return connection;
    }
}
