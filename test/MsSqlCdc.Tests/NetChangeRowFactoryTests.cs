using FluentAssertions;
using System;
using static FluentAssertions.FluentActions;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace MsSqlCdc.Tests;

public class NetChangeRowFactoryTests
{
    public static IEnumerable<object[]> NetChangesFieldData()
    {
        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"__$start_lsn", BitConverter.GetBytes(25000).Reverse().ToArray()},
                {"__$operation", (int)NetChangeOperation.Update},
                {"__$update_mask", new byte[128]},
                {"Id", 10},
                {"Name", "Rune"},
                {"Salary", 20000.00},
            },
            "dbo_Employee",
            new NetChangeRow(
                25000,
                NetChangeOperation.Update,
                new byte[128],
                "dbo_Employee",
                new Dictionary<string, object> {
                    {"Id", 10},
                    {"Name",  "Rune"},
                    {"Salary",  20000.00}
                })
        };

        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"__$start_lsn", BitConverter.GetBytes(25000).Reverse().ToArray()},
                {"__$operation", (int)NetChangeOperation.Insert},
                {"__$update_mask", new byte[128]},
                {"Id", 1},
                {"Name", "Simon"},
            },
            "dbo_Employee",
             new NetChangeRow(
                25000,
                NetChangeOperation.Insert,
                new byte[128],
                "dbo_Employee",
                new Dictionary<string, object> {
                    {"Id", 1},
                    {"Name", "Simon"},
                })
        };

        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"__$start_lsn", BitConverter.GetBytes(25000).Reverse().ToArray()},
                {"__$operation", (int)NetChangeOperation.Delete},
                {"__$update_mask", new byte[128]},
                {"Id", 0},
                {"Name", "Jesper"},
            },
            "dbo_Employee",
             new NetChangeRow(
                25000,
                NetChangeOperation.Delete,
                new byte[128],
                "dbo_Employee",
                new Dictionary<string, object>{
                    {"Id",  0},
                    {"Name", "Jesper"},
                })
        };

        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"__$start_lsn", BitConverter.GetBytes(25000).Reverse().ToArray()},
                {"__$operation", (int)NetChangeOperation.Insert},
                {"__$update_mask", new byte[128]},
                {"Id", 10},
            },
            "dbo_Animal",
             new NetChangeRow(
                25000,
                NetChangeOperation.Insert,
                new byte[128],
                "dbo_Animal",
                new Dictionary<string, object>{
                    {"Id", 10},
                })
        };

        yield return new object[]
        {
             new Dictionary<string, object>
            {
                {"__$start_lsn", BitConverter.GetBytes(25000).Reverse().ToArray()},
                {"__$operation", (int)NetChangeOperation.InsertOrUpdate},
                {"__$update_mask", new byte[128]},
            },
            "dbo_Animal",
             new NetChangeRow(
                25000,
                NetChangeOperation.InsertOrUpdate,
                new byte[128],
                "dbo_Animal",
                new Dictionary<string, object>{
                })
        };

        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"__$start_lsn", BitConverter.GetBytes(25000).Reverse().ToArray()},
                {"__$update_mask", new byte[128]},
                {"__$operation", (int)NetChangeOperation.Insert},
            },
            "dbo_Animal",
             new NetChangeRow(
                25000,
                NetChangeOperation.Insert,
                new byte[128],
                "dbo_Animal",
                new Dictionary<string, object>{
                })
        };

        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"__$update_mask", new byte[128]},
                {"__$start_lsn", BitConverter.GetBytes(25000).Reverse().ToArray()},
                {"__$operation", (int)NetChangeOperation.InsertOrUpdate},
            },
            "dbo_Animal",
             new NetChangeRow(
                25000,
                NetChangeOperation.InsertOrUpdate,
                new byte[128],
                "dbo_Animal",
                new Dictionary<string, object>{
                })
        };

        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"Id", 0},
                {"__$operation", (int)NetChangeOperation.Delete},
                {"Name", "Jesper"},
                {"__$update_mask", new byte[128]},
                {"__$start_lsn", BitConverter.GetBytes(25000).Reverse().ToArray()},
            },
            "dbo_Employee",
             new NetChangeRow(
                25000,
                NetChangeOperation.Delete,
                new byte[128],
                "dbo_Employee",
                new Dictionary<string, object>{
                    {"Id", 0},
                    {"Name", "Jesper"},
                })
        };

        // The important part with this one is that update_mask can be DBNull.Value
        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"__$operation", (int)NetChangeOperation.Delete},
                {"__$update_mask", DBNull.Value},
                {"__$start_lsn", BitConverter.GetBytes(25000).Reverse().ToArray()},
                {"Id", 0},
                {"Name", "Jesper"},
            },
            "dbo_Employee",
             new NetChangeRow(
                25000,
                NetChangeOperation.Delete,
                null,
                "dbo_Employee",
                new Dictionary<string, object>{
                    {"Id", 0},
                    {"Name", "Jesper"},
                })
        };
    }

    public static IEnumerable<object[]> InvalidNetChangesFieldData()
    {
        yield return new object[]
        {
            new Dictionary<string, object>(),
            "dbo_Employee",
        };

        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()},
            },
            "dbo_Employee",
        };

        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"__$update_mask", new byte[128]},
                {"__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()},
            },
            "dbo_Employee",
        };

        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"__$update_mask", new byte[128]},
                {"__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()},
                {"__$start_lsn", BitConverter.GetBytes(25000).Reverse().ToArray()},
            },
            "dbo_Employee",
        };

        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"__$update_mask", new byte[128]},
                {"__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()},
                {"__$start_lsn", BitConverter.GetBytes(25000).Reverse().ToArray()},
                {"Id", 0},
                {"Name", "Rune"}
            },
            "dbo_Employee",
        };

        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"Address", "Streetvalley 20"},
                {"Salary", 2000.00},
                {"Id", 0},
                {"Name", "Rune"}
            },
            "dbo_Employee",
        };
    }

    [Theory]
    [Trait("Category", "Unit")]
    [MemberData(nameof(NetChangesFieldData))]
    public void Conversion_cdc_column_to_change_row(
        Dictionary<string, object> columnFields,
        string captureInstance,
        NetChangeRow expected)
    {
        var result = NetChangeRowFactory.Create(columnFields, captureInstance);
        result.Should().BeEquivalentTo(expected);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [MemberData(nameof(InvalidNetChangesFieldData))]
    public void Conversion_cdc_column_without_default_fields_is_invalid(
        Dictionary<string, object> columnFields,
        string captureInstance)
    {
        Invoking(() => NetChangeRowFactory.Create(columnFields, captureInstance))
            .Should()
            .Throw<ArgumentException>()
            .WithMessage($"The column fields does not contain all the default CDC column fields.");
    }
}
