using FluentAssertions;
using System;
using static FluentAssertions.FluentActions;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace MsSqlCdc.Tests;

public class AllChangeRowFactoryTests
{
    public static IEnumerable<object[]> AllChangesFieldsData()
    {
        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"__$start_lsn", BitConverter.GetBytes(25000L).Reverse().ToArray()},
                {"__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()},
                {"__$operation", 4},
                {"__$update_mask", new byte[128]},
                {"Id", 10},
                {"Name", "Rune"},
                {"Salary", 20000.00},
            },
            "dbo_Employee",
            new AllChangeRow(
                25000L,
                25002L,
                AllChangeOperation.AfterUpdate,
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
                {"__$start_lsn", BitConverter.GetBytes(25000L).Reverse().ToArray()},
                {"__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()},
                {"__$operation", 3},
                {"__$update_mask", new byte[128]},
                {"Id", 1},
                {"Name", "Simon"},
            },
            "dbo_Employee",
             new AllChangeRow(
                25000L,
                25002L,
                AllChangeOperation.BeforeUpdate,
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
                {"__$start_lsn", BitConverter.GetBytes(25000L).Reverse().ToArray()},
                {"__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()},
                {"__$operation", 1},
                {"__$update_mask", new byte[128]},
                {"Id", 0},
                {"Name", "Jesper"},
            },
            "dbo_Employee",
             new AllChangeRow(
                25000L,
                25002L,
                AllChangeOperation.Delete,
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
                {"__$start_lsn", BitConverter.GetBytes(25000L).Reverse().ToArray()},
                {"__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()},
                {"__$operation", 2},
                {"__$update_mask", new byte[128]},
                {"Id", 10},
            },
            "dbo_Animal",
             new AllChangeRow(
                25000L,
                25002L,
                AllChangeOperation.Insert,
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
                {"__$start_lsn", BitConverter.GetBytes(25000L).Reverse().ToArray()},
                {"__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()},
                {"__$operation", 2},
                {"__$update_mask", new byte[128]},
            },
            "dbo_Animal",
             new AllChangeRow(
                25000L,
                25002L,
                AllChangeOperation.Insert,
                new byte[128],
                "dbo_Animal",
                new Dictionary<string, object>{
                })
        };

        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"__$start_lsn", BitConverter.GetBytes(25000L).Reverse().ToArray()},
                {"__$update_mask", new byte[128]},
                {"__$operation", 2},
                {"__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()},
            },
            "dbo_Animal",
             new AllChangeRow(
                25000L,
                25002L,
                AllChangeOperation.Insert,
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
                {"__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()},
                {"__$start_lsn", BitConverter.GetBytes(25000L).Reverse().ToArray()},
                {"__$operation", 2},
            },
            "dbo_Animal",
             new AllChangeRow(
                25000L,
                25002L,
                AllChangeOperation.Insert,
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
                {"__$operation", 1},
                {"Name", "Jesper"},
                {"__$update_mask", new byte[128]},
                {"__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()},
                {"__$start_lsn", BitConverter.GetBytes(25000L).Reverse().ToArray()},
            },
            "dbo_Employee",
             new AllChangeRow(
                25000L,
                25002L,
                AllChangeOperation.Delete,
                new byte[128],
                "dbo_Employee",
                new Dictionary<string, object>{
                    {"Id", 0},
                    {"Name", "Jesper"},
                })
        };
    }

    public static IEnumerable<object[]> CdcDefaultFieldsInvalidData()
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
                {"__$start_lsn", BitConverter.GetBytes(25000L).Reverse().ToArray()},
            },
            "dbo_Employee",
        };

        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"__$update_mask", new byte[128]},
                {"__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()},
                {"__$start_lsn", BitConverter.GetBytes(25000L).Reverse().ToArray()},
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
    [MemberData(nameof(AllChangesFieldsData))]
    public void Conversion_cdc_column_to_change_row(
        Dictionary<string, object> columnFields,
        string captureInstance,
        AllChangeRow expected)
    {
        var result = AllChangeRowFactory.Create(columnFields, captureInstance);
        result.Should().BeEquivalentTo(expected);
    }

    [Theory]
    [Trait("Category", "Unit")]
    [MemberData(nameof(CdcDefaultFieldsInvalidData))]
    public void Conversion_cdc_column_without_default_fields_is_invalid(
        Dictionary<string, object> columnFields,
        string captureInstance)
    {
        Invoking(() => AllChangeRowFactory.Create(columnFields, captureInstance))
            .Should()
            .Throw<ArgumentException>()
            .WithMessage($"The column fields does not contain all the default CDC column fields.");
    }
}
