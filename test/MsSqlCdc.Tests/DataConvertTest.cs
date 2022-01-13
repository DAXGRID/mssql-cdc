using Xunit;
using FluentAssertions;
using static FluentAssertions.FluentActions;
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Text.Json;

namespace MsSqlCdc.Tests;

public class DataConverTest
{
    public static IEnumerable<object[]> CdcColumnFieldsData()
    {
        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"__$start_lsn", BitConverter.GetBytes(25000L).Reverse().ToArray()},
                {"__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()},
                {"__$operation", (int)Operation.AfterUpdate},
                {"__$update_mask", Encoding.ASCII.GetBytes("MASK")},
                {"Id", 10},
                {"Name", "Rune"},
                {"Salary", 20000.00},
            },
            "dbo_Employee",
            new ChangeRow(
                25000L,
                25002L,
                Operation.AfterUpdate,
                "MASK",
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
                {"__$operation", (int)Operation.BeforeUpdate},
                {"__$update_mask", Encoding.ASCII.GetBytes("MASK")},
                {"Id", 1},
                {"Name", "Simon"},
            },
            "dbo_Employee",
             new ChangeRow(
                25000L,
                25002L,
                Operation.BeforeUpdate,
                "MASK",
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
                {"__$operation", (int)Operation.Delete},
                {"__$update_mask", Encoding.ASCII.GetBytes("MASK")},
                {"Id", 0},
                {"Name", "Jesper"},
            },
            "dbo_Employee",
             new ChangeRow(
                25000L,
                25002L,
                Operation.Delete,
                "MASK",
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
                {"__$operation", (int)Operation.Insert},
                {"__$update_mask", Encoding.ASCII.GetBytes("MASK")},
                {"Id", 10},
            },
            "dbo_Animal",
             new ChangeRow(
                25000L,
                25002L,
                Operation.Insert,
                "MASK",
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
                {"__$operation", (int)Operation.Insert},
                {"__$update_mask", Encoding.ASCII.GetBytes("MASK")},
            },
            "dbo_Animal",
             new ChangeRow(
                25000L,
                25002L,
                Operation.Insert,
                "MASK",
                "dbo_Animal",
                new Dictionary<string, object>{
                })
        };

        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"__$start_lsn", BitConverter.GetBytes(25000L).Reverse().ToArray()},
                {"__$update_mask", Encoding.ASCII.GetBytes("MASK")},
                {"__$operation", (int)Operation.Insert},
                {"__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()},
            },
            "dbo_Animal",
             new ChangeRow(
                25000L,
                25002L,
                Operation.Insert,
                "MASK",
                "dbo_Animal",
                new Dictionary<string, object>{
                })
        };

        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"__$update_mask", Encoding.ASCII.GetBytes("MASK")},
                {"__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()},
                {"__$start_lsn", BitConverter.GetBytes(25000L).Reverse().ToArray()},
                {"__$operation", (int)Operation.Insert},
            },
            "dbo_Animal",
             new ChangeRow(
                25000L,
                25002L,
                Operation.Insert,
                "MASK",
                "dbo_Animal",
                new Dictionary<string, object>{
                })
        };

        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"Id", 0},
                {"__$operation", (int)Operation.Delete},
                {"Name", "Jesper"},
                {"__$update_mask", Encoding.ASCII.GetBytes("MASK")},
                {"__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()},
                {"__$start_lsn", BitConverter.GetBytes(25000L).Reverse().ToArray()},
            },
            "dbo_Employee",
             new ChangeRow(
                25000L,
                25002L,
                Operation.Delete,
                "MASK",
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
                {"__$update_mask", Encoding.ASCII.GetBytes("MASK")},
                {"__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()},
            },
            "dbo_Employee",
        };

        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"__$update_mask", Encoding.ASCII.GetBytes("MASK")},
                {"__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()},
                {"__$start_lsn", BitConverter.GetBytes(25000L).Reverse().ToArray()},
            },
            "dbo_Employee",
        };

        yield return new object[]
        {
            new Dictionary<string, object>
            {
                {"__$update_mask", Encoding.ASCII.GetBytes("MASK")},
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
    [MemberData(nameof(CdcColumnFieldsData))]
    public void Conversion_cdc_column_to_change_row(
        Dictionary<string, object> columnFields,
        string captureInstance,
        ChangeRow expected)
    {
        var result = DataConvert.ConvertCdcColumn(columnFields, captureInstance);

        // We do this since record type equality operator does not work with dynamic members.
        JsonSerializer.Serialize(result).Should().Be(JsonSerializer.Serialize(expected));
    }

    [Theory]
    [MemberData(nameof(CdcDefaultFieldsInvalidData))]
    public void Conversion_cdc_column_without_default_fields_is_invalid(
        Dictionary<string, object> columnFields,
        string captureInstance)
    {
        Invoking(() => DataConvert.ConvertCdcColumn(columnFields, captureInstance))
            .Should()
            .Throw<ArgumentException>()
            .WithMessage($"The column fields does not contain all the default CDC column fields.");
    }

    [Theory]
    [InlineData(1, Operation.Delete)]
    [InlineData(2, Operation.Insert)]
    [InlineData(3, Operation.BeforeUpdate)]
    [InlineData(4, Operation.AfterUpdate)]
    public void Operation_valid_number_representation_should_be_converted(int input, Operation expected)
    {
        var operation = DataConvert.ConvertOperation(input);
        operation.Should().Be(expected);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(-10)]
    [InlineData(0)]
    [InlineData(100)]
    [InlineData(int.MinValue)]
    [InlineData(int.MaxValue)]
    public void Operation_invalid_number_representation_should_not_be_converted(int input)
    {
        Invoking(() => DataConvert.ConvertOperation(input)).Should().Throw<ArgumentException>();
    }

    [Theory]
    [InlineData(RelationalOperator.LargestLessThan, "largest less than")]
    [InlineData(RelationalOperator.LargestLessThanOrEqual, "largest less than or equal")]
    [InlineData(RelationalOperator.SmallestGreaterThan, "smallest greater than")]
    [InlineData(RelationalOperator.SmallestGreaterThanOrEqual, "smallest greater than or equal")]
    public void Convert_relation_operator_to_string_representation(
        RelationalOperator relationalOperator,
        string expected)
    {
        var stringRepresentation = DataConvert.ConvertRelationOperator(relationalOperator);
        stringRepresentation.Should().Be(expected);
    }

    [Theory]
    [InlineData(NetChangesRowFilterOption.All, "all")]
    [InlineData(NetChangesRowFilterOption.AllWithMask, "all with mask")]
    [InlineData(NetChangesRowFilterOption.AllWithMerge, "all with merge")]
    public void Convert_net_changes_filter_options_to_string_representation(
        NetChangesRowFilterOption filterOption,
        string expected)
    {
        var stringRepresentation = DataConvert.ConvertNetChangesRowFilterOption(filterOption);
        stringRepresentation.Should().Be(expected);
    }

    [Theory]
    [InlineData(AllChangesRowFilterOption.All, "all")]
    [InlineData(AllChangesRowFilterOption.AllUpdateOld, "all update old")]
    public void Convert_all_changes_filter_options_to_string_representation(
        AllChangesRowFilterOption filterOption,
        string expected)
    {
        var stringRepresentation = DataConvert.ConvertAllChangesRowFilterOption(filterOption);
        stringRepresentation.Should().Be(expected);
    }
}
