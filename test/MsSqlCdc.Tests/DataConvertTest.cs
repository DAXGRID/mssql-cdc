using Xunit;
using FluentAssertions;
using static FluentAssertions.FluentActions;
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Text.Json;
using System.Dynamic;

namespace MsSqlCdc.Tests;

public class DataConverTest
{
    public static IEnumerable<object[]> CdcColumnFieldsData()
    {
        yield return new object[]
        {
            new List<(string name, object fieldValue)>
            {
                ("__$start_lsn", BitConverter.GetBytes(25000L).Reverse().ToArray()),
                ("__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()),
                ("__$operation", (int)Operation.AfterUpdate),
                ("__$update_mask", Encoding.ASCII.GetBytes("MASK")),
                ("Id", 10),
                ("Name", "Rune"),
                ("Salary", 20000.00),
            },
        };

        yield return new object[]
        {
            new List<(string name, object fieldValue)>
            {
                ("__$start_lsn", BitConverter.GetBytes(25000L).Reverse().ToArray()),
                ("__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()),
                ("__$operation", (int)Operation.BeforeUpdate),
                ("__$update_mask", Encoding.ASCII.GetBytes("MASK")),
                ("Id", 1),
                ("Name", "Simon"),
            },
        };

        yield return new object[]
        {
            new List<(string name, object fieldValue)>
            {
                ("__$start_lsn", BitConverter.GetBytes(25000L).Reverse().ToArray()),
                ("__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()),
                ("__$operation", (int)Operation.Delete),
                ("__$update_mask", Encoding.ASCII.GetBytes("MASK")),
                ("Id", 10),
                ("Name", "Jesper"),
            },
        };

        yield return new object[]
        {
            new List<(string name, object fieldValue)>
            {
                ("__$start_lsn", BitConverter.GetBytes(25000L).Reverse().ToArray()),
                ("__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()),
                ("__$operation", (int)Operation.Insert),
                ("__$update_mask", Encoding.ASCII.GetBytes("MASK")),
                ("Id", 10),
            },
        };

        yield return new object[]
        {
            new List<(string name, object fieldValue)>
            {
                ("__$start_lsn", BitConverter.GetBytes(25000L).Reverse().ToArray()),
                ("__$seqval", BitConverter.GetBytes(25002L).Reverse().ToArray()),
                ("__$operation", (int)Operation.Insert),
                ("__$update_mask", Encoding.ASCII.GetBytes("MASK")),
            },
        };
    }

    [Theory]
    [MemberData(nameof(CdcColumnFieldsData))]
    public void ConvertCdcColumn_ShouldReturnChangeData_OnValidInput(
        List<(string name, object fieldValue)> columnFields)
    {
        var captureInstance = "dbo_Employee";

        var body = columnFields.Skip(4)
            .Aggregate(new ExpandoObject() as IDictionary<string, object>,
                       (acc, x) => { acc[x.name] = x.fieldValue; return acc; }) as dynamic;

        var changeData = new ChangeRow<dynamic>(
            BitConverter.ToInt64(((byte[])columnFields[0].fieldValue).Reverse().ToArray()),
            BitConverter.ToInt64(((byte[])columnFields[1].fieldValue).Reverse().ToArray()),
            (Operation)columnFields[2].fieldValue,
            Encoding.UTF8.GetString((byte[])columnFields[3].fieldValue),
            captureInstance,
            body
        );

        var result = DataConvert.ConvertCdcColumn(columnFields, captureInstance);

        // We do this since record type equality operator does not work with dynamic members.
        JsonSerializer.Serialize(result).Should().Be(JsonSerializer.Serialize(changeData));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void ConvertCdcColumn_ShouldThrowException_OnColumnFieldsBeingLessThanFour(int columnFieldsCount)
    {
        var captureInstance = "dbo_Employee";

        var columnFields = Enumerable.Range(0, columnFieldsCount)
            .Select(x => (name: x.ToString(), value: (object)x)).ToList();

        Invoking(() => DataConvert.ConvertCdcColumn(columnFields, captureInstance)).Should().Throw<Exception>();
    }

    [Theory]
    [InlineData(1, Operation.Delete)]
    [InlineData(2, Operation.Insert)]
    [InlineData(3, Operation.BeforeUpdate)]
    [InlineData(4, Operation.AfterUpdate)]
    public void ConvertIntOperation_ShouldReturnCorrectEnumConvertion(int input, Operation expected)
    {
        var operation = DataConvert.ConvertIntOperation(input);
        operation.Should().Be(expected);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(-10)]
    [InlineData(0)]
    [InlineData(100)]
    [InlineData(int.MinValue)]
    [InlineData(int.MaxValue)]
    public void ConvertIntOperation_ShouldThrowException_OnInvalidIntRepresentation(int input)
    {
        Invoking(() => DataConvert.ConvertIntOperation(input)).Should().Throw<ArgumentException>();
    }

    [Theory]
    [InlineData(RelationalOperator.LargestLessThan, "largest less than")]
    [InlineData(RelationalOperator.LargestLessThanOrEqual, "largest less than or equal")]
    [InlineData(RelationalOperator.SmallestGreaterThan, "smallest greater than")]
    [InlineData(RelationalOperator.SmallestGreaterThanOrEqual, "smallest greater than or equal")]
    public void RelationOperatorToStringRepresentation(RelationalOperator relationalOperator, string expected)
    {
        var stringRepresentation = DataConvert.RelationOperatorToStringRepresentation(relationalOperator);
        stringRepresentation.Should().Be(expected);
    }

    [Theory]
    [InlineData(NetChangesRowFilterOption.All, "all")]
    [InlineData(NetChangesRowFilterOption.AllWithMask, "all with mask")]
    [InlineData(NetChangesRowFilterOption.AllWithMerge, "all with merge")]
    public void NetChangesRowFilterOptionToStringRepresentation(
        NetChangesRowFilterOption filterOption,
        string expected)
    {
        var stringRepresentation = DataConvert.NetChangesRowFilterOptionToStringRepresentation(filterOption);
        stringRepresentation.Should().Be(expected);
    }

    [Theory]
    [InlineData(AllChangesRowFilterOption.All, "all")]
    [InlineData(AllChangesRowFilterOption.AllUpdateOld, "all update old")]
    public void AllChangesRowFilterOptionToStringRepresentation(
        AllChangesRowFilterOption filterOption,
        string expected)
    {
        var stringRepresentation = DataConvert.AllChangesRowFilterOptionToStringRepresentation(filterOption);
        stringRepresentation.Should().Be(expected);
    }
}
