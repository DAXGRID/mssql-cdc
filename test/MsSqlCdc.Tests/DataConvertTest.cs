using Xunit;
using FluentAssertions;
using static FluentAssertions.FluentActions;
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using FluentAssertions.Execution;

namespace MsSqlCdc.Tests;

public class DataConverTest
{
    [Theory]
    [InlineData(25000L, 25002L, Operation.AfterUpdate, "ABB", 10, "Rune", 20000.00)]
    [InlineData(25L, 25202L, Operation.BeforeUpdate, "DSDFS", 2, "Simon", 0.00)]
    [InlineData(250000000L, 250021L, Operation.Delete, "DFS", 3, "Foo", 1000000000.00)]
    [InlineData(0L, 2L, Operation.Insert, "DFS", 3, "John", 1000000000.00)]
    public void ConvertCdcColumn_ShouldReturnChangeData_OnValidInput(
        long startLsn,
        long seqVal,
        Operation operation,
        string updateMask,
        int id,
        string name,
        double salary)
    {
        var captureInstance = "dbo_Employee";
        var columnFields = new List<(string name, object value)>
        {
            ("__$start_lsn", BitConverter.GetBytes(startLsn).Reverse().ToArray()),
            ("__$seqval", BitConverter.GetBytes(seqVal).Reverse().ToArray()),
            ("__$operation", (int)operation),
            ("__$update_mask", Encoding.ASCII.GetBytes(updateMask)),
            ("Id", id),
            ("Name", name),
            ("Salary", salary),
        };

        var changeData = new ChangeData<dynamic>(
            startLsn,
            seqVal,
            operation,
            updateMask,
            "dbo_Employee",
            new
            {
                Id = id,
                Name = name,
                Salary = salary,
            }
        );

        var result = DataConvert.ConvertCdcColumn(columnFields, captureInstance);

        using (var scope = new AssertionScope())
        {
            Assert.True(result.Body.Id == changeData.Body.Id);
            Assert.True(result.Body.Name == changeData.Body.Name);
            Assert.True(result.Body.Salary == changeData.Body.Salary);
            result.StartLineSequenceNumber.Should().Be(changeData.StartLineSequenceNumber);
            result.SequenceValue.Should().Be(changeData.SequenceValue);
            result.Operation.Should().Be(changeData.Operation);
            result.UpdateMask.Should().Be(changeData.UpdateMask);
        }
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
}
