using FluentAssertions;
using Xunit;

namespace MsSqlCdc.Tests;

public class DataConverTests
{
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
