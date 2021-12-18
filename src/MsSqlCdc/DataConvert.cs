using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;

namespace MsSqlCdc;

internal static class DataConvert
{
    /// <summary>
    /// Converts a a colection of columns represented as Tuple<string, object> to ChangeData<dynamic> representation.
    /// </summary>
    /// <param name="columnFields">List of tuples with Item1 being the name column and Item2 being the column value</param>
    /// <param name="captureInstance">The tablename of the column.</param>
    /// <returns>Returns the CDC column as a ChangeData record.</returns>
    /// <exception cref="Exception"></exception>
    public static ChangeData<dynamic> ConvertCdcColumn(
        List<(string fieldName, object fieldValue)> columnFields,
        string captureInstance)
    {
        if (columnFields.Count < 3)
            throw new Exception($"Count of column fields should be 4 or greater, instead got '{columnFields.Count}'.");

        var startLsn = ConvertBinaryLsn((byte[])columnFields[0].fieldValue);
        var seqVal = ConvertBinaryLsn((byte[])columnFields[1].fieldValue);
        var operation = ConvertIntOperation((int)columnFields[2].fieldValue);
        var updateMask = Encoding.UTF8.GetString((byte[])columnFields[3].fieldValue);

        var body = columnFields.Skip(4)
            .Aggregate(new ExpandoObject() as IDictionary<string, object>,
                       (acc, x) => { acc[x.fieldName] = x.fieldValue; return acc; }) as dynamic;

        return new ChangeData<dynamic>(
            startLsn,
            seqVal,
            operation,
            updateMask,
            captureInstance,
            body
        );
    }

    /// <summary>
    /// Convert the binary representation of the line-sequence-number to Int64.
    /// Automatically handle endianness doing the convertion.
    /// </summary>
    /// <param name="bytes">The byte array representation of the LSN number.</param>
    /// <returns>The Int64 representation of the line-sequence-number.</returns>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    public static long ConvertBinaryLsn(byte[] bytes)
    {
        if (BitConverter.IsLittleEndian)
            return BitConverter.ToInt64(bytes.Reverse().ToArray());

        return BitConverter.ToInt64(bytes);
    }

    /// <summary>
    /// Converts the number representation to an Enum representation of the value.
    /// </summary>
    /// <param name="representation">The number representation of the Operation.</param>
    /// <returns>Enum representation of the number representation.</returns>
    /// <exception cref="ArgumentException"></exception>
    public static Operation ConvertIntOperation(int representation)
    {
        return representation switch
        {
            1 => Operation.Delete,
            2 => Operation.Insert,
            3 => Operation.BeforeUpdate,
            4 => Operation.AfterUpdate,
            _ => throw new ArgumentException($"Not valid representation value '{representation}'")
        };
    }

    public static string RelationOperatorToStringRepresentation(RelationalOperator relationalOperator)
    {
        return relationalOperator switch
        {
            RelationalOperator.LargestLessThan => "largest less than",
            RelationalOperator.LargestLessThanOrEqual => "largest less than or equal",
            RelationalOperator.SmallestGreaterThan => "smallest greater than",
            RelationalOperator.SmallestGreaterThanOrEqual => "smallest greater than or equal",
            _ => throw new ArgumentException($"Not valid representation value '{relationalOperator}'")
        };
    }
}
