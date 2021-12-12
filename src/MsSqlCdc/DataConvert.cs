using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;

namespace MsSqlCdc;

public static class DataConvert
{
    /// <summary>
    /// Converts a a colection of columns represented as Tuple<string, object> to ChangeData<dynamic> representation.
    /// </summary>
    /// <param name="column">List of tuples with Item1 being the name column and Item2 being the column value</param>
    /// <param name="tableName">The tablename of the column.</param>
    /// <returns>Returns the CDC column as a ChangeData record.</returns>
    public static ChangeData<dynamic> ConvertCdcColumn(List<Tuple<string, object>> column, string tableName)
    {
        var startLsn = ConvertBinaryLsn((byte[])column[0].Item2);
        var seqVal = ConvertBinaryLsn((byte[])column[1].Item2);
        var operation = ConvertIntOperation((int)column[2].Item2);
        var updateMask = Encoding.UTF8.GetString((byte[])column[3].Item2);

        var body = column.Skip(4)
            .Aggregate(new ExpandoObject() as IDictionary<string, object>,
                       (acc, x) => { acc[x.Item1] = x.Item2; return acc; }) as dynamic;

        return new ChangeData<dynamic>(
            startLsn,
            seqVal,
            operation,
            updateMask,
            tableName,
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
}
