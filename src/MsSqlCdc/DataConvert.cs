using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Numerics;
using System.Text;

namespace MsSqlCdc;

internal static class DataConvert
{
    /// <summary>
    /// Converts a a collection of columns represented as Tuple<string, object> to ChangeData<dynamic> representation.
    /// </summary>
    /// <param name="columnFields">List of tuples with Item1 being the name column and Item2 being the column value</param>
    /// <param name="captureInstance">The tablename of the column.</param>
    /// <returns>Returns the CDC column as a ChangeData record.</returns>
    /// <exception cref="Exception"></exception>
    public static ChangeRow<dynamic> ConvertCdcColumn(
        List<(string fieldName, object fieldValue)> columnFields,
        string captureInstance)
    {
        bool isDefaultCdcField(string fieldName) =>
                fieldName == CdcFieldName.StartLsn ||
                fieldName == CdcFieldName.SeqVal ||
                fieldName == CdcFieldName.Operation ||
                fieldName == CdcFieldName.UpdateMask;

        if (columnFields.Where(x => isDefaultCdcField(x.fieldName)).Count() < 4)
            throw new ArgumentException(
                $"The column fields does not contain all the default CDC column fields.");

        var startLsn = ConvertBinaryLsn(
            (byte[])columnFields.First(x => x.fieldName == CdcFieldName.StartLsn).fieldValue);
        var seqVal = ConvertBinaryLsn(
            (byte[])columnFields.First(x => x.fieldName == CdcFieldName.SeqVal).fieldValue);
        var operation = ConvertOperation(
            (int)columnFields.First(x => x.fieldName == CdcFieldName.Operation).fieldValue);
        var updateMask = Encoding.UTF8.GetString(
            (byte[])columnFields.First(x => x.fieldName == CdcFieldName.UpdateMask).fieldValue);

        var body = columnFields
        .Where(x => !isDefaultCdcField(x.fieldName))
        .Aggregate(new ExpandoObject() as IDictionary<string, object>,
                   (acc, x) => { acc[x.fieldName] = x.fieldValue; return acc; }) as dynamic;

        return new ChangeRow<dynamic>(
            startLsn,
            seqVal,
            operation,
            updateMask,
            captureInstance,
            body);
    }

    /// <summary>
    /// Converts the number representation to an Enum representation of the value.
    /// </summary>
    /// <param name="representation">The number representation of the Operation.</param>
    /// <returns>Enum representation of the number representation.</returns>
    /// <exception cref="ArgumentException"></exception>
    public static Operation ConvertOperation(int representation)
        => representation switch
        {
            1 => Operation.Delete,
            2 => Operation.Insert,
            3 => Operation.BeforeUpdate,
            4 => Operation.AfterUpdate,
            _ => throw new ArgumentException($"Not valid representation value '{representation}'")
        };

    /// <summary>
    /// Converts RelationOperator enum to a string representation to be used in MS-SQL.
    /// </summary>
    /// <param name="representation">The enum representation of RelationOperator.</param>
    /// <returns>String representation of RelationOperator.</returns>
    /// <exception cref="ArgumentException"></exception>
    public static string ConvertRelationOperator(RelationalOperator relationalOperator)
        => relationalOperator switch
        {
            RelationalOperator.LargestLessThan => "largest less than",
            RelationalOperator.LargestLessThanOrEqual => "largest less than or equal",
            RelationalOperator.SmallestGreaterThan => "smallest greater than",
            RelationalOperator.SmallestGreaterThanOrEqual => "smallest greater than or equal",
            _ => throw new ArgumentException($"Not valid representation value '{relationalOperator}'")
        };

    /// <summary>
    /// Converts NetChangesRowFilterOption enum to a string representation to be used in MS-SQL.
    /// </summary>
    /// <param name="representation">The enum representation of NetChangesRowFilterOption.</param>
    /// <returns>String representation of NetChangesRowfilterOption.</returns>
    /// <exception cref="ArgumentException"></exception>
    public static string ConvertNetChangesRowFilterOption(
        NetChangesRowFilterOption netChangesRowFilterOption) => netChangesRowFilterOption switch
        {
            NetChangesRowFilterOption.All => "all",
            NetChangesRowFilterOption.AllWithMask => "all with mask",
            NetChangesRowFilterOption.AllWithMerge => "all with merge",
            _ => throw new ArgumentException($"Not valid representation value '{netChangesRowFilterOption}'")
        };

    /// <summary>
    /// Converts AllChangesRowFilterOption enum to a string representation to be used in MS-SQL.
    /// </summary>
    /// <param name="representation">The enum representation of AllChangesRowFilterOption.</param>
    /// <returns>String representation of AllChangesRowFilterOption.</returns>
    /// <exception cref="ArgumentException"></exception>
    public static string ConvertAllChangesRowFilterOption(
        AllChangesRowFilterOption allChangesRowFilterOption) => allChangesRowFilterOption switch
        {
            AllChangesRowFilterOption.All => "all",
            AllChangesRowFilterOption.AllUpdateOld => "all update old",
            _ => throw new ArgumentException($"Not valid representation value '{allChangesRowFilterOption}'")
        };

    /// <summary>
    /// Convert LSN BigInteger to ByteArray in BigEndian format.
    /// </summary>
    /// <param name="representation">BigInteger representation of LSN.</param>
    /// <returns>Binary array of BigInteger LSN.</returns>
    public static byte[] ConvertLsnBigEridian(BigInteger lsn) => lsn.ToByteArray().Reverse().ToArray();

    /// <summary>
    /// Convert the binary representation of the line-sequence-number to BigInteger.
    /// Automatically handle endianness doing the conversion.
    /// </summary>
    /// <param name="bytes">The byte array representation of the LSN number.</param>
    /// <returns>The BigInteger representation of the line-sequence-number.</returns>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    public static BigInteger ConvertBinaryLsn(byte[] bytes) => BitConverter.IsLittleEndian
        ? new BigInteger(bytes.Reverse().ToArray())
        : new BigInteger(bytes);
}
