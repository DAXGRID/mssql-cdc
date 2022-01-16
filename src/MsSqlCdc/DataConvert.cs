using System;
using System.Linq;
using System.Numerics;

namespace MsSqlCdc;

internal static class DataConvert
{
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
    /// Convert LSN BigInteger to ByteArray in BigEndian format,
    /// Also makes sure that the size of the returned byte array is always 10 bytes.
    /// </summary>
    /// <param name="representation">BigInteger representation of LSN.</param>
    /// <returns>Binary array of BigInteger LSN.</returns>
    public static byte[] ConvertLsnBigEndian(BigInteger lsn)
    {
        var newArray = new byte[10];
        var lsnBytes = lsn.ToByteArray(isBigEndian: true);
        var startAt = newArray.Length - lsnBytes.Length;
        Array.Copy(lsnBytes, 0, newArray, startAt, lsnBytes.Length);
        return newArray;
    }

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
