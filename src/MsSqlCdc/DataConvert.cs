using System;
using System.Linq;

namespace MsSqlCdc;

public static class DataConvert
{
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
