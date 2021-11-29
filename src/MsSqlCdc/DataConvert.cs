using System;
using System.Linq;
using System.Text;

namespace MsSqlCdc;

public static class DataConvert
{
    public static string ConvertVarBinary(byte[] bytes)
    {
        return Encoding.UTF8.GetString(bytes);
    }

    public static long ConvertBinaryLsnBigInt(byte[] bytes)
    {
        if (BitConverter.IsLittleEndian)
            bytes = bytes.Reverse().ToArray();

        return BitConverter.ToInt64(bytes);
    }
}
