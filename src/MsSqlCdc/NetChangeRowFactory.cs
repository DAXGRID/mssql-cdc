using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;

namespace MsSqlCdc;

internal static class NetChangeRowFactory
{
    /// <summary>
    /// Converts a a collection of columns represented as Dictionary<string, object> to ChangeData representation.
    /// </summary>
    /// <param name="fields">Dictionary of field name and field value.</param>
    /// <param name="captureInstance">The capture instance.</param>
    /// <returns>Returns the CDC column as a ChangeData record.</returns>
    /// <exception cref="Exception"></exception>
    public static NetChangeRow Create(
        IReadOnlyDictionary<string, object> fields,
        string captureInstance)
    {
        if (GetRequiredFields(fields).Count() < 3)
            throw new ArgumentException($"The column fields does not contain all the default CDC column fields.");

        return new NetChangeRow(
            GetStartLsn(fields),
            GetOperation(fields),
            GetUpdateMask(fields),
            captureInstance,
            GetAdditionalFields(fields));
    }

    private static BigInteger GetStartLsn(IReadOnlyDictionary<string, object> fields) =>
        DataConvert.ConvertBinaryLsn((byte[])fields[CdcFieldName.StartLsn]);

    private static byte[]? GetUpdateMask(IReadOnlyDictionary<string, object> fields) =>
        fields[CdcFieldName.UpdateMask] != DBNull.Value
        ? (byte[])fields[CdcFieldName.UpdateMask]
        : null;

    private static bool IsRequiredField(string fieldName) =>
        fieldName == CdcFieldName.StartLsn ||
        fieldName == CdcFieldName.Operation ||
        fieldName == CdcFieldName.UpdateMask;

    private static IEnumerable<KeyValuePair<string, object>> GetRequiredFields(
        IReadOnlyDictionary<string, object> fields) => fields.Where(x => IsRequiredField(x.Key));

    private static Dictionary<string, object> GetAdditionalFields(IReadOnlyDictionary<string, object> fields)
        => fields.Where(x => !IsRequiredField(x.Key)).ToDictionary(x => x.Key, x => x.Value);

    private static NetChangeOperation GetOperation(IReadOnlyDictionary<string, object> fields) =>
         ConvertOperation((int)fields[CdcFieldName.Operation]);

    private static NetChangeOperation ConvertOperation(int representation)
        => representation switch
        {
            1 => NetChangeOperation.Delete,
            2 => NetChangeOperation.Insert,
            4 => NetChangeOperation.Update,
            5 => NetChangeOperation.InsertOrUpdate,
            _ => throw new ArgumentException($"Not valid representation value '{representation}'")
        };
}
