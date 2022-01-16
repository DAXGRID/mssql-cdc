using System;
using System.Collections.Generic;
using System.Linq;
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
        if (fields.Where(x => IsRequiredField(x.Key)).Count() < 3)
            throw new ArgumentException($"The column fields does not contain all the default CDC column fields.");

        var startLsn = DataConvert.ConvertBinaryLsn((byte[])fields[CdcFieldName.StartLsn]);
        var operation = ConvertOperation((int)fields[CdcFieldName.Operation]);
        var updateMask = Encoding.UTF8.GetString((byte[])fields[CdcFieldName.UpdateMask]);
        var optionalFields = GetOptionalFields(fields);

        return new NetChangeRow(
            startLsn,
            operation,
            updateMask,
            captureInstance,
            optionalFields);
    }

    private static bool IsRequiredField(string fieldName) =>
        fieldName == CdcFieldName.StartLsn ||
        fieldName == CdcFieldName.Operation ||
        fieldName == CdcFieldName.UpdateMask;

    private static Dictionary<string, object> GetOptionalFields(IReadOnlyDictionary<string, object> fields)
        => fields.Where(x => !IsRequiredField(x.Key)).ToDictionary(x => x.Key, x => x.Value);

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
