using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MsSqlCdc;

internal static class AllChangeRowFactory
{
    /// <summary>
    /// Converts a a collection of columns represented as Dictionary<string, object> to ChangeData representation.
    /// </summary>
    /// <param name="fields">Dictionary of field name and field value.</param>
    /// <param name="captureInstance">The capture instance.</param>
    /// <returns>Returns the CDC column as a ChangeData record.</returns>
    /// <exception cref="Exception"></exception>
    public static AllChangeRow Create(IReadOnlyDictionary<string, object> fields, string captureInstance)
    {
        if (GetRequiredFields(fields).Count() < 4)
            throw new ArgumentException($"The column fields does not contain all the default CDC column fields.");

        var startLsn = DataConvert.ConvertBinaryLsn((byte[])fields[CdcFieldName.StartLsn]);
        var seqVal = DataConvert.ConvertBinaryLsn((byte[])fields[CdcFieldName.SeqVal]);
        var operation = ConvertOperation((int)fields[CdcFieldName.Operation]);
        var updateMask = Encoding.UTF8.GetString((byte[])fields[CdcFieldName.UpdateMask]);
        var notDefaultFields = GetNotDefaultFields(fields);

        return new AllChangeRow(
            startLsn,
            seqVal,
            operation,
            updateMask,
            captureInstance,
            notDefaultFields);
    }

    private static bool IsRequiredField(string fieldName) =>
        fieldName == CdcFieldName.StartLsn ||
        fieldName == CdcFieldName.SeqVal ||
        fieldName == CdcFieldName.Operation ||
        fieldName == CdcFieldName.UpdateMask;

    private static IEnumerable<KeyValuePair<string, object>> GetRequiredFields(
        IReadOnlyDictionary<string, object> fields)
        => fields.Where(x => IsRequiredField(x.Key));

    private static Dictionary<string, object> GetNotDefaultFields(IReadOnlyDictionary<string, object> fields)
        => fields.Where(x => !IsRequiredField(x.Key)).ToDictionary(x => x.Key, x => x.Value);

    private static AllChangeOperation ConvertOperation(int representation)
        => representation switch
        {
            1 => AllChangeOperation.Delete,
            2 => AllChangeOperation.Insert,
            3 => AllChangeOperation.BeforeUpdate,
            4 => AllChangeOperation.AfterUpdate,
            _ => throw new ArgumentException($"Not valid representation value '{representation}'")
        };
}
