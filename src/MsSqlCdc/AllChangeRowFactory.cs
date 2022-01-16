using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MsSqlCdc;

internal class AllChangeRowFactory
{
    /// <summary>
    /// Converts a a collection of columns represented as Dictionary<string, object> to ChangeData representation.
    /// </summary>
    /// <param name="columnFields">Dictionary of field name and field value.</param>
    /// <param name="captureInstance">The capture instance.</param>
    /// <returns>Returns the CDC column as a ChangeData record.</returns>
    /// <exception cref="Exception"></exception>
    public static AllChangeRow Create(
        IReadOnlyDictionary<string, object> columnFields,
        string captureInstance)
    {
        bool isDefaultCdcField(string fieldName) =>
                fieldName == CdcFieldName.StartLsn ||
                fieldName == CdcFieldName.SeqVal ||
                fieldName == CdcFieldName.Operation ||
                fieldName == CdcFieldName.UpdateMask;

        if (columnFields.Where(x => isDefaultCdcField(x.Key)).Count() < 4)
            throw new ArgumentException(
                        $"The column fields does not contain all the default CDC column fields.");

        var nonDefaultCdcFields = columnFields
            .Where(x => !isDefaultCdcField(x.Key))
            .ToDictionary(x => x.Key, x => x.Value);

        var startLsn = DataConvert.ConvertBinaryLsn((byte[])columnFields[CdcFieldName.StartLsn]);
        var seqVal = DataConvert.ConvertBinaryLsn((byte[])columnFields[CdcFieldName.SeqVal]);
        var operation = ConvertAllChangeOperation((int)columnFields[CdcFieldName.Operation]);
        var updateMask = Encoding.UTF8.GetString((byte[])columnFields[CdcFieldName.UpdateMask]);

        return new AllChangeRow(
            startLsn,
            seqVal,
            operation,
            updateMask,
            captureInstance,
            nonDefaultCdcFields);
    }

    private static AllChangeOperation ConvertAllChangeOperation(int representation)
        => representation switch
        {
            1 => AllChangeOperation.Delete,
            2 => AllChangeOperation.Insert,
            3 => AllChangeOperation.BeforeUpdate,
            4 => AllChangeOperation.AfterUpdate,
            _ => throw new ArgumentException($"Not valid representation value '{representation}'")
        };
}
