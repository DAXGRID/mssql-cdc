using System;
using System.Collections.Generic;
using System.Numerics;

namespace MsSqlCdc;

public enum AllChangeOperation
{
    Delete,
    Insert,
    BeforeUpdate,
    AfterUpdate
}

public record AllChangeRow
{
    private readonly byte[] _updateMask;

    /// <summary>
    /// Commit LSN associated with the change that preserves the commit order of the change.
    /// Changes committed in the same transaction share the same commit LSN value.
    /// </summary>
    public BigInteger StartLineSequenceNumber { get; init; }

    /// <summary>
    /// Sequence value used to order changes to a row within a transaction.
    /// </summary>
    public BigInteger SequenceValue { get; init; }

    /// <summary>
    /// Identifies the data manipulation language (DML) operation needed
    /// to apply the row of change data to the target data source.
    /// </summary>
    public AllChangeOperation Operation { get; init; }

    /// <summary>
    /// A bit mask with a bit corresponding to each captured column identified for the capture instance.
    /// This value has all defined bits set to 1 when __$operation = 1 or 2. When __$operation = 3 or 4,
    /// only those bits corresponding to columns that changed are set to 1.
    /// </summary>
    public byte[] GetUpdateMask() => (byte[])_updateMask.Clone();

    /// <summary>
    /// The name of the capture instance associated with the change.
    /// </summary>
    public string CaptureInstance { get; set; }

    /// <summary>
    /// The row fields.
    /// </summary>
    public IReadOnlyDictionary<string, object> Fields { get; init; }

    public AllChangeRow(
        BigInteger startLineSequenceNumber,
        BigInteger sequenceValue,
        AllChangeOperation operation,
        byte[] updateMask,
        string captureInstance,
        IReadOnlyDictionary<string, object> fields)
    {
        if (fields is null)
            throw new ArgumentNullException(nameof(fields), "Cannot be null.");
        if (string.IsNullOrWhiteSpace(captureInstance))
            throw new ArgumentNullException(nameof(captureInstance), "Cannot be null, empty or whitespace.");

        StartLineSequenceNumber = startLineSequenceNumber;
        SequenceValue = sequenceValue;
        Operation = operation;
        _updateMask = updateMask;
        CaptureInstance = captureInstance;
        Fields = fields;
    }
}
