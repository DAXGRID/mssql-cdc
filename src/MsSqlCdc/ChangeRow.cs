using System;
using System.Numerics;

namespace MsSqlCdc;

public enum Operation
{
    Delete = 1,
    Insert = 2,
    BeforeUpdate = 3,
    AfterUpdate = 4
}

public record ChangeRow<T>
{
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
    public Operation Operation { get; init; }

    /// <summary>
    /// A bit mask with a bit corresponding to each captured column identified for the capture instance.
    /// This value has all defined bits set to 1 when __$operation = 1 or 2. When __$operation = 3 or 4,
    /// only those bits corresponding to columns that changed are set to 1.
    /// </summary>
    public string UpdateMask { get; init; }

    /// <summary>
    /// The name of the capture instance associated with the change.
    /// </summary>
    public string CaptureInstance { get; set; }

    /// <summary>
    /// Dynamic column fields.
    /// </summary>
    public T Body { get; init; }

    public ChangeRow(
        BigInteger startLineSequenceNumber,
        BigInteger sequenceValue,
        Operation operation,
        string updateMask,
        string captureInstance,
        T body)
    {
        if (body is null)
            throw new ArgumentNullException($"{nameof(body)} cannot be null.");
        if (string.IsNullOrWhiteSpace(captureInstance))
            throw new ArgumentNullException($"{nameof(captureInstance)} cannot be null, empty or whitespace.");

        StartLineSequenceNumber = startLineSequenceNumber;
        SequenceValue = sequenceValue;
        Operation = operation;
        UpdateMask = updateMask;
        CaptureInstance = captureInstance;
        Body = body;
    }
}
