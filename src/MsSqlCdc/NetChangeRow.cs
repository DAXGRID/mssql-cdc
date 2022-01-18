using System;
using System.Collections.Generic;
using System.Numerics;

namespace MsSqlCdc;

public enum NetChangeOperation
{
    Delete = 1,
    Insert = 2,
    Update = 4,
    InsertOrUpdate = 5
}

public record NetChangeRow
{
    /// <summary>
    /// All changes committed in the same transaction share the same commit LSN.
    /// For example, if an update operation on the source table modifies two columns in two rows,
    /// the change table will contain four rows, each with the same __$start_lsnvalue.
    /// </summary>
    public BigInteger StartLineSequenceNumber { get; init; }

    /// <summary>
    /// Identifies the data manipulation language (DML) operation needed to apply the row of
    /// change data to the target data source.
    /// If the value of the row_filter_option parameter is all or all with mask,
    /// the value in this column can be one of the following values:
    /// 1 = Delete
    /// 2 = Insert
    /// 4 = Update
    /// If the value of the row_filter_option parameter is all with merge,
    /// the value in this column can be one of the following values:
    /// 1 = Delete
    /// 5 = Insert or update
    /// </summary>
    public NetChangeOperation Operation { get; init; }

    /// <summary>
    /// A bit mask with a bit corresponding to each captured column identified for the capture instance.
    /// This value has all defined bits set to 1 when __$operation = 1 or 2. When __$operation = 3 or 4,
    /// only those bits corresponding to columns that changed are set to 1.
    /// </summary>
    public byte[]? UpdateMask { get; init; }

    /// <summary>
    /// The name of the capture instance associated with the change.
    /// </summary>
    public string CaptureInstance { get; set; }

    /// <summary>
    /// The row fields.
    /// </summary>
    public IReadOnlyDictionary<string, object> Fields { get; init; }

    public NetChangeRow(
        BigInteger startLineSequenceNumber,
        NetChangeOperation operation,
        byte[]? updateMask,
        string captureInstance,
        IReadOnlyDictionary<string, object> fields)
    {
        if (fields is null)
            throw new ArgumentNullException($"{nameof(fields)} cannot be null.");
        if (string.IsNullOrWhiteSpace(captureInstance))
            throw new ArgumentNullException($"{nameof(captureInstance)} cannot be null, empty or whitespace.");

        StartLineSequenceNumber = startLineSequenceNumber;
        Operation = operation;
        UpdateMask = updateMask;
        CaptureInstance = captureInstance;
        Fields = fields;
    }
}
