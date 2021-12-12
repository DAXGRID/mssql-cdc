using System;

namespace MsSqlCdc;

public enum Operation : ushort
{
    Delete = 1,
    Insert = 2,
    BeforeUpdate = 3,
    AfterUpdate = 4
}

public record ChangeData<T>
{
    public long StartLineSequenceNumber { get; init; }
    public long SequenceValue { get; init; }
    public Operation Operation { get; init; }
    public string UpdateMask { get; init; }
    public string TableName { get; set; }
    public T Body { get; init; }

    public ChangeData(
        long startLineSequenceNumber,
        long sequenceValue,
        Operation operation,
        string updateMask,
        string tableName,
        T body)
    {
        if (body is null)
            throw new ArgumentNullException($"{nameof(body)} cannot be null.");
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentNullException($"{nameof(tableName)} cannot be null, empty or whitespace.");

        StartLineSequenceNumber = startLineSequenceNumber;
        SequenceValue = sequenceValue;
        Operation = operation;
        UpdateMask = updateMask;
        TableName = tableName;
        Body = body;
    }
}
