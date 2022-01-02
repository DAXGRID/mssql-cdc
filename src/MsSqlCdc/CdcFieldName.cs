namespace MsSqlCdc;

internal static class CdcFieldName
{
    public const string StartLsn = "__$start_lsn";
    public const string SeqVal = "__$seqval";
    public const string Operation = "__$operation";
    public const string UpdateMask = "__$update_mask";
}
