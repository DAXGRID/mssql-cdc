namespace MssqlCdc;

public record Config
{
    public string? ConnectionString { get; init; }
}
