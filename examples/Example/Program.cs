using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using MsSqlCdc;

namespace Example;

public class Program
{
    public static void Main(string[] args)
    {
        var connectionString = CreateConnectionString();
        var pollingIntervalMs = 100;
        var tables = new List<string> { "dbo_Employee" };
        var cdcCancellation = new CancellationTokenSource();
        var cdcCancellationToken = cdcCancellation.Token;

        var changeDataChannel = Channel.CreateUnbounded<IReadOnlyCollection<AllChangeRow>>();
        _ = Task.Factory.StartNew(async () =>
        {
            var lowBoundLsn = await GetStartLsn(connectionString);
            while (true)
            {
                if (cdcCancellationToken.IsCancellationRequested)
                {
                    // We mark the channel as completed to notify that all consumers should
                    // read the last elements and stop.
                    changeDataChannel.Writer.Complete();
                    break;
                }

                using var connection = new SqlConnection(connectionString);
                try
                {
                    await connection.OpenAsync();

                    var highBoundLsn = await Cdc.GetMaxLsn(connection);

                    if (lowBoundLsn <= highBoundLsn)
                    {
                        Console.WriteLine($"Polling with from '{lowBoundLsn}' to '{highBoundLsn}");

                        var changes = new List<AllChangeRow>();
                        foreach (var table in tables)
                        {
                            var changeSets = await Cdc.GetAllChanges(
                                connection, table, lowBoundLsn, highBoundLsn, AllChangesRowFilterOption.AllUpdateOld);
                            changes.AddRange(changeSets);
                        }

                        var orderedChanges = changes.OrderBy(x => x.SequenceValue).ToList();
                        await changeDataChannel.Writer.WriteAsync(orderedChanges);

                        lowBoundLsn = await Cdc.GetNextLsn(connection, highBoundLsn);
                    }
                    else
                    {
                        // No changes
                        Console.WriteLine($"No changes since last poll '{lowBoundLsn}'");
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
                finally
                {
                    await connection.CloseAsync();
                    await Task.Delay(pollingIntervalMs);
                }
            }
        }, cdcCancellationToken);

        var options = new JsonSerializerOptions
        {
            WriteIndented = true,
            Converters =
            {
                new JsonStringEnumConverter()
            }
        };

        _ = Task.Factory.StartNew(async () =>
        {
            await foreach (var changes in changeDataChannel.Reader.ReadAllAsync())
            {
                var changeDataJson = JsonSerializer.Serialize(changes, options);
                Console.WriteLine(changeDataJson + "\n");
            }
        });

        Console.ReadKey();
        cdcCancellation.Cancel();
    }

    private static async Task<BigInteger> GetStartLsn(string connectionString)
    {
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        var currentMaxLsn = await Cdc.GetMaxLsn(connection);
        return await Cdc.GetNextLsn(connection, currentMaxLsn);
    }

    private static string CreateConnectionString()
    {
        SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
        builder.DataSource = "localhost";
        builder.UserID = "sa";
        builder.Password = "myAwesomePassword1";
        builder.InitialCatalog = "TestDb";
        builder.Encrypt = false;
        return builder.ConnectionString;
    }
}
