using System;
using System.Threading.Tasks;
using MsSqlCdc;

namespace Example;
public class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Starting to listen");
        var listener = new CdcListener();
        await listener.Start();
    }
}
