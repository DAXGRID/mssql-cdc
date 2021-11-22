using System.Threading.Tasks;

namespace MssqlCdc;

public class Cdc : ICdc
{
    Task ICdc.Subscribe => throw new System.NotImplementedException();

    public static Task Subscribe()
    {
        throw new System.NotImplementedException();
    }

    public void Dispose()
    {
        throw new System.NotImplementedException();
    }
}
