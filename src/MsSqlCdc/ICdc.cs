using System;
using System.Threading.Tasks;

namespace MssqlCdc;

public interface ICdc : IDisposable
{
    Task Subscribe { get; }
}
