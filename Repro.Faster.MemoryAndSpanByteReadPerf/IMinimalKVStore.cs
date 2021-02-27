using FASTER.core;
using System;
using System.Threading.Tasks;

namespace Repro.Faster.MemoryAndSpanByteReadPerf
{
    public interface IMinimalKVStore : IDisposable
    {
        Task<(Status, string?)> ReadAsync(int key);
        void Upsert(int key, string value);
    }
}