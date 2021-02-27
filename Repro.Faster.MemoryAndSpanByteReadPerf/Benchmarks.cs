using BenchmarkDotNet.Attributes;
using FASTER.core;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Repro.Faster.MemoryAndSpanByteReadPerf
{
    [MemoryDiagnoser]
    public class Benchmarks
    {
#pragma warning disable CS8618
        private IMinimalKVStore _minimalKVStore;
#pragma warning restore CS8618
        private const int READ_NUM_OPERATIONS = 10_000;
        private readonly List<Task> _readTasks = new();

        // ***** ObjLog store concurrent reads benchmark ***** 
        [GlobalSetup(Target = nameof(ObjLogStore_Reads))]
        public void ObjLogStore_Reads_GlobalSetup()
        {
            _minimalKVStore = new ObjLogMinimalKVStore();
            Parallel.For(0, READ_NUM_OPERATIONS, key => _minimalKVStore.Upsert(key, key.ToString()));
        }

        [Benchmark]
        public async Task ObjLogStore_Reads()
        {
            await ReadAll();
        }

        // ***** Memory<byte> store concurrent reads benchmark ***** 
        [GlobalSetup(Target = nameof(MemoryStore_Reads))]
        public void MemoryStore_Reads_GlobalSetup()
        {
            _minimalKVStore = new MemoryMinimalKVStore();
            Parallel.For(0, READ_NUM_OPERATIONS, key => _minimalKVStore.Upsert(key, key.ToString()));
        }

        [Benchmark]
        public async Task MemoryStore_Reads()
        {
            await ReadAll();
        }

        // ***** SpanByte store concurrent reads benchmark ***** 
        [GlobalSetup(Target = nameof(SpanByteStore_Reads))]
        public void SpanByteStore_Reads_GlobalSetup()
        {
            _minimalKVStore = new SpanByteMinimalKVStore();
            Parallel.For(0, READ_NUM_OPERATIONS, key => _minimalKVStore.Upsert(key, key.ToString()));
        }

        [Benchmark]
        public async Task SpanByteStore_Reads()
        {
            await ReadAll();
        }

        // ***** Helpers ***** 
        private async Task ReadAll()
        {
            for (int key = 0; key < READ_NUM_OPERATIONS; key++)
            {
                _readTasks.Add(ReadAsync(key));
            }
            await Task.WhenAll(_readTasks).ConfigureAwait(false);
        }

        private async Task<(Status, string?)> ReadAsync(int key)
        {
            await Task.Yield();

            return await _minimalKVStore.ReadAsync(key).ConfigureAwait(false);
        }

        [IterationSetup(Targets = new string[] { nameof(ObjLogStore_Reads), nameof(MemoryStore_Reads), nameof(SpanByteStore_Reads) })]
        public void IterationSetup()
        {
            _readTasks.Clear();
        }

        [GlobalCleanup(Targets = new string[] { nameof(ObjLogStore_Reads), nameof(MemoryStore_Reads), nameof(SpanByteStore_Reads) })]
        public void GlobalCleanup()
        {
            _minimalKVStore.Dispose();
        }
    }
}
