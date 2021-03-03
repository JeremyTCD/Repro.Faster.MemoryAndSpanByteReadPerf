using BenchmarkDotNet.Running;
using FASTER.core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Repro.Faster.MemoryAndSpanByteReadPerf
{
    public class Program
    {
        private static IMinimalKVStore _minimalKVStore;
        private const int READ_NUM_OPERATIONS = 10_000;
        private static List<Task> _readTasks = new();

        public static async Task Main()
        {
            await MemoryDiagnostics();

            //await RunBenchmarks();
        }

        private static async Task MemoryDiagnostics()
        {
            _minimalKVStore = new SpanByteMinimalKVStore();

            // Insert
            Parallel.For(0, READ_NUM_OPERATIONS, key => _minimalKVStore.Upsert(key, key.ToString()));

            // Create Session instances
            await ReadAll();

            // Test
            await ReadAll();
            await ReadAll();
            await ReadAll();
        }

        private static async Task ReadAll()
        {
            // Clear old tasks
            foreach (Task task in _readTasks)
            {
                task.Dispose();
            }
            _readTasks = new();

            // Read
            for (int key = 0; key < READ_NUM_OPERATIONS; key++)
            {
                _readTasks.Add(ReadAsync(key));
            }
            await Task.WhenAll(_readTasks).ConfigureAwait(false);
        }

        private static async Task<(Status, string?)> ReadAsync(int key)
        {
            await Task.Yield();

            return await _minimalKVStore.ReadAsync(key).ConfigureAwait(false);
        }

        private static async Task RunBenchmarks()
        {
            await VerifyStoreWorks(new ObjLogMinimalKVStore());
            await VerifyStoreWorks(new MemoryMinimalKVStore());
            await VerifyStoreWorks(new SpanByteMinimalKVStore());

            BenchmarkRunner.Run<Benchmarks>();
        }

        private static async Task VerifyStoreWorks(IMinimalKVStore minimalKVStore)
        {
            Parallel.For(0, 10_000, key => minimalKVStore.Upsert(key, key.ToString()));

            List<Task<(Status, string?)>> readTasks = new();
            for (int key = 0; key < 10_000; key++)
            {
                readTasks.Add(ReadAsync(key, minimalKVStore));
            }
            await Task.WhenAll(readTasks).ConfigureAwait(false);
            Parallel.For(0, 10_000, key =>
            {
                (Status status, string? result) = readTasks[key].Result;
                Assert.Equal(Status.OK, status);
                Assert.Equal(key.ToString(), result);
            });

            Console.WriteLine($"{minimalKVStore.GetType().Name} verified");

            minimalKVStore.Dispose();
        }

        private static async Task<(Status, string?)> ReadAsync(int key, IMinimalKVStore objLogMinimalKVStore)
        {
            await Task.Yield();

            return await objLogMinimalKVStore.ReadAsync(key).ConfigureAwait(false);
        }
    }
}
