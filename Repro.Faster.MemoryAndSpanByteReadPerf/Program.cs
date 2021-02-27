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
        public static async Task Main(string[] args)
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
