using FASTER.core;
using MessagePack;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO;
using System.Threading.Tasks;

namespace Repro.Faster.MemoryAndSpanByteReadPerf
{
    public class SpanByteMinimalKVStore : IMinimalKVStore
    {
        private readonly SpanByteFunctions _spanByteFunctions = new();
        private readonly ConcurrentQueue<ClientSession<int, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions>> _sessionPool = new();
        private readonly FasterKV<int, SpanByte> _kvStore;
        private readonly FasterKV<int, SpanByte>.ClientSessionBuilder<SpanByte, SpanByteAndMemory, Empty> _clientSessionBuilder;

        public int NumSessions => _sessionPool.Count;

        public SpanByteMinimalKVStore()
        {
            // Settings
            string logDirectory = Path.Combine(Path.GetTempPath(), "FasterLogs");
            string logFileName = Guid.NewGuid().ToString();
            var logSettings = new LogSettings
            {
                LogDevice = Devices.CreateLogDevice(Path.Combine(logDirectory, $"{logFileName}.log"), deleteOnClose: true),
                ObjectLogDevice = Devices.CreateLogDevice(Path.Combine(logDirectory, $"{logFileName}.obj.log"), deleteOnClose: true),
                PageSizeBits = 12,
                MemorySizeBits = 13
            };

            // Create store
            _kvStore = new(1L << 20, logSettings);
            _clientSessionBuilder = _kvStore.For(_spanByteFunctions);
        }

        public void Upsert(int key, string value)
        {
            var session = GetPooledSession();

            byte[] bytes = MessagePackSerializer.Serialize(value);
            unsafe
            {
                fixed (byte* pointer = bytes)
                {
                    var valueSpanByte = SpanByte.FromPointer(pointer, bytes.Length);
                    session.Upsert(key, valueSpanByte);
                }
            }

            _sessionPool.Enqueue(session);
        }

        public async Task<(Status, string?)> ReadAsync(int key)
        {
            var session = GetPooledSession();

            (Status status, SpanByteAndMemory spanByteAndMemory) = (await session.ReadAsync(key).ConfigureAwait(false)).Complete();

            _sessionPool.Enqueue(session);

            using IMemoryOwner<byte> memoryOwner = spanByteAndMemory.Memory;

            return (status, status != Status.OK ? default : MessagePackSerializer.Deserialize<string>(memoryOwner.Memory));
        }

        private ClientSession<int, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions> GetPooledSession()
        {
            if (_sessionPool.TryDequeue(out ClientSession<int, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions>? result))
            {
                return result;
            }

            return _clientSessionBuilder.NewSession<SpanByteFunctions>();
        }

        public void Dispose()
        {
            foreach(var session in _sessionPool)
            {
                session.Dispose();
            }

            _kvStore.Dispose();
        }

        public class SpanByteFunctions : SpanByteFunctions<int, SpanByteAndMemory, Empty>
        {
            public unsafe override void SingleReader(ref int key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst)
            {
                value.CopyTo(ref dst, MemoryPool<byte>.Shared);
            }

            public unsafe override void ConcurrentReader(ref int key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory dst)
            {
                value.CopyTo(ref dst, MemoryPool<byte>.Shared);
            }
        }
    }
}
