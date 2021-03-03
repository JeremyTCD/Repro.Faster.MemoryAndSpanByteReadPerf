using FASTER.core;
using MessagePack;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO;
using System.Threading.Tasks;

namespace Repro.Faster.MemoryAndSpanByteReadPerf
{
    public class MemoryMinimalKVStore : IMinimalKVStore
    {
        private readonly MemoryFunctions<int, byte, Empty> _memoryFunctions = new();
        private readonly ConcurrentQueue<ClientSession<int, Memory<byte>, Memory<byte>, (IMemoryOwner<byte>, int), Empty, MemoryFunctions<int, byte, Empty>>> _sessionPool = new();
        private readonly FasterKV<int, Memory<byte>> _kvStore;
        private readonly FasterKV<int, Memory<byte>>.ClientSessionBuilder<Memory<byte>, (IMemoryOwner<byte>, int), Empty> _clientSessionBuilder;

        public int NumSessions => _sessionPool.Count;

        public MemoryMinimalKVStore()
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
            _clientSessionBuilder = _kvStore.For(_memoryFunctions);
        }

        public void Upsert(int key, string value)
        {
            var session = GetPooledSession();

            byte[] bytes = MessagePackSerializer.Serialize(value);
            session.Upsert(key, new Memory<byte>(bytes));

            _sessionPool.Enqueue(session);
        }

        public async Task<(Status, string?)> ReadAsync(int key)
        {
            var session = GetPooledSession();

            (Status status, (IMemoryOwner<byte>, int) result) = (await session.ReadAsync(key).ConfigureAwait(false)).Complete();

            _sessionPool.Enqueue(session);

            using IMemoryOwner<byte> memoryOwner = result.Item1;

            return (status, status != Status.OK ? default : MessagePackSerializer.Deserialize<string>(memoryOwner.Memory));
        }

        public void Dispose()
        {
            foreach (var session in _sessionPool)
            {
                session.Dispose();
            }

            _kvStore.Dispose();
        }

        private ClientSession<int, Memory<byte>, Memory<byte>, (IMemoryOwner<byte>, int), Empty, MemoryFunctions<int, byte, Empty>> GetPooledSession()
        {
            if (_sessionPool.TryDequeue(out ClientSession<int, Memory<byte>, Memory<byte>, (IMemoryOwner<byte>, int), Empty, MemoryFunctions<int, byte, Empty>>? result))
            {
                return result;
            }

            return _clientSessionBuilder.NewSession<MemoryFunctions<int, byte, Empty>>();
        }
    }
}
