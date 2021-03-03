using FASTER.core;
using MessagePack;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading.Tasks;

namespace Repro.Faster.MemoryAndSpanByteReadPerf
{
    public class ObjLogMinimalKVStore : IMinimalKVStore
    {
        private readonly SimpleFunctions<int, string, Empty> _simpleFunctions = new();
        private readonly ConcurrentQueue<ClientSession<int, string, string, string, Empty, SimpleFunctions<int, string, Empty>>> _sessionPool = new();
        private readonly FasterKV<int, string> _kvStore;
        private readonly FasterKV<int, string>.ClientSessionBuilder<string, string, Empty> _clientSessionBuilder;

        public int NumSessions => _sessionPool.Count;

        public ObjLogMinimalKVStore()
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
            var serializerSettings = new SerializerSettings<int, string>()
            {
                valueSerializer = () => new ObjLogValueSerializer()
            };

            // Create store
            _kvStore = new(1L << 20, logSettings, serializerSettings: serializerSettings);
            _clientSessionBuilder = _kvStore.For(_simpleFunctions);
        }

        public void Upsert(int key, string value)
        {
            var session = GetPooledSession();
            session.Upsert(key, value);
            _sessionPool.Enqueue(session);
        }

        public async Task<(Status, string?)> ReadAsync(int key)
        {
            var session = GetPooledSession();
            (Status, string) result = (await session.ReadAsync(key).ConfigureAwait(false)).Complete();
            _sessionPool.Enqueue(session);

            return result;
        }

        private ClientSession<int, string, string, string, Empty, SimpleFunctions<int, string, Empty>> GetPooledSession()
        {
            if (_sessionPool.TryDequeue(out ClientSession<int, string, string, string, Empty, SimpleFunctions<int, string, Empty>>? result))
            {
                return result;
            }

            return _clientSessionBuilder.NewSession<SimpleFunctions<int, string, Empty>>();
        }

        public void Dispose()
        {
            Console.WriteLine($"Number of sessions created: {_sessionPool.Count})");

            foreach (var session in _sessionPool)
            {
                session.Dispose();
            }

            _kvStore.Dispose();
        }

        public class ObjLogValueSerializer : BinaryObjectSerializer<string>
        {
            public override void Deserialize(out string obj)
            {
                obj = MessagePackSerializer.Deserialize<string>(reader.BaseStream);
            }

            public override void Serialize(ref string obj)
            {
                MessagePackSerializer.Serialize(writer.BaseStream, obj);
            }
        }
    }
}
