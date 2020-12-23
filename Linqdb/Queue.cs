using ServerSharedData;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    class QueueData
    { 
        public int Count { get; set; }
        public List<byte> Data { get; set; }
    }
    public class Queue
    {
        static ConcurrentDictionary<string, QueueData> _data = new ConcurrentDictionary<string, QueueData>();
        static ConcurrentDictionary<string, object> _locks = new ConcurrentDictionary<string, object>();
        static object _lock = new object();
        static int MaxQueueSize = 300;
        public static void PutToQueue(byte[] item, string queueName)
        {
            if (!_data.ContainsKey(queueName))
            {
                lock (_lock)
                {
                    if (!_data.ContainsKey(queueName))
                    {
                        _data[queueName] = new QueueData() { Count = 0, Data = new List<byte>() };
                        _locks[queueName] = new object();
                    }
                }
            }

            lock (_locks[queueName])
            {
                var sb = _data[queueName];
                if (sb.Count + 1 >= MaxQueueSize)
                {
                    throw new LinqDbException("Linqdb: queue's " + queueName + " item count exceed limit of " + MaxQueueSize + ". Add more readers.");
                }
                sb.Count++;
                sb.Data.AddRange(BitConverter.GetBytes(item.Length));
                sb.Data.AddRange(item);
            }
        }

        public static byte[] GetAllFromQueue(string queueName)
        {
            if (!_data.ContainsKey(queueName))
            {
                lock (_lock)
                {
                    if (!_data.ContainsKey(queueName))
                    {
                        _data[queueName] = new QueueData() { Count = 0, Data = new List<byte>() };
                        _locks[queueName] = new object();
                    }
                }
            }

            QueueData data;
            lock (_locks[queueName])
            {
                data = _data[queueName];
                _data[queueName] = new QueueData() { Count = 0, Data = new List<byte>() };
            }
            return data.Data.ToArray();
        }
    }
}
