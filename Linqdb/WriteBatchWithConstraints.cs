using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public class WriteBatchWithConstraints : IDisposable
    {
        public WriteBatch _writeBatch = new WriteBatch();
        long KeysCount { get; set; }
        long MaxKeys = 1500000;
        public void Put(byte[] key, byte[] value)
        {
            KeysCount++;
            if (KeysCount > MaxKeys)
            {
                throw new LinqDbException("Linqdb: modification batch is too large.");
            }
            _writeBatch.Put(key, value);
        }
        public void Delete(byte[] key)
        {
            KeysCount++;
            if (KeysCount > MaxKeys)
            {
                throw new LinqDbException("Linqdb: modification batch is too large.");
            }
            _writeBatch.Delete(key);
        }
        public void Dispose()
        {
            _writeBatch.Dispose();
        }
    }
}
