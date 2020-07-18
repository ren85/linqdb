using RocksDbSharp;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public RocksDb leveld_db = null;
        object _lock_ = new object();

        RocksDb struct_db = null;
        object _lock_struct_ = new object();

        string PathToDb { get; set; }
        public Ldb(string path, bool delete_indexes)
        {
            PathToDb = path;
            if (string.IsNullOrEmpty(PathToDb))
            {
                throw new LinqDbException("LinqDb path is empty");
            }
            if (leveld_db == null)
            {
                lock (_lock_)
                {
                    if (leveld_db == null)
                    {
                        if (!Directory.Exists(Path.Combine(PathToDb, "data")))
                        {
                            Directory.CreateDirectory(Path.Combine(PathToDb, "data"));
                        }

                        var options = new DbOptions().SetCreateIfMissing(true).SetBloomLocality(10);
                        leveld_db = RocksDb.Open(options, Path.Combine(PathToDb, "data"));
                        
                    }
                }
            }

            if (struct_db == null)
            {
                lock (_lock_struct_)
                {
                    if (struct_db == null)
                    {
                        if (!Directory.Exists(Path.Combine(PathToDb, "struct")))
                        {
                            Directory.CreateDirectory(Path.Combine(PathToDb, "struct"));
                        }

                        var options = new DbOptions().SetCreateIfMissing(true).SetBloomLocality(10);
                        struct_db = RocksDb.Open(options, Path.Combine(PathToDb, "struct"));
                    }
                    if (delete_indexes)
                    {
                        RemoveAllFileIndexes();
                    }
                }

                struct_cache = new ConcurrentDictionary<string, TableInfo>();
            }
        }


        bool is_disposed { get; set; }
        public void Dispose()
        {
            is_disposed = true;
            if (leveld_db != null)
            {
                leveld_db.Dispose();
                leveld_db = null;
            }
            if (struct_db != null)
            {
                struct_db.Dispose();
                struct_db = null;
            }
        }
    }


    public class LinqDbException : Exception
    {
        public LinqDbException() : base()
        {
        }

        public LinqDbException(string message) : base(message)
        {
        }
    }
}
