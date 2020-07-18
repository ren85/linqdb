using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public void Replicate(string path)
        {
            CopyTo(path);
        }
        public object _replicate_lock = new object();
        public void CopyTo(string path)
        {
            lock (_replicate_lock)
            {
                if (Directory.Exists(path))
                {
                    throw new LinqDbException("Linqdb: replicate folder already exists: " + path);
                }

                Directory.CreateDirectory(path);

                var options = new DbOptions().SetCreateIfMissing(true);

                using (var new_leveld_db = RocksDb.Open(options, Path.Combine(path, "data")))
                using (var new_struct_db = RocksDb.Open(options, Path.Combine(path, "struct")))
                using (var data_snapshot = leveld_db.CreateSnapshot())
                using (var struct_snapshot = struct_db.CreateSnapshot())
                {
                    var ro = new ReadOptions().SetSnapshot(data_snapshot);
                    using (var it = leveld_db.NewIterator(null, ro))
                    {
                        it.SeekToFirst();
                        while (it.Valid())
                        {
                            new_leveld_db.Put(it.Key(), it.Value());
                            it.Next();
                        }
                    }
                    var struct_ro = new ReadOptions().SetSnapshot(struct_snapshot);
                    using (var it = struct_db.NewIterator(null, struct_ro))
                    {
                        it.SeekToFirst();
                        while (it.Valid())
                        {
                            new_struct_db.Put(it.Key(), it.Value());
                            it.Next();
                        }
                    }
                }
            }
        }
    }
}
