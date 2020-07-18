using LinqDb;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.tables;


namespace Testing.basic_tests
{
#if (!SOCKETS && !SERVER && !SAMEDB && !INDEXES)
    class LowLevelDelete : ITest
    {
        public void Do(Db db)
        {
            bool dispose = false; if (db == null) { db = new Db("DATA"); dispose = true; }
            
            db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));

            var d = new SomeData()
            {
                Id = 1,
                Normalized = 1.2,
                PeriodId = 5,
                NameSearch = "1"
            };
            db.Table<SomeData>().Save(d);
            var d1 = new SomeData()
            {
                Id = 2,
                Normalized = 2.3,
                PeriodId = 10,
                NameSearch = "2"
            };
            db.Table<SomeData>().Save(d1);
            var d2 = new SomeData()
            {
                Id = 3,
                Normalized = 4.5,
                PeriodId = 15,
                NameSearch = "3"
            };
            db.Table<SomeData>().Save(d2);
            db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));

            var b = new BinaryData()
            {
                Id = 1,
                Data = new byte[3] { (byte)1, (byte)2, (byte)3 }
            };
            db.Table<BinaryData>().Save(b);
            var b1 = new BinaryData()
            {
                Id = 2,
                Data = new byte[3] { (byte)1, (byte)2, (byte)3 }
            };
            db.Table<BinaryData>().Save(b1);
            var b2 = new BinaryData()
            {
                Id = 3
            };
            db.Table<BinaryData>().Save(b2);
            db.Table<BinaryData>().Delete(new HashSet<int>(db.Table<BinaryData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));


            var level_db = db._internal_data_db;

            var keys = new List<byte[]>();
            var values = new List<byte[]>();
            using (var it = level_db.NewIterator())
            {
                it.SeekToFirst();
                while (it.Valid())
                {
                    keys.Add(it.Key());
                    values.Add(it.Value());
                    it.Next();
                }
            }

            if (keys.Any(f => f[0] != (byte)99 && f[0] != (byte)100) ||
                values.Any(f => f.Any(z => z != 0)) ||
                keys.Any(f => f.Length > 3))
            {
                throw new Exception("Assert failure");
            }


            var list = new List<SomeData>() { d, d1, d2 };
            db.Table<SomeData>().SaveBatch(list);
            db.Table<SomeData>().Delete(1);
            db.Table<SomeData>().Delete(2);
            db.Table<SomeData>().Delete(3);


            keys = new List<byte[]>();
            values = new List<byte[]>();
            using (var it = level_db.NewIterator())
            {
                it.SeekToFirst();
                while (it.Valid())
                {
                    keys.Add(it.Key());
                    values.Add(it.Value());
                    it.Next();
                }
            }

            if (keys.Any(f => f[0] != (byte)99 && f[0] != (byte)100) ||
                values.Any(f => f.Any(z => z != 0)) ||
                keys.Any(f => f.Length > 3))
            {
                throw new Exception("Assert failure");
            }


            list = new List<SomeData>() { d, d1, d2 };
            db.Table<SomeData>().SaveBatch(list);

            d.NameSearch = "abc abc1";
            d1.NameSearch = "abc abc2";
            d2.NameSearch = "abc abc3";
            db.Table<SomeData>().SaveBatch(list);

            var r = db.Table<SomeData>().Search(f => f.NameSearch, "abc").SelectEntity();
            if (r.Count() != 3)
            {
                throw new Exception("Assert failure");
            }
            db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            keys = new List<byte[]>();
            values = new List<byte[]>();
            using (var it = level_db.NewIterator())
            {
                it.SeekToFirst();
                while (it.Valid())
                {
                    keys.Add(it.Key());
                    values.Add(it.Value());
                    it.Next();
                }
            }

            if (keys.Any(f => f[0] != (byte)99 && f[0] != (byte)100) ||
                values.Any(f => f.Any(z => z != 0)) ||
                keys.Any(f => f.Length > 3))
            {
                throw new Exception("Assert failure");
            }

            if (dispose) { db.Dispose(); }
            if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
#endif
}
