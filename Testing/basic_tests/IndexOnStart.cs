#if (SERVER)
using LinqdbClient;
using ServerLogic;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
    class IndexOnStart : ITest
    {
        public void Do(Db db_)
        {
            bool dispose = true;
            
            var db = new Db("DATA", "writer_user", "wr1ter123");
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };

            db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));

            var d = new SomeData()
            {
                Id = 1,
                Normalized = 1.2,
                GroupBy2 = 5
            };
            db.Table<SomeData>().Save(d);
            d = new SomeData()
            {
                Id = 2,
                Normalized = 2.2,
                GroupBy2 = 5
            };
            db.Table<SomeData>().Save(d);

            db.Table<SomeData>().CreateGroupByMemoryIndex(f => f.GroupBy2, z => z.Normalized);
            
            var res = db.Table<SomeData>().GroupBy(f => f.GroupBy2).Select(f => new { f.Key, Sum = f.Sum(z => z.Normalized) });

            if (res.Count() != 1 || res[0].Key != 5 || Math.Round((double)res[0].Sum, 2) != 3.4)
            {
                throw new Exception("Assert failure");
            }

            Logic.Dispose();
            SocketTesting.TestDispose();

            db = new Db("DATA", "writer_user", "wr1ter123");
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };

            d = new SomeData()
            {
                Id = 1,
                Normalized = 1.2,
                GroupBy2 = 5
            };
            db.Table<SomeData>().Save(d);
            d = new SomeData()
            {
                Id = 2,
                Normalized = 2.2,
                GroupBy2 = 5
            };
            db.Table<SomeData>().Save(d);

            res = db.Table<SomeData>().GroupBy(f => f.GroupBy2).Select(f => new { f.Key, Sum = f.Sum(z => z.Normalized) });

            if (res.Count() != 1 || res[0].Key != 5 || Math.Round((double)res[0].Sum, 2) != 3.4)
            {
                throw new Exception("Assert failure");
            }

            db.Table<SomeData>().RemoveGroupByMemoryIndex(f => f.GroupBy2, z => z.Normalized);

            Logic.Dispose();
            SocketTesting.TestDispose();

            db = new Db("DATA", "writer_user", "wr1ter123");
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };

            d = new SomeData()
            {
                Id = 1,
                Normalized = 1.2,
                GroupBy2 = 5
            };
            db.Table<SomeData>().Save(d);
            d = new SomeData()
            {
                Id = 2,
                Normalized = 2.2,
                GroupBy2 = 5
            };
            db.Table<SomeData>().Save(d);

            try
            {
                res = db.Table<SomeData>().GroupBy(f => f.GroupBy2).Select(f => new { f.Key, Sum = f.Sum(z => z.Normalized) });
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("does not exist"))
                {
                    throw new Exception("Assert failure");
                }
            }

            db.Table<SomeData>().RemoveGroupByMemoryIndex(f => f.GroupBy2, z => z.Normalized);

            Logic.Dispose();
            SocketTesting.TestDispose();

            if (dispose)
            {
                ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA");
            }
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
#endif