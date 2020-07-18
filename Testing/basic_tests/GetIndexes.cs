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
    class GetIndexes : ITest
    {
        public void Do(Db db_)
        {
            bool dispose = true;

            var db = new Db("DATA", "writer_user", "wr1ter123");
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };

#if (SOCKETS || SAMEDB || INDEXES || SERVER)
            db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif

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

            var res = db.GetExistingIndexes();

            if (!res.Contains("SomeData GroupBy2 Normalized"))
            {
                throw new Exception("Assert failure");
            }

            db.Table<SomeData>().CreateGroupByMemoryIndex(f => f.GroupBy2, z => z.Normalized);

            var res2 = db.GetExistingIndexes();

            if (res.Aggregate((a,b) =>a+"|"+b) != res2.Aggregate((a, b) => a + "|" + b))
            {
                throw new Exception("Assert failure");
            }

            db.Table<SomeData>().RemoveGroupByMemoryIndex(f => f.GroupBy2, z => z.Normalized);
            res = db.GetExistingIndexes();
            if (res.Count() != 0)
            {
                throw new Exception("Assert failure");
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