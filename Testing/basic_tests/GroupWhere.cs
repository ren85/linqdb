#if (SERVER || SOCKETS)
using LinqdbClient;
using ServerLogic;
#else
using LinqDb;
#endif
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
    class GroupWhere : ITest
    {
        public void Do(Db db)
        {
            bool dispose = false; if (db == null) { db = new Db("DATA"); dispose = true; }
#if (SERVER)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif
#if (SOCKETS)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
#endif
#if (SOCKETS || SAMEDB || INDEXES || SERVER)
            db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif

            var d = new SomeData()
            {
                Id = 1,
                Normalized = 1.2,
                GroupBy = 5,
                NameSearch = "a"
            };
            db.Table<SomeData>().Save(d);

            d = new SomeData()
            {
                Id = 2,
                Normalized = 7,
                GroupBy = 3,
                NameSearch = "a"
            };
            db.Table<SomeData>().Save(d);
            
            db.Table<SomeData>().CreateGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);

            d = new SomeData()
            {
                Id = 3,
                Normalized = 2.3,
                GroupBy = 10,
                NameSearch = "a"
            };
            db.Table<SomeData>().Save(d);
            d = new SomeData()
            {
                Id = 4,
                Normalized = 4.5,
                GroupBy = 10,
                NameSearch = "b"
            };
            db.Table<SomeData>().Save(d);


            var res = db.Table<SomeData>()
                        .Where(f => f.Normalized > 3)
                        .GroupBy(f => f.GroupBy)
                        .Select(f => new
                        {
                            Key = f.Key,
                            Sum = f.Sum(z => z.Normalized),
                            Total = f.Count()
                        })
                        .ToList();

            if (res.Count() != 2 || res.Where(f => f.Key == 3).First().Total != 1 || res.Where(f => f.Key == 10).First().Total != 1 ||
                res.Where(f => f.Key == 3).First().Sum != 7 || res.Where(f => f.Key == 10).First().Sum != 4.5)
            {
                throw new Exception("Assert failure");
            }

            db.Table<SomeData>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);

#if (SERVER || SOCKETS)
            if (dispose) { Logic.Dispose(); }
#else
            if (dispose) { db.Dispose(); }
#endif
#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
            if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
#endif
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
