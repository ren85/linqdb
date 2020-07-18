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
    class SearchSlices : ITest
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
            for (int i = 1; i < 3100; i++)
            {
                var d = new SomeData()
                {
                    Id = i,
                    Normalized = i,
                    PeriodId = i,
                    NameSearch = "test "+i+" abc"
                };
                db.Table<SomeData>().Save(d);
            }


            var res = db.Table<SomeData>()
                        .Search(f => f.NameSearch, "test")
                        .OrderBy(f => f.Id)
                        .Select(f => new
                        {
                            Id = f.Id,
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != db.Table<SomeData>().Count())
            {
                throw new Exception("Assert failure");
            }
            res = db.Table<SomeData>()
                    .Search(f => f.NameSearch, "test",  0, 1)
                    .OrderBy(f => f.Id)
                    .Select(f => new
                    {
                        Id = f.Id,
                        PeriodId = f.PeriodId
                    });
            if (res.Count() != 999 && res.Any(f => f.Id >= 1000))
            {
                throw new Exception("Assert failure");
            }
            res = db.Table<SomeData>()
                    .Search(f => f.NameSearch, "test", 1, 1)
                    .OrderBy(f => f.Id)
                    .Select(f => new
                    {
                        Id = f.Id,
                        PeriodId = f.PeriodId
                    });
            if (res.Count() != 1000 && res.Any(f => f.Id < 1000 || f.Id >= 2000))
            {
                throw new Exception("Assert failure");
            }
#if (SERVER || SOCKETS)
            if(dispose) { Logic.Dispose(); }
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
