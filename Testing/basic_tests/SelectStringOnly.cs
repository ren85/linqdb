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
    class SelectStringOnly : ITest
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
                PeriodId = 5,
                NameSearch = "5.175.13.1",
            };
            db.Table<SomeData>().Save(d);
            d = new SomeData()
            {
                Id = 2,
                Normalized = 2.3,
                PeriodId = 10,
                NameSearch = "5.175.13.2"
            };
            db.Table<SomeData>().Save(d);
            d = new SomeData()
            {
                Id = 3,
                Normalized = 4.5,
                PeriodId = 15,
                NameSearch = "5.175.13.3"
            };
            db.Table<SomeData>().Save(d);

            var res = db.Table<SomeData>()
                        .Select(f => new
                        {
                            f.NameSearch,
                            f.Id
                        });
            if (res.Count() != 3 || res[0].NameSearch == null || !res[0].NameSearch.StartsWith("5.175.13") || res[1].NameSearch == null || !res[1].NameSearch.StartsWith("5.175.13") || res[2].NameSearch == null || !res[2].NameSearch.StartsWith("5.175.13"))
            {
                throw new Exception("Assert failure");
            }

            var res2 = db.Table<SomeData>()
                        .Select(f => new
                        {
                            f.NameSearch,
                            f.Id,
                            f.Normalized
                        });
            if (res2.Count() != 3 || res2[0].Normalized == null || res2[1].Normalized == null || res2[2].Normalized == null)
            {
                throw new Exception("Assert failure");
            }



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
