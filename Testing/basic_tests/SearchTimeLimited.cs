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
    class SearchTimeLimited : ITest
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
            db.Table<SomeData>().DeleteNonAtomically(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
            try
            {
                var d = new SomeData()
                {
                    Id = 1,
                    Normalized = 1.2,
                    PeriodId = 5,
                    NameSearch = "test 123 abc"
                };
                db.Table<SomeData>().Save(d);
                d = new SomeData()
                {
                    Id = 2,
                    Normalized = 0.9,
                    PeriodId = 7,
                    NameSearch = "test"
                };
                db.Table<SomeData>().Save(d);
                d = new SomeData()
                {
                    Id = 3,
                    Normalized = 0.5,
                    PeriodId = 10
                };
                db.Table<SomeData>().Save(d);


                var statistics = new LinqdbSelectStatistics();
                var res = db.Table<SomeData>()
                            .SearchTimeLimited(f => f.NameSearch, "test", 1000)
                            .OrderBy(f => f.Id)
                            .Select(f => new
                            {
                                Id = f.Id,
                                PeriodId = f.PeriodId
                            }, statistics);
                if (res.Count() != 2 || res[0].Id != 1 || res[1].Id != 2)
                {
                    throw new Exception("Assert failure");
                }
                if ((int)statistics.Total != res.Count())
                {
                    throw new Exception("Assert failure");
                }


                db.Table<SomeData>().SaveNonAtomically(Enumerable.Range(0, 1000000).Select(f => new SomeData()
                {
                    NameSearch = "test 123 abc " + f
                }).ToList());

                res = db.Table<SomeData>()
                        .SearchTimeLimited(f => f.NameSearch, "test", 10)
                        .OrderBy(f => f.Id)
                        .Select(f => new
                        {
                            Id = f.Id,
                            PeriodId = f.PeriodId
                        }, statistics);

                if ((int)statistics.SearchedPercentile > 90 || res.Count > 900000)
                {
                    throw new Exception("Assert failure");
                }
            }
            finally 
            {
                db.Table<SomeData>().DeleteNonAtomically(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
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
