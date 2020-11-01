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
    class Total : ITest
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
                PeriodId = 5
            };
            db.Table<SomeData>().Save(d);
            d = new SomeData()
            {
                Id = 2,
                Normalized = 2.3,
                PeriodId = 10
            };
            db.Table<SomeData>().Save(d);
            d = new SomeData()
            {
                Id = 3,
                Normalized = 4.5,
                PeriodId = 15
            };
            db.Table<SomeData>().Save(d);

            var statistics = new LinqdbSelectStatistics();
            var res = db.Table<SomeData>()
                        .Where(f => f.Normalized > 2 && f.Normalized < 3)
                        .OrderBy(f => f.Date)
                        .Select(f => new
                        {
                            PeriodId = f.PeriodId
                        }, statistics);
            if (res.Count() != statistics.Total)
            {
                throw new Exception("Assert failure");
            }

            statistics = new LinqdbSelectStatistics();
            res = db.Table<SomeData>()
                        .OrderBy(f => f.Date)
                        .Skip(1)
                        .Take(1)
                        .Select(f => new
                        {
                            PeriodId = f.PeriodId
                        }, statistics);
            if (statistics.Total != 3)
            {
                throw new Exception("Assert failure");
            }

            statistics = new LinqdbSelectStatistics();
            var res2 = db.Table<SomeData>()
                        .OrderBy(f => f.Date)
                        .Skip(1)
                        .Take(2)
                        .SelectEntity(statistics);
            if (statistics.Total != 3)
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
