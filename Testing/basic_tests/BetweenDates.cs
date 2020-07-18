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
    class Betweens : ITest
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
                Date = Convert.ToDateTime("2015-12-15 13:30")
            };
            db.Table<SomeData>().Save(d); ;
            d = new SomeData()
            {
                Id = 2,
                Normalized = 2.3,
                PeriodId = 10,
                Date = Convert.ToDateTime("2015-12-14 14:42")
            };
            db.Table<SomeData>().Save(d); ;
            d = new SomeData()
            {
                Id = 3,
                Normalized = 4.5,
                PeriodId = 15,
                Date = Convert.ToDateTime("2015-12-15 15:42")
            };
            db.Table<SomeData>().Save(d);

            var res = db.Table<SomeData>()
                        .Between(f => f.Date, Convert.ToDateTime("2015-12-15 15:00"), Convert.ToDateTime("2015-12-15 16:00"))
                        .Select(f => new
                        {
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 1 || res[0].PeriodId != 15)
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
