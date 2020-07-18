﻿#if (SERVER || SOCKETS)
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
    class IntersectDates : ITest
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
            var now = DateTime.Now;

            var d = new SomeData()
            {
                Id = 2,
                Normalized = 0.9,
                PeriodId = 7,
                Date = now
            };
            db.Table<SomeData>().Save(d);;
            d = new SomeData()
            {
                Id = 3,
                Normalized = 0.5,
                PeriodId = 10
            };
            db.Table<SomeData>().Save(d);;
            d = new SomeData()
            {
                Id = 4,
                Normalized = -4.5,
                PeriodId = 15
            };
            db.Table<SomeData>().Save(d);;

            var res = db.Table<SomeData>()
                        .Intersect(f => f.Date, new HashSet<DateTime?>() { null, now })
                        .OrderBy(f => f.PeriodId)
                        .Select(f => new
                        {
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 3 || res[0].PeriodId != 7 || res[1].PeriodId != 10 || res[2].PeriodId != 15)
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