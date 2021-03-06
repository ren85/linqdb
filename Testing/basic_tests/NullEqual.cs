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
    class NullEqual : ITest
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
                Normalized = null,
                PeriodId = 5,
            };
            db.Table<SomeData>().Save(d);;
            d = new SomeData()
            {
                Id = 2,
                Normalized = 2.3,
                PeriodId = 10,
            };
            db.Table<SomeData>().Save(d);;
            d = new SomeData()
            {
                Id = 3,
                Normalized = null,
                PeriodId = 15,
            };
            db.Table<SomeData>().Save(d);;

            var res = db.Table<SomeData>()
                        .Where(f => f.Normalized == null)
                        .Select(f => new
                        {
                            PeriodId = f.PeriodId,
                            Normalized = f.Normalized
                        });
            if (res.Count() != 2 || res[0].PeriodId + res[1].PeriodId != 20 || res[0].Normalized != null)
            {
                throw new Exception("Assert failure");
            }
#if (SERVER || SOCKETS)
            if(dispose) { if(dispose) { Logic.Dispose(); } }
#else
            if (dispose) { if (dispose) { db.Dispose(); } }
#endif
#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
            if(dispose) { if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); } }
#endif
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
