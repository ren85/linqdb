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
    class UpdateDoubleIndex : ITest
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
                Normalized = -1.2,
                PeriodId = 5,
                NameSearch = "Admin11",
                Date = Convert.ToDateTime("2005-01-01")
            };
            db.Table<SomeData>().Save(d);


            var res = db.Table<SomeData>()
                        .Where(f => f.Normalized == -1.2)
                        .Select(f => new
                        {
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 1 || res[0].PeriodId != 5)
            {
                throw new Exception("Assert failure");
            }

            d.Normalized = null;

            db.Table<SomeData>().Save(d);
            res = db.Table<SomeData>()
                        .Where(f => f.Normalized == -1.2)
                        .Select(f => new
                        {
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 0)
            {
                throw new Exception("Assert failure");
            }

            res = db.Table<SomeData>()
                        .Where(f => f.Normalized == null)
                        .Select(f => new
                        {
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 1 || res[0].PeriodId != 5)
            {
                throw new Exception("Assert failure");
            }

            d.Normalized = -1.2;
            db.Table<SomeData>().Save(d);
            res = db.Table<SomeData>()
                        .Where(f => f.Normalized == null)
                        .Select(f => new
                        {
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 0)
            {
                throw new Exception("Assert failure");
            }

            db.Table<SomeData>().Delete(1);
            res = db.Table<SomeData>()
                        .Where(f => f.Normalized == -1.2 || f.Normalized == null)
                        .Select(f => new
                        {
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 0)
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
