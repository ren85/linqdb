#if (SERVER)
using LinqdbClient;
using ServerLogic;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
    class IndexRemove : ITest
    {
        public void Do(Db db)
        {
            bool dispose = false;
            if (db == null)
            {
                db = new Db("DATA");
                dispose = true;
            }
#if (SERVER)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif
#if (SOCKETS)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
#endif

#if (SOCKETS || SAMEDB || INDEXES || SERVER)
            db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif

            var existing = db.GetExistingIndexes();
            db.Table<SomeData>().CreatePropertyMemoryIndex(f => f.PeriodId);

            var sd = new SomeData()
            {
                Id = 1,
                PeriodId = 1,
                Normalized = 1
            };

            db.Table<SomeData>().Save(sd);

            var res = db.Table<SomeData>().Where(f => f.PeriodId == 1).SelectEntity();
            if (res.Count() != 1 || res[0].PeriodId != 1)
            {
                throw new Exception("Assert failure");
            }

            db.Table<SomeData>().RemovePropertyMemoryIndex(f => f.PeriodId);
            res = db.Table<SomeData>().Where(f => f.PeriodId == 1).SelectEntity();
            if (res.Count() != 1 || res[0].PeriodId != 1)
            {
                throw new Exception("Assert failure");
            }

            db.Table<SomeData>().CreateGroupByMemoryIndex(f => f.GroupBy, z => z.PeriodId);
            db.Table<SomeData>().RemoveGroupByMemoryIndex(f => f.GroupBy, f => f.PeriodId);

            try
            {
                var res2 = db.Table<SomeData>()
                            .GroupBy(f => f.GroupBy)
                            .Select(f => new
                            {
                                Key = f.Key,
                                Sum = f.Sum(z => z.PeriodId),
                                Total = f.Count()
                            })
                            .ToList();
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("does not exist."))
                {
                    throw new Exception("Assert failure");
                }
            }


            var existing2 = db.GetExistingIndexes();
            if (existing2.Contains("SomeData PeriodId|"))
            {
                throw new Exception("Assert failure");
            }
            if (existing2.Contains("SomeData GroupBy PeriodId|"))
            {
                throw new Exception("Assert failure");
            }

            if (existing.Contains("SomeData PeriodId|"))
            {
                db.Table<SomeData>().RemovePropertyMemoryIndex(f => f.PeriodId);
            }
            if (existing.Contains("SomeData GroupBy PeriodId|"))
            {
                db.Table<SomeData>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.PeriodId);
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
#endif