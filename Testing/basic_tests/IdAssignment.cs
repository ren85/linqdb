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
    class IdAssignment : ITest
    {
        public void Do(Db db_unused)
        {
            if (db_unused != null)
            {
#if (SERVER)
                Logic.Dispose();
#else
                db_unused.Dispose();
#endif
                if (Directory.Exists("DATA"))
                {
                    ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA");
                }
            }

            var db = new Db("DATA");

#if (SERVER)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif

            var d = new SomeData()
            {
                Id = 1,
                Normalized = 1.2,
                PeriodId = 5
            };
            db.Table<SomeData>().Save(d); ;
            d = new SomeData()
            {
                Id = 2,
                Normalized = 2.3,
                PeriodId = 10
            };
            db.Table<SomeData>().Save(d); ;
            d = new SomeData()
            {
                Id = 3,
                Normalized = 4.5,
                PeriodId = 15
            };
            db.Table<SomeData>().Save(d); ;

            var res = db.Table<SomeData>()
                        .Where(f => f.Id == 1)
                        .Select(f => new
                        {
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 1 || res[0].PeriodId != 5)
            {
                throw new Exception("Assert failure");
            }

            d = new SomeData()
            {
                Normalized = 1.2,
                PeriodId = 10
            };
            db.Table<SomeData>().Save(d);
            res = db.Table<SomeData>()
                        .Where(f => f.Id == 1)
                        .Select(f => new
                        {
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 1 || res[0].PeriodId != 5)
            {
                throw new Exception("Assert failure");
            }
            res = db.Table<SomeData>()
                        .Where(f => f.Id == 4)
                        .Select(f => new
                        {
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 1 || res[0].PeriodId != 10)
            {
                throw new Exception("Assert failure");
            }

            d = new SomeData()
            {
                Normalized = 1.2,
                PeriodId = 21
            };
            db.Table<SomeData>().Save(d);
            res = db.Table<SomeData>()
                        .Where(f => f.Id == 5)
                        .Select(f => new
                        {
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 1 || res[0].PeriodId != 21)
            {
                throw new Exception("Assert failure");
            }

#if (SERVER)
            Logic.Dispose();
#else
            db.Dispose();
#endif
            ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA");
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}