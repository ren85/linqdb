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
#if (!SOCKETS)
    class UpdateWithZero : ITest
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
                Normalized = -1.2,
                PeriodId = 5
            };
            db.Table<SomeData>().Save(d);
            d = new SomeData()
            {
                Id = 2,
                Normalized = -0.9,
                PeriodId = 7
            };
            db.Table<SomeData>().Save(d);
#if (SERVER)
            db.Table<Testing.tables.SomeType>()._internal._db._InternalClearCache();
#endif
            var count = db.Table<Testing.tables4.SomeData>().Count();

            var dic = new Dictionary<int, double>() { { 1, 0 }, { 2, 0 } };
            db.Table<Testing.tables4.SomeData>().Update(f => f.SomeDouble, dic);

            var res = db.Table<Testing.tables4.SomeData>().SelectEntity();
            if (res.Count() != 2 || res[0].Value != 0 || res[1].Value != 0)
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
#endif
        }
