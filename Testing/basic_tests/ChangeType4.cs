#if (SERVER)
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

namespace Testing.basic_tests
{
#if (!SOCKETS)
    class ChangeType4 : ITest
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

            var d = new Testing.tables5.SomeData()
            {
                Id = 1,
                Normalized = 1.2,
                PeriodId = 5
            };
            db.Table<Testing.tables5.SomeData>().Save(d);
            d = new Testing.tables5.SomeData()
            {
                Id = 2,
                Normalized = 1.3,
                PeriodId = 5
            };
            db.Table<Testing.tables5.SomeData>().Save(d);
            d = new Testing.tables5.SomeData()
            {
                Id = 3,
                Normalized = 1.4,
                PeriodId = 5
            };
            db.Table<Testing.tables5.SomeData>().Save(d);

#if (SERVER)
            db.Table<Testing.tables5.SomeData>()._internal._db._InternalClearCache();
#endif

            var dic = new Dictionary<int, int>() { { 2, 1 } };
            db.Table<Testing.tables.SomeData>().Update(f => f.GroupBy, dic);

            var res = db.Table<Testing.tables.SomeData>()
                        .Where(f => f.Normalized > 1.1 && f.Normalized < 1.35)
                        .SelectEntity();
            if (res.Count() != 2 || res[0].GroupBy != 0 || res[1].GroupBy != 1)
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
