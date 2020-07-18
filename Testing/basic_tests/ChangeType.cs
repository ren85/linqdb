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
    class ChangeType : ITest
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

            var d = new Testing.tables.SomeData()
            {
                Normalized = 1.2,
                PeriodId = 5
            };
            db.Table<Testing.tables.SomeData>().Save(d);

            var res = db.Table<Testing.tables.SomeData>()
                        .Intersect(f => f.Id, new List<int>() { d.Id })
                        .Select(f => new
                        {
                            Id = f.Id,
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 1 || res[0].Id != 1 || res[0].PeriodId != 5)
            {
                throw new Exception("Assert failure");
            }

            var d_new = new Testing.tables2.SomeData()
            {
                Normalized = 1.2,
                PeriodId = 5
            };

            try
            {
#if (SERVER)
                db.Table<Testing.tables.SomeType>()._internal._db._InternalClearCache();
#endif
                db.Table<Testing.tables2.SomeData>().Save(d_new);
                throw new Exception("Assert failure");
            }
            catch (Exception e)
            {
                if (!e.Message.Contains("Linqdb: Column type cannot be changed:"))
                {
                    throw new Exception("Assert failure");
                }
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