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
    class NewField : ITest
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

            var d = new Testing.tables4.SomeType()
            {
                Id = 1,
                Value = 1,
                Name = "1"
            };
            db.Table<Testing.tables4.SomeType>().Save(d);
            var d2 = new Testing.tables.SomeType()
            {
                Id = 2,
                Value = 2,
                PeriodId = 2,
                Name = "2"
            };
#if (SERVER)
            db.Table<Testing.tables.SomeType>()._internal._db._InternalClearCache();
#endif
            db.Table<Testing.tables.SomeType>().Save(d2);
            var res = db.Table<Testing.tables.SomeType>().Where(f => f.Id == 1).Select(f => new { f.PeriodId });
            if (res.Count() != 1 || res[0].PeriodId != 0)
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
