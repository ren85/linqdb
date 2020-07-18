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
    class TypeChangesDelete : ITest
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

            var d = new Testing.tables2.SomeType()
            {
                Name = "test1",
                PeriodId = 1,
            }; 
            db.Table<Testing.tables2.SomeType>().Save(d);

            var res = db.Table<Testing.tables2.SomeType>()
                        .Where(f => f.Id == d.Id)
                        .Select(f => new
                        {
                            Id = f.Id,
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 1 || res[0].Id != 1 || res[0].PeriodId != 1)
            {
                throw new Exception("Assert failure");
            }

#if (SERVER)
            db.Table<Testing.tables.SomeType>()._internal._db._InternalClearCache();
#endif
            //add new double column and delete
            var count = db.Table<Testing.tables.SomeType>().Count();
            if (count != 1)
            {
                throw new Exception("Assert failure");
            }
            db.Table<Testing.tables.SomeType>().Delete(d.Id);

            count = db.Table<Testing.tables.SomeType>().Count();
            if (count != 0)
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
