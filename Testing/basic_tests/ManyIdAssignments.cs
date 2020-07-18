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
    class ManyIdAssignments : ITest
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

            var list = new List<SomeData>();
            int total = 150000;
            for (int i = 1; i <= total; i++)
            {
                var d = new SomeData()
                {
                    Normalized = i,
                    PeriodId = i,
                    Date = DateTime.Now.AddDays(i),
                    NameSearch = i+"", 
                    ObjectId = i,
                    PersonId = i,
                    Value = i
                };
                list.Add(d);
            }

            db.Table<SomeData>().SaveBatch(list.Take(50000).ToList());
            db.Table<SomeData>().SaveBatch(list.Skip(50000).Take(50000).ToList());
            db.Table<SomeData>().SaveBatch(list.Skip(100000).ToList());

            var res = db.Table<SomeData>().SelectEntity();
            if (res.Count() != total)
            {
                throw new Exception("Assert failure");
            }

            foreach (var r in list)
            {
                if (r.Id != r.PeriodId)
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
}