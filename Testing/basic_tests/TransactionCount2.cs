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
    class TransactionCount2 : ITest
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

            var a = new SomeData()
            {
                Id = 1,
                Normalized = 1.2,
                PeriodId = 5,
                NameSearch = "test"
            };
            db.Table<SomeData>().Save(a);

            using (var tran = new LinqdbTransaction())
            {
                var d = new SomeData()
                {
                    Id = 2,
                    Normalized = 1.2,
                    PeriodId = 5,
                    NameSearch = "test"
                };
                db.Table<SomeData>(tran).Save(d);

                d = new SomeData()
                {
                    Id = 3,
                    Normalized = 1.2,
                    PeriodId = 5,
                    NameSearch = "test"
                };
                db.Table<SomeData>(tran).Save(d);

                db.Table<SomeData>(tran).Update(f => f.NameSearch, 1, "hohoho");

                tran.Commit();
            }

            var count = db.Table<SomeData>().Count();

            if (count != 3)
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
