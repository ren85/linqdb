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
    class TransactionWriteDelete2 : ITest
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

            var count = db.Table<SomeData>()
                        .Search(f => f.NameSearch, "test")
                        .Count();

            if (count != 0)
            {
                throw new Exception("Assert failure");
            }

            for (int i = 1; i < 1000; i++)
            {
                var d = new SomeData()
                {
                    Id = i,
                    Normalized = 1.2,
                    PeriodId = 5,
                    NameSearch = "test"
                };
                db.Table<SomeData>().Save(d);
            }

            using (var tran = new LinqdbTransaction())
            {

                db.Table<SomeData>(tran).Delete(1);
                var d = new SomeData()
                {
                    Id = 1000,
                    Normalized = 1.2,
                    PeriodId = 5,
                    NameSearch = "test"
                };
                db.Table<SomeData>(tran).Save(d);
                tran.Commit();
            }

            count = db.Table<SomeData>()
                      .Where(f => f.Id > 999)
                      .Search(f => f.NameSearch, "test")
                      .Count();

            if (count != 1)
            {
                throw new Exception("Assert failure");
            }


            using (var tran = new LinqdbTransaction())
            {
                db.Table<SomeData>(tran).Delete(2);
                var d = new SomeData()
                {
                    Id = 1001,
                    Normalized = 1.2,
                    PeriodId = 5,
                    NameSearch = "test"
                };
                db.Table<SomeData>(tran).Save(d);

                tran.Commit();
            }

            count = db.Table<SomeData>()
                        .Where(f => f.Id > 999)
                        .Search(f => f.NameSearch, "test")
                        .Count();

            if (count != 2)
            {
                throw new Exception("Assert failure");
            }

#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
            int last_phase = db.Table<SomeData>().LastStep();
            if (last_phase != 1)
            {
                throw new Exception("Assert failure");
            }
#endif



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
