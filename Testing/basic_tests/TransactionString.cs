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
    class TransactionString : ITest
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
            using (var transaction = new LinqdbTransaction())
            {
                var d = new SomeData()
                {
                    NameSearch = "test"
                };
                db.Table<SomeData>(transaction).Save(d);

                d = new SomeData()
                {
                    NameSearch = "test"
                };
                db.Table<SomeData>(transaction).Save(d);
                transaction.Commit();
            }
            var res = db.Table<SomeData>().Search(f => f.NameSearch, "test").SelectEntity();
            if (res.Count() != 2)
            {
                throw new Exception("Assert failure");
            }
            if (db.Table<SomeData>().Count() != 2)
            {
                throw new Exception("Assert failure");
            }

            var dic = new Dictionary<int, string>() { { res[0].Id, "updated" }, { res[1].Id, "updated" } };
            using (var transaction = new LinqdbTransaction())
            {
                db.Table<SomeData>(transaction).Update(f => f.NameSearch, dic);
                transaction.Commit();
            }
            res = db.Table<SomeData>().Search(f => f.NameSearch, "test").SelectEntity();
            if (res.Count() != 0)
            {
                throw new Exception("Assert failure");
            }
            res = db.Table<SomeData>().Search(f => f.NameSearch, "updated").SelectEntity();
            if (res.Count() != 2)
            {
                throw new Exception("Assert failure");
            }
            if (db.Table<SomeData>().Count() != 2)
            {
                throw new Exception("Assert failure");
            }

            using (var transaction = new LinqdbTransaction())
            {
                db.Table<SomeData>(transaction).Delete(new HashSet<int>(new int[2] { res[0].Id, res[1].Id }));
                transaction.Commit();
            }
            res = db.Table<SomeData>().Search(f => f.NameSearch, "test").SelectEntity();
            if (res.Count() != 0)
            {
                throw new Exception("Assert failure");
            }
            res = db.Table<SomeData>().Search(f => f.NameSearch, "updated").SelectEntity();
            if (res.Count() != 0)
            {
                throw new Exception("Assert failure");
            }
            res = db.Table<SomeData>().SelectEntity();
            if (res.Count() != 0)
            {
                throw new Exception("Assert failure");
            }
            if (db.Table<SomeData>().Count() != 0)
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

