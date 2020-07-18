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
    class TransactionIds : ITest
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
            int id1 = 0;
            int id2 = 0;
            using (var transaction = new LinqdbTransaction())
            {
                var d = new SomeData()
                {
                    NameSearch = "test",
                    Normalized = 1
                };
                db.Table<SomeData>(transaction).Save(d);
                if (d.Id == 0)
                {
                    throw new Exception("Assert failure");
                }
                id1 = d.Id;

                d = new SomeData()
                {
                    NameSearch = "test",
                    Normalized = 1
                };
                db.Table<SomeData>(transaction).Save(d);
                if (d.Id == 0)
                {
                    throw new Exception("Assert failure");
                }
                id2 = d.Id;
                if (id1 == id2)
                {
                    throw new Exception("Assert failure");
                }
                transaction.Commit();
            }

            var res = db.Table<SomeData>().Where(f => f.Id == id1).SelectEntity();
            if (res.Count() != 1)
            {
                throw new Exception("Assert failure");
            }
            res = db.Table<SomeData>().Where(f => f.Id == id2).SelectEntity();
            if (res.Count() != 1)
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