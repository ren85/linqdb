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
using Testing.basic_tests.diff;
using Testing.tables;

namespace Testing.basic_tests
{
    class DiffNamespace : ITest
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
            db.Table<Testing.tables.SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.Table<Testing.tables.diff.SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif

            db.Table<SomeData>().Save(new SomeData()
            {
                Date = DateTime.Now,
            });

            var res = db.Table<SomeData>().SelectEntity();
            var res2 = db.Table<Testing.tables.diff.SomeData>().SelectEntity();


            var res3 = db.Table<SomeData>().Select(f => new { f.Id }).ToList();
            (new DiffTest()).Do(db);

            var res4 = db.Table<SomeData>().Select(f => new { f.Id }).ToList();
            (new Testing2.DiffTest()).Do(db);
#if (SERVER || SOCKETS)
            if(dispose) { Logic.Dispose(); }
#else
            if (dispose) { db.Dispose(); }
#endif
#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
            if (dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
#endif
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
