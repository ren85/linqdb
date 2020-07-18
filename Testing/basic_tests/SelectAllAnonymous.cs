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
    class SelectAllAnonymous : ITest
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
            db.Table<Small>().Delete(new HashSet<int>(db.Table<Small>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.Table<Small2>().Delete(new HashSet<int>(db.Table<Small2>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
            var d = new Small()
            {
                Id = 1,
                Some = 5
            };
            db.Table<Small>().Save(d);

            var res = db.Table<Small>()
                        .Select(f => new
                        {
                            f.Id,
                            f.Some
                        });
            if (res.Count() != 1 || res[0].Id != 1 || res[0].Some != 5)
            {
                throw new Exception("Assert failure");
            }

            var d2 = new Small2()
            {
                Id = 1,
                A = 5
            };
            db.Table<Small2>().Save(d2);

            var res2 = db.Table<Small2>()
                        .Select(f => new
                        {
                            f.Id,
                            f.A
                        });
            if (res2.Count() != 1 || res2[0].Id != 1 || res2[0].A != 5)
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
