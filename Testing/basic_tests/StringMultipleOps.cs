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
    class StringMultipleOps : ITest
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
            var d = new SomeData()
            {
                Id = 2,
                NameSearch = "test",
                Normalized = 1
            };
            db.Table<SomeData>().Save(d);
            var res = db.Table<SomeData>()
                        .Select(f => new
                        {
                            Id = f.Id,
                            Name = f.NameSearch
                        });
            if (res.Count() != 1 || res[0].Name != "test")
            {
                throw new Exception("Assert failure 1: " + res.Count() + " " + (res.Count() > 0 ? res[0].Name : ""));
            }

            var ids = db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList();
            db.Table<SomeData>().Delete(new HashSet<int>(ids));

            d = new SomeData()
            {
                Id = 1,
                NameSearch = "test2"
            };
            db.Table<SomeData>().Save(d);
            d = new SomeData()
            {
                Id = 2,
                NameSearch = null
            };
            db.Table<SomeData>().Save(d);
            res = db.Table<SomeData>()
                     .Where(f => f.NameSearch != null)
                     .Select(f => new
                     {
                         Id = f.Id,
                         Name = f.NameSearch
                     });
            if (res.Count() != 1 || res[0].Id != 1 || res[0].Name != "test2")
            {
                throw new Exception("Assert failure 2: " + res.Count() + " " + (res.Count() > 0 ? res[0].Name + " " + res[0].Id : ""));
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