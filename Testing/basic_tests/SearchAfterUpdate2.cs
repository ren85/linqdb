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
    class SearchAfterUpdate2 : ITest
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
            db.Table<LogEntry>().Delete(new HashSet<int>(db.Table<LogEntry>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif

            var d = new LogEntry()
            {
                Id = 1,
                DataSearch = @"var list ="
            };
            db.Table<LogEntry>().Save(d);

            var list = new List<LogEntry>();
            var le1 = new LogEntry()
            {
                Id = 1,
                DataSearch = @"template >"
            };
            list.Add(le1);
            var le2 = new LogEntry()
            {
                Id = 2,
                DataSearch = @"var list ="
            };
            list.Add(le2);
            db.Table<LogEntry>().SaveBatch(list);

            var res = db.Table<LogEntry>()
                        .Search(f => f.DataSearch, "var list =")
                        .Select(f => new
                        {
                            f.Id,
                            f.DataSearch
                        });
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
