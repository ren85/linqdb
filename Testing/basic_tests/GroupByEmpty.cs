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
    class GroupByEmpty : ITest
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


            db.Table<SomeData>().CreateGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);

           
            var res = db.Table<SomeData>()
                        .Where(f => f.Normalized > 3)
                        .GroupBy(f => f.GroupBy)
                        .Select(f => new
                        {
                            Key = f.Key,
                            Sum = f.Sum(z => z.Normalized),
                            Distinct = f.CountDistinct(z => z.Normalized),
                            Max = f.Max(z => z.Normalized),
                            Min = f.Min(z => z.Normalized),
                            Avg = f.Average(z => z.Normalized),
                            Total = f.Count()
                        })
                        .ToList();


            db.Table<SomeData>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);

#if (SERVER || SOCKETS)
            if (dispose) { Logic.Dispose(); }
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
