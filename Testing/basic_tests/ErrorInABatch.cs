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
    class ErrorInABatch : ITest
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
            var data = Enumerable.Range(1, 10000).Select(f => new SomeData()
            {
                Id = f,
                Value = f
            }).ToList();
            var bad = Enumerable.Range(1, 1000).Select(f => new SomeData()
            {
                Value = double.NaN
            }).ToList();
            data.AddRange(bad);

            data = data.OrderBy(a => Guid.NewGuid()).ToList();
            int count = 0;
            Parallel.ForEach(data, f =>
            {
                try
                {
                    db.Table<SomeData>().Save(f);
                }
                catch (Exception ex)
                {
                    if (ex.Message.Contains("This error could belong to another entity which happened"))
                    {
                        count++;
                    }
                } 
            });

            if (count == 0)
            {
                throw new Exception("Assert failure");
            }
            
            
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
