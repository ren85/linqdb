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
    class TransParallelWorkload3 : ITest
    {
        public static int errors = 0;
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
            using (var trans = new LinqdbTransaction())
            {
                db.Table<BinaryData>(trans).Delete(new HashSet<int>(db.Table<BinaryData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
                db.Table<SomeData>(trans).Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
                trans.Commit();
            }            
#endif
            var job1 = new TransParallelWorkload();
            var job2 = new TransParallelWorkload2();



            var t1 = Task.Run(() =>
            {
                try
                {
                    job1.Do2(db);
                }
                catch (Exception e)
                {
                    TransParallelWorkload3.errors++;
                }
            });

            var t2 = Task.Run(() =>
            {
                try
                {
                    job2.Do2(db);
                }
                catch (Exception e)
                {
                    TransParallelWorkload3.errors++;
                }
            });


            t1.Wait();
            t2.Wait();


            if (TransParallelWorkload3.errors > 0)
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
