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
    class ParallelWorkloadAll : ITest
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
            db.Table<BinaryData>().Delete(new HashSet<int>(db.Table<BinaryData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.Table<UsersItem>().Delete(new HashSet<int>(db.Table<UsersItem>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.Table<Answer>().Delete(new HashSet<int>(db.Table<Answer>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.Table<KaggleClass>().Delete(new HashSet<int>(db.Table<KaggleClass>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif

            var job1 = new ParallelWorkload();
            var job2 = new ParallelWorkload2();
            var job3 = new ParallelSave();
            var job4 = new ParallelBatch();



            var t1 = Task.Run(() =>
            {
                try
                {
                    job1.Do2(db);
                }
                catch (Exception e)
                {
                    ParallelWorkloadAll.errors++;
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
                    ParallelWorkloadAll.errors++;
                }
            });

            var t3 = Task.Run(() =>
            {
                try
                {
                    job3.Do2(db);
                }
                catch (Exception e)
                {
                    ParallelWorkloadAll.errors++;
                }
            });

            var t4 = Task.Run(() =>
            {
                try
                {
                    job4.Do2(db);
                }
                catch (Exception e)
                {
                    ParallelWorkloadAll.errors++;
                }
            });


            t1.Wait();
            t2.Wait();
            t3.Wait();
            t4.Wait();

            if (ParallelWorkloadAll.errors > 0)
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