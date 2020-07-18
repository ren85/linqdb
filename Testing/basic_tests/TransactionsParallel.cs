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

//2658 sec

namespace Testing.basic_tests
{
    class TransactionsParallel : ITest
    {
        bool InsertInTransaction(Db db, int i)
        {
            using (var transaction = new LinqdbTransaction())
            {
                var d = new SomeData()
                {
                    Id = i,
                    Normalized = 1.2,
                    PeriodId = 5,
                    Value = i
                };
                db.Table<SomeData>(transaction).Save(d);

                var d2 = new BinaryData()
                {
                    Id = i,
                    Data = new List<byte>() { 1, 2, 3 }.ToArray()
                };
                db.Table<BinaryData>(transaction).Save(d2);

                if (i % 2 == 0)
                {
                    transaction.Commit();
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }
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
            db.Table<BinaryData>().Delete(new HashSet<int>(db.Table<BinaryData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
#if (DATA)
            int preinserted = 1000000;
#else
            int preinserted = 0;
#endif
            int total = 100000;

            var plist = new List<SomeData>();
            for (int i = 1; i < preinserted; i++)
            {
                plist.Add(new SomeData()
                {
                    Id = total + 200 + i,
                    PeriodId = total + 200 + i,
                    Value = total + 200 + i
                });
            }
            for (int i = 0; ; i++)
            {
                var list = plist.Skip(i * 50000).Take(50000).ToList();
                if (!list.Any())
                {
                    break;
                }
                db.Table<SomeData>().SaveBatch(list);
            }


            var tasks = new List<Task<bool>>();
            for (int i = 1; i <= total; i++)
            {
                var c = i;
                var t = Task.Run<bool>(() =>
                {
                    try
                    {
                        return InsertInTransaction(db, c);
                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                });
                tasks.Add(t);
            }

            tasks.ForEach(f => f.Wait());

            if(tasks.Count(f => f.Result) != total / 2)
            {
                throw new Exception("Assert failure");
            }

            int l = total + 100;
            var res1 = db.Table<SomeData>().Where(f => f.Id < l).Select(f => new { f.Id }).Select(f => f.Id).ToList();
            if (res1.Any(f => f % 2 == 1))
            {
                throw new Exception("Assert failure");
            }
            var res2 = db.Table<BinaryData>().Select(f => new { f.Id }).Select(f => f.Id).ToList();
            if (res1.Any(f => f % 2 == 1))
            {
                throw new Exception("Assert failure");
            }

            var ids = db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList();
            for (int i = 0; ; i++)
            {
                var ids_delete = ids.Skip(i * 50000).Take(50000).ToList();
                if (!ids_delete.Any())
                {
                    break;
                }
                db.Table<SomeData>().Delete(new HashSet<int>(ids_delete));
            }

            ids = db.Table<BinaryData>().Select(f => new { f.Id }).Select(f => f.Id).ToList();
            for (int i = 0; ; i++)
            {
                var ids_delete = ids.Skip(i * 50000).Take(50000).ToList();
                if (!ids_delete.Any())
                {
                    break;
                }
                db.Table<BinaryData>().Delete(new HashSet<int>(ids_delete));
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
