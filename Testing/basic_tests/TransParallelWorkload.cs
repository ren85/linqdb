#if (SERVER || SOCKETS)
using LinqdbClient;
using ServerLogic;
#else
using LinqDb;
#endif
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
    class TransParallelWorkload : ITest
    {
        public Db db { get; set; }
        public bool DoInit { get; set; }
        public TransParallelWorkload()
        {
        }
        public void Do(Db db)
        {
            Do2(db);
        }
        public void Do2(Db db)
        {
            bool dispose = false;
            if (db == null)
            {
                db = new Db("DATA");
                dispose = true;
#if (SERVER)
                db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif
#if (SOCKETS)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
#endif
#if (SOCKETS || SAMEDB || INDEXES || SERVER)
                using (var trans = new LinqdbTransaction())
                {
                    db.Table<SomeData>(trans).Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
                    trans.Commit();
                }
#endif
                DoInit = true;
            }
            this.db = db;

            var start_list = new List<SomeData>(JobTrans.start);
            for (int i = 1; i < JobTrans.start; i++)
            {
                start_list.Add(new SomeData()
                {
                    Id = i,
                    GroupBy = i % 10,
                    Normalized = Convert.ToDouble("0," + i)
                });
            }
            if (start_list.Any())
            {
                for (int i = 0; ; i++)
                {
                    var list = start_list.Skip(i * 50000).Take(50000).ToList();
                    if (!list.Any())
                    {
                        break;
                    }
                    using (var trans = new LinqdbTransaction())
                    {
                        db.Table<SomeData>(trans).SaveBatch(list);
                        trans.Commit();
                    }
                }
            }


            JobTrans.errors = 0;
            JobTrans.numbers = new List<int>();
            JobTrans.rg = new Random();
            var sw = new Stopwatch();
            sw.Start();
            for (int j = 0; j < 5; j++)
            {
                var jobs = new List<JobTrans>();
                for (int i = 0; i < 10000; i++)
                {
                    jobs.Add(new JobTrans());
                }

                Parallel.ForEach(jobs, /*new ParallelOptions { MaxDegreeOfParallelism = 500 },*/ f =>
                {
                    JobTrans.Do(db);
                });
            }

            if (JobTrans.errors > 0)
            {
                throw new Exception("Assert failure, errors " + JobTrans.errors);
            }
            Console.WriteLine("Select: {0} ms", JobTrans.totalms_select / (double)JobTrans.total_select);
            Console.WriteLine("Save: {0} ms", JobTrans.totalms_save / (double)JobTrans.total_save);
            Console.WriteLine("Delete: {0} ms", JobTrans.totalms_delete / (double)JobTrans.total_delete);
            Console.WriteLine("Update: {0} ms", JobTrans.totalms_update / (double)JobTrans.total_update);
            sw.Stop();
            Console.WriteLine("Total jobs 50000, time {0} sec", sw.ElapsedMilliseconds / 1000);
            var ids = db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList();
            for (int i = 0; ; i++)
            {
                var ids_delete = ids.Skip(i * 50000).Take(50000).ToList();
                if (!ids_delete.Any())
                {
                    break;
                }
                using (var trans = new LinqdbTransaction())
                {
                    db.Table<SomeData>().Delete(new HashSet<int>(ids_delete));
                    trans.Commit();
                }                
            }
            if (DoInit)
            {
#if (SERVER || SOCKETS)
            if(dispose) { Logic.Dispose(); }
#else
                if (dispose) { db.Dispose(); }
#endif
#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
            if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
#endif
            }
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }

    class JobTrans
    {
#if (DATA)
        public static int start = 1000000;
#else
        public static int start = 1;
#endif
        public static Random rg = new Random();
        public static object _lock = new object();
        public static int errors = 0;

        public static long totalms_select = 0;
        public static long total_select = 0;
        public static long totalms_save = 0;
        public static long total_save = 0;
        public static long totalms_delete = 0;
        public static long total_delete = 0;
        public static long totalms_update = 0;
        public static long total_update = 0;

        public static int NextNumber()
        {
            lock (_lock)
            {
                int n = rg.Next(0, Int32.MaxValue / 2 - 1);
                if (n <= start + 10)
                {
                    n += start + 10;
                }
                return n;
            }
        }

        public static List<int> numbers = new List<int>();
        public static void Do(Db db)
        {
            try
            {
                var sw = new Stopwatch();
                sw.Start();
                int next = JobTrans.NextNumber();
                if (next % 4 == 0)
                {
                    var d = new SomeData()
                    {
                        Id = next,
                        NameSearch = next + " " + next,
                        Normalized = Convert.ToDouble("0," + next),
                        ObjectId = next,
                        PeriodId = next,
                        PersonId = next,
                        Value = next
                    };

                    using (var trans = new LinqdbTransaction())
                    {
                        db.Table<SomeData>(trans).Save(d);
                        trans.Commit();
                    }
                    sw.Stop();
                    lock (_lock)
                    {
                        numbers.Add(next);
                        totalms_save += sw.ElapsedMilliseconds;
                        total_save++;
                    }
                }
                else if (next % 4 == 1)
                {
                    int to_read = 0;
                    lock (_lock)
                    {
                        if (!numbers.Any())
                        {
                            return;
                        }
                        to_read = numbers[rg.Next() % numbers.Count];
                    }
                    var entity = db.Table<SomeData>().Where(f => f.Id == to_read).SelectEntity().FirstOrDefault();
                    if (entity == null)
                    {
                        lock (_lock)
                        {
                            if (numbers.Contains(to_read))
                            {
                                throw new Exception("Assert failure 1");
                            }
                            else
                            {
                                return;
                            }
                        }
                    }
                    if (entity.Id != to_read || entity.NameSearch != to_read + " " + to_read || entity.Normalized != Convert.ToDouble("0," + to_read) || entity.ObjectId != to_read ||
                        entity.PersonId != to_read || entity.PersonId != to_read || entity.Value != to_read || !(entity.PeriodId == to_read || entity.PeriodId == -1 * to_read))
                    {
                        throw new Exception("Assert failure 2");
                    }

                    var search_res = db.Table<SomeData>().Search(f => f.NameSearch, to_read + "").Select(f => new { Id = f.Id }).Select(f => f.Id).ToList();
                    if (!search_res.Contains(to_read))
                    {
                        lock (_lock)
                        {
                            if (numbers.Contains(to_read))
                            {
                                throw new Exception("Assert failure 3");
                            }
                            else
                            {
                                return;
                            }
                        }
                    }

                    sw.Stop();
                    lock (_lock)
                    {
                        totalms_select += sw.ElapsedMilliseconds;
                        total_select++;
                    }

                }
                else if (next % 4 == 2)
                {
                    int to_delete = 0;
                    lock (_lock)
                    {
                        if (!numbers.Any())
                        {
                            return;
                        }

                        to_delete = numbers[rg.Next() % numbers.Count];
                        while (numbers.Contains(to_delete))
                        {
                            numbers.Remove(to_delete);
                        }
                    }
                    using (var trans = new LinqdbTransaction())
                    {
                        db.Table<SomeData>(trans).Delete(to_delete);
                        trans.Commit();
                    }

                    sw.Stop();
                    lock (_lock)
                    {
                        totalms_delete += sw.ElapsedMilliseconds;
                        total_delete++;
                    }
                }
                else
                {
                    int to_update = 0;
                    lock (_lock)
                    {
                        if (!numbers.Any())
                        {
                            return;
                        }

                        to_update = numbers[rg.Next() % numbers.Count];
                    }
                    var val = db.Table<SomeData>().Where(f => f.Id == to_update).Select(f => new { PeriodId = f.PeriodId }).FirstOrDefault();
                    if (val == null)
                    {
                        lock (_lock)
                        {
                            if (numbers.Contains(to_update))
                            {
                                throw new Exception("Assert failure 4");
                            }
                            else
                            {
                                return;
                            }
                        }
                    }
                    using (var trans = new LinqdbTransaction())
                    {
                        db.Table<SomeData>(trans).Update(f => f.PeriodId, to_update, -1 * val.PeriodId);
                        trans.Commit();
                    }

                    sw.Stop();
                    lock (_lock)
                    {
                        totalms_update += sw.ElapsedMilliseconds;
                        total_update++;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Trans Parallel: " + ex.Message);
                lock (_lock)
                {
                    errors++;
                }
            }
        }
    }
}
