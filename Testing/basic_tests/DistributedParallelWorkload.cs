using LinqdbClient;
using ServerLogic;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.distributedtables;
using Testing.tables;

namespace Testing.basic_tests
{
    public class DistributedParallelWorkload : ITestDistributed
    {
        public DistributedDb db { get; set; }
        public bool DoInit { get; set; }
        public DistributedParallelWorkload()
        {
        }
        public void Do(DistributedDb db)
        {
            Do2(db);
        }
        public void Do2(DistributedDb db)
        {
            bool dispose = false;
            if (db == null)
            {
                DoInit = true;
                db = SocketTestingDistributed.GetTestDb();
                dispose = true;
            }
            this.db = db;

            var sids = db.DistributedTable<SomeDataDistributed>().Select(f => new { f.Sid, f.Id }).Select(f => new DistributedId(f.Sid, f.Id)).ToList();
            db.DistributedTable<SomeDataDistributed>().Delete(sids);


            var start_list = new List<SomeDataDistributed>(DistributedJob.start);
            for (int i = 0; i < DistributedJob.start; i++)
            {
                start_list.Add(new SomeDataDistributed()
                {
                    Gid = i,
                    GroupBy = i % 10,
                    Normalized = Convert.ToDouble("0." + i, CultureInfo.InvariantCulture)
                });
            }
            for (int i = 0; ; i++)
            {
                var list = start_list.Skip(i * 50000).Take(50000).ToList();
                if (!list.Any())
                {
                    break;
                }
                db.DistributedTable<SomeDataDistributed>().SaveBatch(list);
            }

            DistributedJob.errors = 0;
            DistributedJob.numbers = new List<int>();
            DistributedJob.rg = new Random();

            var sw = new Stopwatch();
            sw.Start();
            for (int j = 0; j < 5; j++)
            {
                var jobs = new List<SomeDataDistributed>();
                for (int i = 0; i < 10000; i++)
                {
                    jobs.Add(new SomeDataDistributed());
                }

                Parallel.ForEach(jobs, /*new ParallelOptions { MaxDegreeOfParallelism = 500 },*/ f =>
                {
                    DistributedJob.Do(db);
                });
            }

            if (DistributedJob.errors > 0)
            {
                throw new Exception("Assert failure");
            }
            Console.WriteLine("Select: {0} ms", DistributedJob.totalms_select / (double)DistributedJob.total_select);
            Console.WriteLine("Save: {0} ms", DistributedJob.totalms_save / (double)DistributedJob.total_save);
            Console.WriteLine("Delete: {0} ms", DistributedJob.totalms_delete / (double)DistributedJob.total_delete);
            Console.WriteLine("Update: {0} ms", DistributedJob.totalms_update / (double)DistributedJob.total_update);
            sw.Stop();
            Console.WriteLine("Total jobs 50000, time {0} sec", sw.ElapsedMilliseconds / 1000);

            var ids = db.DistributedTable<SomeDataDistributed>().Select(f => new { f.Id, f.Sid }).Select(f => new DistributedId(f.Sid, f.Id)).ToList();
            for (int i = 0; ; i++)
            {
                var ids_delete = ids.Skip(i * 50000).Take(50000).ToList();
                if (!ids_delete.Any())
                {
                    break;
                }
                db.DistributedTable<SomeDataDistributed>().Delete(ids_delete.ToList());
            }

            if (DoInit)
            {
                if(dispose) { Logic.Dispose(); }
            }
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }

    class DistributedJob
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
        public static void Do(DistributedDb db)
        {
            try
            {
                var sw = new Stopwatch();
                sw.Start();
                int next = DistributedJob.NextNumber();
                if (next % 4 == 0)
                {
                    var d = new SomeDataDistributed()
                    {
                        Gid = next,
                        NameSearch = next + " " + next,
                        Normalized = Convert.ToDouble("0." + next, CultureInfo.InvariantCulture),
                        ObjectId = next,
                        PeriodId = next,
                        PersonId = next,
                        Value = next
                    };

                    db.DistributedTable<SomeDataDistributed>().Save(d);
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
                    int to_read;
                    lock (_lock)
                    {
                        if (!numbers.Any())
                        {
                            return;
                        }
                        to_read = numbers[rg.Next() % numbers.Count];
                    }
                    var id_to_read = to_read;
                    var entity = db.DistributedTable<SomeDataDistributed>().Where(f => f.Gid == id_to_read).SelectEntity().FirstOrDefault();
                    if (entity == null)
                    {
                        lock (_lock)
                        {
                            if (numbers.Contains(to_read))
                            {
                                throw new Exception("Assert failure");
                            }
                            else
                            {
                                return;
                            }
                        }
                    }
                    if (entity.Gid != id_to_read || entity.NameSearch != id_to_read + " " + id_to_read || entity.Normalized != Convert.ToDouble("0." + id_to_read, CultureInfo.InvariantCulture) || entity.ObjectId != id_to_read ||
                       entity.PersonId != id_to_read  || entity.Value != id_to_read || !(entity.PeriodId == id_to_read || entity.PeriodId == -1 * id_to_read))
                    {
                        throw new Exception("Assert failure!");
                    }

                    var search_res = db.DistributedTable<SomeDataDistributed>().Search(f => f.NameSearch, id_to_read + "").Select(f => new { Gid = f.Gid }).Select(f => f.Gid).ToList();
                    if (!search_res.Contains(id_to_read))
                    {
                        lock (_lock)
                        {
                            if (numbers.Contains(to_read))
                            {
                                throw new Exception("Assert failure");
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
                    int to_delete;
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
                    var to_delete_id = db.DistributedTable<SomeDataDistributed>().Where(f => f.Gid == to_delete).Select(f => new { f.Id, f.Sid }).Select(f => new DistributedId(f.Sid, f.Id)).ToList();
                    db.DistributedTable<SomeDataDistributed>().Delete(to_delete_id.First());

                    sw.Stop();
                    lock (_lock)
                    {
                        totalms_delete += sw.ElapsedMilliseconds;
                        total_delete++;
                    }
                }
                else
                {
                    int to_update;
                    lock (_lock)
                    {
                        if (!numbers.Any())
                        {
                            return;
                        }

                        to_update = numbers[rg.Next() % numbers.Count];
                    }

                    var to_update_id = db.DistributedTable<SomeDataDistributed>().Where(f => f.Gid == to_update).Select(f => new { f.Id, f.Sid, f.PeriodId })
                        .Select(f => new 
                        { 
                            f.PeriodId,
                            DistributedId = new DistributedId(f.Sid, f.Id) 
                        })
                        .ToList();
                    if (!to_update_id.Any())
                    {
                        lock (_lock)
                        {
                            if (numbers.Contains(to_update))
                            {
                                throw new Exception("Assert failure");
                            }
                            else
                            {
                                return;
                            }
                        }
                    }

                    db.DistributedTable<SomeDataDistributed>().Update(f => f.PeriodId, to_update_id.First().DistributedId, -1 * to_update_id.First().PeriodId);

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
                Console.WriteLine("Parallel: " + ex.Message);
                lock (_lock)
                {
                    errors++;
                }
            }
        }
    }
}