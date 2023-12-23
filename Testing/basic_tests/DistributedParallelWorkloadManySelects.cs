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
    internal class DistributedParallelWorkloadManySelects : ITestDistributed
    {
        public DistributedDb db { get; set; }
        public bool DoInit { get; set; }
        public DistributedParallelWorkloadManySelects()
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


            var start_list = new List<SomeDataDistributed>(DistributedJobManySelect.start);
            for (int i = 0; i < DistributedJobManySelect.start; i++)
            {
                start_list.Add(new SomeDataDistributed()
                {
                    Gid = i,
                    GroupBy = i % 10,
                    Normalized = Convert.ToDouble("0," + i)
                });
            }
            db.DistributedTable<SomeDataDistributed>().SaveBatch(start_list);

            DistributedJobManySelect.errors = 0;
            DistributedJobManySelect.numbers = new List<int>();
            DistributedJobManySelect.rg = new Random();

            for (int j = 0; j < 5; j++)
            {
                var jobs = new List<DistributedJobManySelect>();
                for (int i = 0; i < 10000; i++)
                {
                    jobs.Add(new DistributedJobManySelect());
                }

                Parallel.ForEach(jobs, /*new ParallelOptions { MaxDegreeOfParallelism = 500 },*/ f =>
                {
                    DistributedJobManySelect.Do(db);
                });
            }

            if (DistributedJobManySelect.errors > 0)
            {
                throw new Exception("Assert failure");
            }
            var ids = db.DistributedTable<SomeDataDistributed>().Select(f => new { f.Id, f.Sid }).Select(f => new DistributedId(f.Sid, f.Id)).ToList();
            db.DistributedTable<SomeDataDistributed>().Delete(ids);

            if (DoInit)
            {
                if (dispose) { Logic.Dispose(); }
            }
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }

    class DistributedJobManySelect
    {
#if (DATA)
        public static int start = 1000000;
#else
        public static int start = 1;
#endif
        public static Random rg = new Random();
        public static object _lock = new object();
        public static int errors = 0;
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
                int next = DistributedJobManySelect.NextNumber();
                if (next % 10 == 0)
                {
                    var d = new SomeDataDistributed()
                    {
                        Gid = next,
                        NameSearch = next + " " + next,
                        Normalized = Convert.ToDouble("0," + next),
                        ObjectId = next,
                        PeriodId = next,
                        PersonId = next,
                        Value = next
                    };

                    db.DistributedTable<SomeDataDistributed>().Save(d);

                    lock (_lock)
                    {
                        numbers.Add(next);
                    }
                }
                else if (next % 10 > 2)
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
                    var entity = db.DistributedTable<SomeDataDistributed>().Where(f => f.Gid == to_read).SelectEntity().FirstOrDefault();
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
                    if (entity.Gid != to_read || entity.NameSearch != to_read + " " + to_read || entity.Normalized != Convert.ToDouble("0," + to_read) || entity.ObjectId != to_read ||
                       entity.PersonId != to_read || entity.PersonId != to_read || entity.Value != to_read || !(entity.PeriodId == to_read || entity.PeriodId == -1 * to_read))
                    {
                        throw new Exception("Assert failure!");
                    }

                    var search_res = db.DistributedTable<SomeDataDistributed>().Search(f => f.NameSearch, to_read + "").Select(f => new { f.Gid }).Select(f => f.Gid).ToList();
                    if (!search_res.Contains(to_read))
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

                }
                else if (next % 10 == 2)
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
                    var did = db.DistributedTable<SomeDataDistributed>().Where(f => f.Gid == to_delete).Select(f => new { f.Id, f.Sid }).Select(f => new DistributedId(f.Sid, f.Id)).Single();
                    db.DistributedTable<SomeDataDistributed>().Delete(did);
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
                    var val = db.DistributedTable<SomeDataDistributed>().Where(f => f.Gid == to_update).Select(f => new { f.Id, f.Sid, f.PeriodId }).FirstOrDefault();
                    if (val == null)
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
                    db.DistributedTable<SomeDataDistributed>().Update(f => f.PeriodId, new DistributedId(val.Sid, val.Id), -1 * val.PeriodId);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Distributed Parallel many select: " + ex.Message);
                lock (_lock)
                {
                    errors++;
                }
            }
        }
    }
}