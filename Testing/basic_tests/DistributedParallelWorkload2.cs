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
    public class DistributedParallelWorkload2 : ITestDistributed
    {
        public DistributedDb db { get; set; }
        public bool DoInit { get; set; }
        public DistributedParallelWorkload2()
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




            var start_list = new List<BinaryDataDistributed>(DistributedJob2.start);
            for (int i = 0; i < DistributedJob2.start; i++)
            {
                start_list.Add(new BinaryDataDistributed()
                {
                    Gid = i,
                    Data = new byte[4] { 1, 2, 3, 4 }
                });
            }
            for (int i = 0; ; i++)
            {
                var list = start_list.Skip(i * 50000).Take(50000).ToList();
                if (!list.Any())
                {
                    break;
                }
                db.DistributedTable<BinaryDataDistributed>().SaveBatch(list);
            }

            DistributedJob2.errors = 0;
            DistributedJob2.numbers = new List<int>();
            DistributedJob2.rg = new Random();

            for (int j = 0; j < 5; j++)
            {
                var jobs = new List<DistributedJob2>();
                for (int i = 0; i < 10000; i++)
                {
                    jobs.Add(new DistributedJob2());
                }

                Parallel.ForEach(jobs, /*new ParallelOptions { MaxDegreeOfParallelism = 500 },*/ f =>
                {
                    DistributedJob2.Do(db);
                });
            }

            if (DistributedJob2.errors > 0)
            {
                throw new Exception("Assert failure");
            }

            var ids = db.DistributedTable<BinaryDataDistributed>().Select(f => new { f.Id, f.Sid }).Select(f => new DistributedId(f.Sid, f.Id)).ToList();
            for (int i = 0; ; i++)
            {
                var ids_delete = ids.Skip(i * 50000).Take(50000).ToList();
                if (!ids_delete.Any())
                {
                    break;
                }
                db.DistributedTable<BinaryDataDistributed>().Delete(ids_delete.ToList());
            }

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

    class DistributedJob2
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
                int next = NextNumber();
                if (next % 4 == 0)
                {
                    var d = new BinaryDataDistributed()
                    {
                        Gid = next,
                        Value = -next,
                        Data = BitConverter.GetBytes(next)
                    };

                    db.DistributedTable<BinaryDataDistributed>().Save(d);

                    lock (_lock)
                    {
                        numbers.Add(next);
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
                    var entity = db.DistributedTable<BinaryDataDistributed>().Where(f => f.Gid == to_read).SelectEntity().FirstOrDefault();
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
                    if (entity.Gid != to_read || BitConverter.ToInt32(entity.Data, 0) != to_read || !(entity.Value == -to_read || entity.Value == null))
                    {
                        throw new Exception("Assert failure!");
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
                    var to_delete_id = db.DistributedTable<BinaryDataDistributed>().Where(f => f.Gid == to_delete).Select(f => new { f.Id, f.Sid }).Select(f => new DistributedId(f.Sid, f.Id)).ToList();
                    db.DistributedTable<BinaryDataDistributed>().Delete(to_delete_id.First());
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

                    var to_update_id = db.DistributedTable<BinaryDataDistributed>().Where(f => f.Gid == to_update).Select(f => new { f.Id, f.Sid, f.Value })
                        .Select(f => new
                        {
                            f.Value,
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

                    db.DistributedTable<BinaryDataDistributed>().Update(f => f.Value, to_update_id.First().DistributedId, (int?)null);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Parallel2: " + ex.Message);
                lock (_lock)
                {
                    errors++;
                }
            }
        }
    }
}