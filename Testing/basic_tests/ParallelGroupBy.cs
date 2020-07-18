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
    class ParallelGroupBy : ITest
    {
        public Db db { get; set; }
        public bool DoInit { get; set; }
        public ParallelGroupBy()
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
                db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
                DoInit = true;
            }
            this.db = db;

            db.Table<SomeData>().CreateGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);

            var start_list = new List<SomeData>(GroupJob.start);
            for (int i = 0; i < GroupJob.start; i++)
            {
                start_list.Add(new SomeData()
                {
                    Id = i,
                    GroupBy = i % 10,
                    Normalized = Convert.ToDouble("0," + i)
                });
            }
            for (int i = 0; ; i++)
            {
                var list = start_list.Skip(i * 50000).Take(50000).ToList();
                if (!list.Any())
                {
                    break;
                }
                db.Table<SomeData>().SaveBatch(list);
            }

            GroupJob.errors = 0;
            GroupJob.numbers = new List<int>();
            GroupJob.rg = new Random();

            for (int j = 0; j < 5; j++)
            {
                var jobs = new List<GroupJob>();
                for (int i = 0; i < 10000; i++)
                {
                    jobs.Add(new GroupJob());
                }

                Parallel.ForEach(jobs, /*new ParallelOptions { MaxDegreeOfParallelism = 500 },*/ f =>
                {
                    GroupJob.Do(db);
                });
            }

            db.Table<SomeData>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);

            if (GroupJob.errors > 0)
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
            if (DoInit)
            {
#if (SERVER || SOCKETS)
                if (dispose) { Logic.Dispose(); }
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

    class GroupJob
    {
        public static Random rg = new Random();
        public static object _lock = new object();
        public static int errors = 0;
#if (DATA)
        public static int start = 1000000;
#else
        public static int start = 1;
#endif
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
                int next = GroupJob.NextNumber();
                if (next % 3 == 0)
                {
                    var d = new SomeData()
                    {
                        Id = next,
                        NameSearch = next + " " + next,
                        Normalized = Convert.ToDouble("0," + next),
                        ObjectId = next,
                        PeriodId = next,
                        PersonId = next,
                        Value = next,
                        GroupBy = next
                    };

                    db.Table<SomeData>().Save(d);

                    lock (_lock)
                    {
                        numbers.Add(next);
                    }
                }
                else if (next % 3 == 1)
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
                    var entity = db.Table<SomeData>().Where(f => f.Id == to_read).GroupBy(f => f.GroupBy).Select(f => new { Id = f.Sum(z => z.Id), Normalized = f.Sum(z => z.Normalized) }).FirstOrDefault();
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
                    if (entity.Id != to_read || entity.Normalized != Convert.ToDouble("0," + to_read))
                    {
                        throw new Exception("Assert failure!");
                    }
                }
                else
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
                    db.Table<SomeData>().Delete(to_delete);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Parallel group by: " + ex.Message);
                lock (_lock)
                {
                    errors++;
                }
            }
        }
    }
}

