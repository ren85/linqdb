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
    class ParallelWorkload2 : ITest
    {
        public Db db { get; set; }
        public bool DoInit { get; set; }
        public ParallelWorkload2()
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
                dispose = true;
                db = new Db("DATA");
#if (SERVER)
                db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif
#if (SOCKETS)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
#endif
#if (SOCKETS || SAMEDB || INDEXES || SERVER)
                db.Table<BinaryData>().Delete(new HashSet<int>(db.Table<BinaryData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
                DoInit = true;
            }
            this.db = db;

            var start_list = new List<BinaryData>(Job2.start);
            for (int i = 0; i < Job2.start; i++)
            {
                start_list.Add(new BinaryData()
                {
                    Id = i,
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
                db.Table<BinaryData>().SaveBatch(list);
            }

            Job2.errors = 0;
            Job2.numbers = new List<int>();
            Job2.rg = new Random();

            for (int j = 0; j < 5; j++)
            {
                var jobs = new List<Job2>();
                for (int i = 0; i < 10000; i++)
                {
                    jobs.Add(new Job2());
                }

                Parallel.ForEach(jobs, /*new ParallelOptions { MaxDegreeOfParallelism = 500 },*/ f =>
                {
                    Job2.Do(db);
                });
            }

            if (Job2.errors > 0)
            {
                throw new Exception("Assert failure");
            }

            var ids = db.Table<BinaryData>().Select(f => new { f.Id }).Select(f => f.Id).ToList();
            for (int i = 0; ; i++)
            {
                var ids_delete = ids.Skip(i * 50000).Take(50000).ToList();
                if (!ids_delete.Any())
                {
                    break;
                }
                db.Table<BinaryData>().Delete(new HashSet<int>(ids_delete));
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

    class Job2
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
        public static void Do(Db db)
        {
            try
            {
                int next = Job2.NextNumber();
                if (next % 4 == 0)
                {
                    var d = new BinaryData()
                    {
                        Id = next,
                        Value = -next,
                        Data = BitConverter.GetBytes(next)
                    };

                    db.Table<BinaryData>().Save(d);

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
                    var entity = db.Table<BinaryData>().Where(f => f.Id == to_read).SelectEntity().FirstOrDefault();
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
                    if (entity.Id != to_read || BitConverter.ToInt32(entity.Data, 0) != to_read || !(entity.Value == -to_read || entity.Value == null))
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
                    db.Table<BinaryData>().Delete(to_delete);
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
                    var val = db.Table<BinaryData>().Where(f => f.Id == to_update).Select(f => new { Value = f.Value }).FirstOrDefault();
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
                    db.Table<BinaryData>().Update(f => f.Value, to_update, (int?)null);
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
