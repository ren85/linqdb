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
    class TransParallelWorkload2 : ITest
    {
        public Db db { get; set; }
        public bool DoInit { get; set; }
        public TransParallelWorkload2()
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
                using (var trans = new LinqdbTransaction())
                {
                    db.Table<BinaryData>(trans).Delete(new HashSet<int>(db.Table<BinaryData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
                    trans.Commit();
                }
#endif
                DoInit = true;
            }
            this.db = db;

            var start_list = new List<BinaryData>(TransJob2.start);
            for (int i = 1; i < TransJob2.start; i++)
            {
                start_list.Add(new BinaryData()
                {
                    Id = i,
                    Data = new byte[4] { 1, 2, 3, 4 }
                });
            }
            if (start_list.Any())
            {
                for(int i = 0; ; i++)
                {
                    var list = start_list.Skip(i * 50000).Take(50000).ToList();
                    if (!list.Any())
                    {
                        break;
                    }
                    using (var trans = new LinqdbTransaction())
                    {
                        db.Table<BinaryData>(trans).SaveBatch(list);
                        trans.Commit();
                    }
                }
            }
            

            TransJob2.errors = 0;
            TransJob2.numbers = new List<int>();
            TransJob2.rg = new Random();

            for (int j = 0; j < 5; j++)
            {
                var jobs = new List<TransJob2>();
                for (int i = 0; i < 10000; i++)
                {
                    jobs.Add(new TransJob2());
                }

                Parallel.ForEach(jobs, /*new ParallelOptions { MaxDegreeOfParallelism = 500 },*/ f =>
                {
                    TransJob2.Do(db);
                });
            }

            if (TransJob2.errors > 0)
            {
                throw new Exception("Assert failure 1");
            }
            var ids = db.Table<BinaryData>().Select(f => new { f.Id }).Select(f => f.Id).ToList();
            for (int i = 0; ; i++)
            {
                var ids_delete = ids.Skip(i * 50000).Take(50000).ToList();
                if (!ids_delete.Any())
                {
                    break;
                }
                using (var trans = new LinqdbTransaction())
                {
                    db.Table<BinaryData>(trans).Delete(new HashSet<int>(ids_delete));
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

    class TransJob2
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
                int next = TransJob2.NextNumber();
                if (next % 4 == 0)
                {
                    var d = new BinaryData()
                    {
                        Id = next,
                        Value = -next,
                        Data = BitConverter.GetBytes(next)
                    };

                    using (var trans = new LinqdbTransaction())
                    {
                        db.Table<BinaryData>(trans).Save(d);
                        trans.Commit();
                    }

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
                                throw new Exception("Assert failure 2");
                            }
                            else
                            {
                                return;
                            }
                        }
                    }
                    if (entity.Id != to_read || BitConverter.ToInt32(entity.Data, 0) != to_read || !(entity.Value == -to_read || entity.Value == null))
                    {
                        throw new Exception("Assert failure 3");
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
                        db.Table<BinaryData>(trans).Delete(to_delete);
                        trans.Commit();
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
                    var val = db.Table<BinaryData>().Where(f => f.Id == to_update).Select(f => new { Value = f.Value }).FirstOrDefault();
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
                        db.Table<BinaryData>(trans).Update(f => f.Value, to_update, (int?)null);
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("TransParallel2: " + ex.Message);
                lock (_lock)
                {
                    errors++;
                }
            }
        }
    }
}
