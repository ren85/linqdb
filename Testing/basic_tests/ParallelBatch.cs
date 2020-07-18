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
    class ParallelBatch : ITest
    {
        public Db db { get; set; }
        public bool DoInit { get; set; }
        public ParallelBatch()
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
                DoInit = true;
            }
#if (SOCKETS || SAMEDB || INDEXES || SERVER)
            db.Table<Answer>().Delete(new HashSet<int>(db.Table<Answer>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.Table<KaggleClass>().Delete(new HashSet<int>(db.Table<KaggleClass>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
            this.db = db;

            int total_answers = 20000;
            var a_list = new List<Answer>();
            var a_ids = new List<int>();
            for (int i = 1; i <= total_answers; i++)
            {
                a_ids.Add(i);
                a_list.Add(new Answer()
                {
                    Id = i,
                    Anwser = "4 5 6",
                    TitleSearch = "7 8 9"
                });
            }
            db.Table<Answer>().SaveBatch(a_list);

            JobBatch.errors = 0;
            int total = 15000;
            var jobs = new List<JobBatch>();
            for (int i = 0; i < total; i++)
            {
                jobs.Add(new JobBatch());
            }

            var list = new List<Task>();

            foreach (var job in jobs)
            {
                var t = Task.Run(() => JobBatch.Do(db, a_ids));
                list.Add(t);
            }
            list.ForEach(f => f.Wait());

            if (JobBatch.errors > 0)
            {
                throw new Exception("Assert failure");
            }

            if (db.Table<Answer>().Count() != total_answers - total)
            {
                throw new Exception("Assert failure");
            }

            if (db.Table<KaggleClass>().Count() != total)
            {
                throw new Exception("Assert failure");
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

    class JobBatch
    {
        public static object _lock = new object();
        public static int errors = 0;

        public static void Do(Db db, List<int> a_ids)
        {
            try
            {
                using (var batch = new LinqdbTransaction())
                {
                    var k = new KaggleClass()
                    {
                        Q1 = "1 2 3",
                        Q2 = "4 5 6",
                        CommonPercentage = 0.5
                    };
                    db.Table<KaggleClass>(batch).Save(k);

                    var rg = new Random();
                    int id = 0;
                    lock (_lock)
                    {
                        if (a_ids.Any())
                        {
                            int which = rg.Next(0, Int32.MaxValue) % a_ids.Count();
                            id = a_ids[which];
                            a_ids.RemoveAt(which);
                        }
                    }
                    if (id > 0)
                    {
                        db.Table<Answer>(batch).Delete(id);
                    }
                    batch.Commit();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ParallelBatch: " + ex.Message);
                lock (_lock)
                {
                    errors++;
                }
            }
        }
    }
}