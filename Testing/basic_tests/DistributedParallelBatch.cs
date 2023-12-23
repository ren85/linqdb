using LinqdbClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.distributedtables;
using Testing.tables;

namespace Testing.basic_tests
{
    public class DistributedParallelBatch : ITestDistributed
    {
        public DistributedDb db { get; set; }
        public bool DoInit { get; set; }
        public DistributedParallelBatch()
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
                db = SocketTestingDistributed.GetTestDb();
                dispose = true;
                DoInit = true;
            }

            var sids = db.DistributedTable<UsersItemDistributed>().Select(f => new { f.Sid, f.Id }).Select(f => new DistributedId(f.Sid, f.Id)).ToList();
            db.DistributedTable<UsersItemDistributed>().Delete(sids);

            this.db = db;

            JobSaveDistributed.errors = 0;
            JobSaveDistributed.n = 0;
            int total = 150;

            var jobs = new List<JobSaveDistributed>();
            for (int i = 0; i < total; i++)
            {
                jobs.Add(new JobSaveDistributed());
            }

            var list = new List<Task>();

            foreach (var job in jobs)
            {
                var t = Task.Run(() => JobSaveDistributed.Do(db));
                list.Add(t);
            }
            list.ForEach(f => f.Wait());

            if (JobSaveDistributed.errors > 0)
            {
                throw new Exception("Assert failure");
            }
            for (int i = 0; i < JobSaveDistributed.n; i++)
            {
                var r = db.DistributedTable<UsersItemDistributed>().Where(f => f.UserId == i).SelectEntity();
                if (r.Count() != 1 || r[0].UserId != i)
                {
                    throw new Exception("Assert failure");
                }
            }
            if (db.DistributedTable<UsersItemDistributed>().Search(f => f.CodeSearch, "0").Count() != total)
            {
                throw new Exception("Assert failure");
            }


            if (DoInit)
            {
                if (dispose)
                {
                    if (dispose) { db.Dispose(); }
                }
            }
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }

    class JobBatchDistributed
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