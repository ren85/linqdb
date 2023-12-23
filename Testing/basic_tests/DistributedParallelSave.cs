using LinqdbClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Testing.distributedtables;
using Testing.tables;


namespace Testing.basic_tests
{
    public class DistributedParallelSave : ITestDistributed
    {
        public DistributedDb db { get; set; }
        public bool DoInit { get; set; }
        public DistributedParallelSave()
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

    class JobSaveDistributed
    {
        public static object _lock = new object();
        public static int errors = 0;
        public static int n = 0;

        public static int NextFixedNumber()
        {
            lock (_lock)
            {
                int tmp = n;
                n++;
                return tmp;
            }
        }

        public static void Do(DistributedDb db)
        {
            try
            {
                int n = NextFixedNumber();
                var d = new UsersItemDistributed()
                {
                    UserId = n
                };
                d.CodeSearch = "0 1 2";
                d.Date = DateTime.Now;
                d.GuidSearch = "asdasd";
                d.ID = "code_123";
                d.IsLive = 1;
                d.LangSearch = "1";
                d.TextSearch = "sdsdf ksajdks";
                d.TitleSearch = "0 1 2";

                db.DistributedTable<UsersItemDistributed>().Save(d);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Distributed Parallel2: " + ex.Message);
                lock (_lock)
                {
                    errors++;
                }
            }
        }
    }
}