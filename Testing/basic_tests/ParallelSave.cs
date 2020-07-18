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
    class ParallelSave : ITest
    {
        public Db db { get; set; }
        public bool DoInit { get; set; }
        public ParallelSave()
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
            db.Table<UsersItem>().Delete(new HashSet<int>(db.Table<UsersItem>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
            this.db = db;

            JobSave.errors = 0;
            JobSave.n = 0;
            int total = 15000;
            //for (int j = 0; j < 5; j++)
            //{
                var jobs = new List<JobSave>();
                for (int i = 0; i < total; i++)
                {
                    jobs.Add(new JobSave());
                }

            //Parallel.ForEach(jobs, /*new ParallelOptions { MaxDegreeOfParallelism = 500 },*/ f =>
            //{
            //    JobSave.Do(db);
            //});


            var list = new List<Task>();

            foreach (var job in jobs)
            {
                var t = Task.Run(() => JobSave.Do(db));
                list.Add(t);
            }
            list.ForEach(f => f.Wait());
            
            if (JobSave.errors > 0)
            {
                throw new Exception("Assert failure");
            }
            for (int i = 0; i < JobSave.n; i++)
            {
                var r = db.Table<UsersItem>().Where(f => f.UserId == i).SelectEntity();
                if (r.Count() != 1 || r[0].UserId != i)
                {
                    //Console.WriteLine(r.Count() + " " + r[0].UserId + " " + i);
                    throw new Exception("Assert failure");
                }
            }
            if (db.Table<UsersItem>().Search(f => f.CodeSearch, "0").Count() != total)
            {
                //Console.WriteLine(db.Table<UsersItem>().Search(f => f.Code, "0").Count());
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

    class JobSave
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

        public static void Do(Db db)
        {
            try
            {
                int n = NextFixedNumber();
                var d = new UsersItem()
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

                db.Table<UsersItem>().Save(d);
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
