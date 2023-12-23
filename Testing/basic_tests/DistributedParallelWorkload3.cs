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
    public class DistributedParallelWorkload3 : ITestDistributed
    {
        public static int errors = 0;
        public DistributedDb db { get; set; }
        public bool DoInit { get; set; }
        public DistributedParallelWorkload3()
        {
        }
        public void Do(DistributedDb db)
        {
            bool dispose = false;
            if (db == null)
            {
                DoInit = true;
                db = SocketTestingDistributed.GetTestDb();
                dispose = true;
            }
            this.db = db;

            var job1 = new DistributedParallelWorkload();
            var job2 = new DistributedParallelWorkload2();

            var t1 = Task.Run(() =>
            {
                try
                {
                    job1.Do2(db);
                }
                catch (Exception)
                {
                    DistributedParallelWorkload3.errors++;
                }
            });

            var t2 = Task.Run(() =>
            {
                try
                {
                    job2.Do2(db);
                }
                catch (Exception)
                {
                    DistributedParallelWorkload3.errors++;
                }
            });


            t1.Wait();
            t2.Wait();


            if (DistributedParallelWorkload3.errors > 0)
            {
                throw new Exception("Assert failure");
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
}
