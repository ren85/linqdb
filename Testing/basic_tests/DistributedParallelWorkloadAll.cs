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
    public class DistributedParallelWorkloadAll : ITestDistributed
    {
        public static int errors = 0;
        public void Do(DistributedDb db)
        {
            bool dispose = false;
            if (db == null)
            {
                db = SocketTestingDistributed.GetTestDb();
                dispose = true;
            }

            var sids = db.DistributedTable<SomeDataDistributed>().Select(f => new { f.Sid, f.Id }).Select(f => new DistributedId(f.Sid, f.Id)).ToList();
            db.DistributedTable<SomeDataDistributed>().Delete(sids);

            sids = db.DistributedTable<BinaryDataDistributed>().Select(f => new { f.Sid, f.Id }).Select(f => new DistributedId(f.Sid, f.Id)).ToList();
            db.DistributedTable<BinaryDataDistributed>().Delete(sids);

            sids = db.DistributedTable<UsersItemDistributed>().Select(f => new { f.Sid, f.Id }).Select(f => new DistributedId(f.Sid, f.Id)).ToList();
            db.DistributedTable<UsersItemDistributed>().Delete(sids);

            var job1 = new DistributedParallelWorkload();
            var job2 = new DistributedParallelWorkload2();
            var job3 = new DistributedParallelSave();



            var t1 = Task.Run(() =>
            {
                try
                {
                    job1.Do2(db);
                }
                catch (Exception e)
                {
                    DistributedParallelWorkloadAll.errors++;
                }
            });

            var t2 = Task.Run(() =>
            {
                try
                {
                    job2.Do2(db);
                }
                catch (Exception e)
                {
                    DistributedParallelWorkloadAll.errors++;
                }
            });

            var t3 = Task.Run(() =>
            {
                try
                {
                    job3.Do2(db);
                }
                catch (Exception e)
                {
                    DistributedParallelWorkloadAll.errors++;
                }
            });


            t1.Wait();
            t2.Wait();
            t3.Wait();

            if (DistributedParallelWorkloadAll.errors > 0)
            {
                throw new Exception("Assert failure");
            }

            if (dispose)
            {
                if (dispose) { db.Dispose(); }
            }
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
