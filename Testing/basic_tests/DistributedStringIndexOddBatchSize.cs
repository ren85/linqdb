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
    public class DistributedStringIndexOddBatchSize : ITestDistributed
    {
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

            var list = new List<SomeDataDistributed>();
            for (int i = 1; i < 1053; i++)
            {
                var d = new SomeDataDistributed()
                {
                    Gid = i,
                    Normalized = 1.2,
                    PeriodId = 5,
                    NameSearch = "test " + i
                };
                list.Add(d);
            }

            db.DistributedTable<SomeDataDistributed>().SaveBatch(list);

            var res = db.DistributedTable<SomeDataDistributed>()
                        .Search(f => f.NameSearch, "test 1051")
                        .Select(f => new
                        {
                            Gid = f.Gid,
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 1 || res[0].Gid != 1051)
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
