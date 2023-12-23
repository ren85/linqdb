using LinqdbClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.distributedtables;

namespace Testing.basic_tests
{
    public class DistributedBatchSave : ITestDistributed
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

            int total = 3000;
            var list = new List<SomeDataDistributed>();
            for (int i = 0; i < total; i++)
            {
                list.Add(new SomeDataDistributed()
                {
                    Normalized = 1.2,
                    PeriodId = i,
                    NameSearch = "test_" + i + " 123_" + i
                });
            }

            db.DistributedTable<SomeDataDistributed>().SaveBatch(list);

            var res = db.DistributedTable<SomeDataDistributed>()
                        .Select(f => new
                        {
                            Id = f.Id,
                            PeriodId = f.PeriodId
                        });
            
            if (res.Count() != total)
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
