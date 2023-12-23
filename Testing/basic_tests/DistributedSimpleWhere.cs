using LinqdbClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.distributedtables;

namespace Testing.basic_tests
{
    public class DistributedSimpleWhere : ITestDistributed
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

            var d = new SomeDataDistributed()
            {
                Id = 1,
                Sid = 1,
                Normalized = 1.2,
                PeriodId = 5
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);
            d = new SomeDataDistributed()
            {
                Id = 1,
                Sid= 1,
                Normalized = 2.1,
                PeriodId = 10
            };
            db.DistributedTable<SomeDataDistributed>().Save(d); 

            var res = db.DistributedTable<SomeDataDistributed>()
                        .Where(f => f.PeriodId == 10)
                        .Select(f => new
                        {
                            Normalized = f.Normalized
                        });
            if (res[0].Normalized != 2.1)
            {
                throw new Exception(string.Format("Assert failure: {0} != 2.1", res[0].Normalized));
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

