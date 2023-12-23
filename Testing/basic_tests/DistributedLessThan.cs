using LinqdbClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.distributedtables;

namespace Testing.basic_tests
{
    public class DistributedLessThan : ITestDistributed
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
                Id = 2,
                Sid = 2,
                Normalized = 2.3,
                PeriodId = 10
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);
            d = new SomeDataDistributed()
            {
                Id = 3,
                Sid = 3,
                Normalized = 4.5,
                PeriodId = 15
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);

            var res = db.DistributedTable<SomeDataDistributed>()
                        .Where(f => f.Normalized < 3)
                        .Select(f => new
                        {
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 2)
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

