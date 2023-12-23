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
    public class DistributedCount : ITestDistributed
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
                Normalized = 0.9,
                PeriodId = 7
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);
            d = new SomeDataDistributed()
            {
                Id = 3,
                Sid = 3,
                Normalized = 0.5,
                PeriodId = 10
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);
            d = new SomeDataDistributed()
            {
                Id = 4,
                Sid = 4,
                Normalized = 4.5,
                PeriodId = 15
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);

            db.DistributedTable<SomeDataDistributed>().Delete(new List<DistributedId>() { 
                new DistributedId(2,2),
                new DistributedId(3,3)
            });
            int count = db.DistributedTable<SomeDataDistributed>().Count();
            if (count != 2)
            {
                throw new Exception("Assert failure");
            }
            count = db.DistributedTable<SomeDataDistributed>().Where(f => f.Id >= 2).Count();
            if (count != 1)
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
