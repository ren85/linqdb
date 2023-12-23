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
    public class DistributedGetIds : ITestDistributed
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
                Gid = 1,
                Normalized = 1.2,
                PeriodId = 5
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);
            d = new SomeDataDistributed()
            {
                Gid = 2,
                Normalized = 0.9,
                PeriodId = 7
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);
            d = new SomeDataDistributed()
            {
                Gid = 3,
                Normalized = 0.5,
                PeriodId = 10
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);
            d = new SomeDataDistributed()
            {
                Gid = 4,
                Normalized = 4.5,
                PeriodId = 15
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);

            var all = db.DistributedTable<SomeDataDistributed>().GetIds();
            if (!all.AllIds || all.Ids.Count() != 4)
            {
                throw new Exception("Assert failure");
            }

            all = db.DistributedTable<SomeDataDistributed>().Where(f => f.Gid > 0).GetIds();
            if (all.Ids.Count() != 4)
            {
                throw new Exception("Assert failure");
            }

            var ids = db.DistributedTable<SomeDataDistributed>().Where(f => f.Gid > 2).GetIds();
            if (ids.Ids.Count() != 2 || ids.AllIds)
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