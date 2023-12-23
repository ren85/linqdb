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
    public class DistributedOrderByEdgeCases : ITestDistributed
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
                Normalized = 2.3,
                PeriodId = 7
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);
            d = new SomeDataDistributed()
            {
                Gid = 3,
                Normalized = 0.5,
                PeriodId = -10
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);
            d = new SomeDataDistributed()
            {
                Gid = 4,
                Normalized = 4.5,
                PeriodId = -15
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);

            var res = db.DistributedTable<SomeDataDistributed>()
                        .OrderBy(f => f.PeriodId)
                        .Take(3)
                        .Select(f => new
                        {
                            f.PeriodId
                        })
                        .OrderBy(f => f.PeriodId)
                        .Skip(1)
                        .Take(2)
                        .ToList();

            if (res.Count() != 2 || res[0].PeriodId != -10 || res[1].PeriodId != 5)
            {
                throw new Exception("Assert failure");
            }
            res = db.DistributedTable<SomeDataDistributed>()
                        .OrderBy(f => f.PeriodId)
                        .Take(3)
                        .Select(f => new
                        {
                            f.PeriodId
                        })
                        .OrderBy(f => f.PeriodId)
                        .Take(3)
                        .ToList();

            if (res.Count() != 3 || res[0].PeriodId != -15 || res[1].PeriodId != -10 || res[2].PeriodId != 5)
            {
                throw new Exception("Assert failure");
            }
            res = db.DistributedTable<SomeDataDistributed>()
                        .OrderByDescending(f => f.PeriodId)
                        .Take(3)
                        .Select(f => new
                        {
                            f.PeriodId
                        })
                        .OrderByDescending(f => f.PeriodId)
                        .Skip(1)
                        .Take(2)
                        .ToList();

            if (res.Count() != 2 || res[0].PeriodId != 5 || res[1].PeriodId != -10)
            {
                throw new Exception("Assert failure");
            }
            res = db.DistributedTable<SomeDataDistributed>()
                        .OrderByDescending(f => f.PeriodId)
                        .Take(3)
                        .Select(f => new
                        {
                            f.PeriodId
                        })
                        .OrderByDescending(f => f.PeriodId)
                        .Take(3)
                        .ToList();
            if (res.Count() != 3 || res[0].PeriodId != 7 || res[1].PeriodId != 5 || res[2].PeriodId != -10)
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