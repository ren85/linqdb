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
    public class DistributedTotal : ITestDistributed
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
                Normalized = 1.2,
                PeriodId = 5
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);
            d = new SomeDataDistributed()
            {
                Normalized = 2.3,
                PeriodId = 10
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);
            d = new SomeDataDistributed()
            {
                Normalized = 4.5,
                PeriodId = 15
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);

            var statistics = new LinqdbSelectStatistics();
            var res = db.DistributedTable<SomeDataDistributed>()
                        .Where(f => f.Normalized > 2 && f.Normalized < 3)
                        .OrderBy(f => f.Date)
                        .Select(f => new
                        {
                            f.PeriodId,
                            f.Date
                        }, statistics)
                        .OrderBy(f => f.Date)
                        .ToList();
            if (res.Count() != statistics.Total)
            {
                throw new Exception("Assert failure");
            }

            statistics = new LinqdbSelectStatistics();
            res = db.DistributedTable<SomeDataDistributed>()
                        .OrderBy(f => f.Date)
                        .Take(2)
                        .Select(f => new
                        {
                            f.PeriodId,
                            f.Date
                        }, statistics)
                        .OrderBy(f => f.Date)
                        .Skip(1)
                        .Take(1)
                        .ToList();
            if (statistics.Total != 3)
            {
                throw new Exception("Assert failure");
            }

            statistics = new LinqdbSelectStatistics();
            var res2 = db.DistributedTable<SomeDataDistributed>()
                        .OrderBy(f => f.Date)
                        .Take(3)
                        .SelectEntity(statistics)
                        .OrderBy(f => f.Date)
                        .Skip(1)
                        .Take(2)
                        .ToList();
            if (statistics.Total != 3)
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
