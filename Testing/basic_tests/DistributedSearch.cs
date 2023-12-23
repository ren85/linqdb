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
    public class DistributedSearch : ITestDistributed
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
                PeriodId = 5,
                NameSearch = "test 123 abc"
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);
            d = new SomeDataDistributed()
            {
                Id = 2,
                Sid = 2,
                Normalized = 0.9,
                PeriodId = 7,
                NameSearch = "test"
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


            var res = db.DistributedTable<SomeDataDistributed>()
                        .Search(f => f.NameSearch, "test")
                        .OrderBy(f => f.Id)
                        .Select(f => new
                        {
                            Id = f.Id,
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 2 || res[0].Id + res[1].Id != 3)
            {
                throw new Exception("Assert failure");
            }
            res = db.DistributedTable<SomeDataDistributed>()
                        .Search(f => f.NameSearch, "test abc")
                        .Select(f => new
                        {
                            Id = f.Id,
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 1 || res[0].Id != 1)
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
