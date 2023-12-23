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
    public class DistributedSearchSlices : ITestDistributed
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
            for (int i = 1; i < 3100; i++)
            {
                var d = new SomeDataDistributed()
                {
                    Gid = i,
                    Normalized = i,
                    PeriodId = i,
                    NameSearch = "test " + i + " abc"
                };
                list.Add(d);
            }
            db.DistributedTable<SomeDataDistributed>().SaveBatch(list);


            var res = db.DistributedTable<SomeDataDistributed>()
                        .Search(f => f.NameSearch, "test")
                        .OrderBy(f => f.Id)
                        .Select(f => new
                        {
                            f.Id,
                            f.Gid,
                            f.PeriodId
                        });
            if (res.Count() != db.DistributedTable<SomeDataDistributed>().Count())
            {
                throw new Exception("Assert failure");
            }
            res = db.DistributedTable<SomeDataDistributed>()
                    .Search(f => f.NameSearch, "test", 0, 1)
                    .OrderBy(f => f.Id)
                    .Select(f => new
                    {
                        f.Id,
                        f.Gid,
                        f.PeriodId
                    });
            if (res.Count() != 999 && res.Any(f => f.Gid >= 1000))
            {
                throw new Exception("Assert failure");
            }
            res = db.DistributedTable<SomeDataDistributed>()
                    .Search(f => f.NameSearch, "test", 1, 1)
                    .OrderBy(f => f.Id)
                    .Select(f => new
                    {
                        f.Id,
                        f.Gid,
                        f.PeriodId
                    });
            if (res.Count() != 1000 && res.Any(f => f.Gid < 1000 || f.Gid >= 2000))
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