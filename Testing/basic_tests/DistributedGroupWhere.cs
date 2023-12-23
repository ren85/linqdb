using LinqdbClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.distributedtables;

namespace Testing.basic_tests
{
    public class DistributedGroupWhere : ITestDistributed
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
                GroupBy = 5,
                NameSearch = "a"
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);

            d = new SomeDataDistributed()
            {
                Gid = 2,
                Normalized = 7,
                GroupBy = 3,
                NameSearch = "a"
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);

            db.DistributedTable<SomeDataDistributed>().CreateGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);

            d = new SomeDataDistributed()
            {
                Gid = 3,
                Normalized = 2.3,
                GroupBy = 10,
                NameSearch = "a"
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);
            d = new SomeDataDistributed()
            {
                Gid = 4,
                Normalized = 4.5,
                GroupBy = 10,
                NameSearch = "b"
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);


            var res = db.DistributedTable<SomeDataDistributed>()
                        .Where(f => f.Normalized > 3)
                        .GroupBy(f => f.GroupBy)
                        .Select(f => new
                        {
                            Key = f.Key,
                            Sum = f.Sum(z => z.Normalized),
                            Total = f.Count()
                        })
                        .GroupBy(f => f.Key)
                        .Select(f => new
                        {
                            Key = f.Key,
                            Sum = f.Sum(z => z.Sum),
                            Total = f.Sum(z => z.Total)
                        })
                        .ToList();

            if (res.Count() != 2 || res.Where(f => f.Key == 3).First().Total != 1 || res.Where(f => f.Key == 10).First().Total != 1 ||
                res.Where(f => f.Key == 3).First().Sum != 7 || res.Where(f => f.Key == 10).First().Sum != 4.5)
            {
                throw new Exception("Assert failure");
            }

            db.DistributedTable<SomeDataDistributed>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);


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
