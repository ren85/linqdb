using LinqdbClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.distributedtables;

namespace Testing.basic_tests
{
    public class DistributedBasicGroup : ITestDistributed
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

            if (res.Count() != 3 || res.Where(f => f.Key == 5).First().Total != 1 || res.Where(f => f.Key == 3).First().Total != 1 || res.Where(f => f.Key == 10).First().Total != 2 ||
                res.Where(f => f.Key == 5).First().Sum != 1.2 || res.Where(f => f.Key == 3).First().Sum != 7 || res.Where(f => f.Key == 10).First().Sum != 6.8)
            {
                throw new Exception("Assert failure");
            }


            d = new SomeDataDistributed()
            {
                Gid = 5,
                Normalized = 9.3,
                GroupBy = 22,
                NameSearch = "a"
            };
            var b = new SomeDataDistributed()
            {
                Gid = 6,
                Normalized = 0.7,
                GroupBy = 22,
                NameSearch = "b"
            };
            db.DistributedTable<SomeDataDistributed>().SaveBatch(new List<SomeDataDistributed>() { d, b });

            res = db.DistributedTable<SomeDataDistributed>()
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

            if (res.Count() != 4 || res.Where(f => f.Key == 5).First().Total != 1 || res.Where(f => f.Key == 3).First().Total != 1 || res.Where(f => f.Key == 10).First().Total != 2 ||
                res.Where(f => f.Key == 5).First().Sum != 1.2 || res.Where(f => f.Key == 3).First().Sum != 7 || res.Where(f => f.Key == 10).First().Sum != 6.8 ||
                res.Where(f => f.Key == 22).First().Sum != 10)
            {
                throw new Exception("Assert failure");
            }

            var vals = db.DistributedTable<SomeDataDistributed>().Where(f => f.Gid == 5 || f.Gid == 6).Select(f => new { f.Id, f.Sid, f.Gid });
            var update_data = new Dictionary<DistributedId, double?>();
            foreach (var val in vals)
            {
                var update_value = 0.0;
                if (val.Gid == 5)
                {
                    update_value = 13.3;
                }
                else
                {
                    update_value = 1.7;
                }
                update_data[new DistributedId(val.Sid, val.Id)] = update_value;
            }            
            db.DistributedTable<SomeDataDistributed>().Update(f => f.Normalized, update_data);

            res = db.DistributedTable<SomeDataDistributed>()
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

            if (res.Count() != 4 || res.Where(f => f.Key == 5).First().Total != 1 || res.Where(f => f.Key == 3).First().Total != 1 || res.Where(f => f.Key == 10).First().Total != 2 ||
                res.Where(f => f.Key == 5).First().Sum != 1.2 || res.Where(f => f.Key == 3).First().Sum != 7 || res.Where(f => f.Key == 10).First().Sum != 6.8 ||
                res.Where(f => f.Key == 22).First().Sum != 15)
            {
                throw new Exception("Assert failure");
            }

            var id_to_delete = db.DistributedTable<SomeDataDistributed>().Where(f => f.Gid == 6).GetIds().Ids.Single();
            db.DistributedTable<SomeDataDistributed>().Delete(id_to_delete);

            res = db.DistributedTable<SomeDataDistributed>()
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

            if (res.Count() != 4 || res.Where(f => f.Key == 5).First().Total != 1 || res.Where(f => f.Key == 3).First().Total != 1 || res.Where(f => f.Key == 10).First().Total != 2 ||
                res.Where(f => f.Key == 5).First().Sum != 1.2 || res.Where(f => f.Key == 3).First().Sum != 7 || res.Where(f => f.Key == 10).First().Sum != 6.8 ||
                res.Where(f => f.Key == 22).First().Sum != 13.3)
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
