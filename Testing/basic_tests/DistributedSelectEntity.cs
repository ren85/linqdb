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
    public class DistributedSelectEntity : ITestDistributed
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
                Date = Convert.ToDateTime("2000-01-01"),
                PersonId = 20,
                Value = 15.5
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);


            var res = db.DistributedTable<SomeDataDistributed>()
                        .SelectEntity();
            if (res.Count() != 1 || res[0].Normalized != 1.2 || res[0].Id != 1 || res[0].PeriodId != 5 || res[0].Date != Convert.ToDateTime("2000-01-01") || res[0].ObjectId != null || res[0].PeriodId != 5 || res[0].Value != 15.5)
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
