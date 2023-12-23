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
    class DistributedBadWhere : ITestDistributed
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
            db.DistributedTable<SomeDataDistributed>().Save(d); ;
            d = new SomeDataDistributed()
            {
                Gid = 2,
                Normalized = 2.3,
                PeriodId = 10
            };
            db.DistributedTable<SomeDataDistributed>().Save(d); ;
            d = new SomeDataDistributed()
            {
                Gid = 3,
                Normalized = 4.5,
                PeriodId = 15
            };
            db.DistributedTable<SomeDataDistributed>().Save(d); ;



            string ex_msg = "";
            try
            {
                var res = db.DistributedTable<SomeDataDistributed>()
                        .Where(f => f.Date > Convert.ToDateTime("2016-11-12"))
                        .Select(f => new
                        {
                            f.PeriodId
                        });
            }
            catch (Exception ex)
            {
                ex_msg = ex.Message;
            }

            if (!ex_msg.StartsWith("Linqdb: error in Where clause - on the right hand side of the operator"))
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