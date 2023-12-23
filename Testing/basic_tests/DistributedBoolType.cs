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
    public class DistributedBoolType : ITestDistributed
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

            var d = new BoolClassDistributed();
            try
            {
                db.DistributedTable<BoolClassDistributed>().Save(d);
                throw new Exception("Assert failure");
            }
            catch (Exception e)
            {
                string msg = "type is not supported";

                if (!e.Message.Contains(msg))
                {
                    throw new Exception("Assert failure");
                }
            }

            var de = new DecimalClassDistributed();
            try
            {
                db.DistributedTable<DecimalClassDistributed>().Save(de);
                throw new Exception("Assert failure");
            }
            catch (Exception e)
            {
                string msg = "type is not supported";

                if (!e.Message.Contains(msg))
                {
                    throw new Exception("Assert failure");
                }
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

    public class BoolClassDistributed
    {
        public int Id { get; set; }
        public int Sid { get; set; }
        public bool B { get; set; }
    }
    public class DecimalClassDistributed
    {
        public int Id { get; set; }
        public int Sid { get; set; }
        public decimal B { get; set; }
    }
}