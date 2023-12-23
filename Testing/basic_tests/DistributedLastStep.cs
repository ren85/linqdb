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
    public class DistributedLastStep : ITestDistributed
    {
        public void Do(DistributedDb db)
        {
            bool dispose = false;
            if (db == null)
            {
                db = SocketTestingDistributed.GetTestDb();
                dispose = true;
            }

            var sids = db.DistributedTable<QuestionDistributed>().Select(f => new { f.Sid, f.Id }).Select(f => new DistributedId(f.Sid, f.Id)).ToList();
            db.DistributedTable<QuestionDistributed>().Delete(sids);

            var count = db.DistributedTable<QuestionDistributed>().Count();
            var q = new QuestionDistributed();
            db.DistributedTable<QuestionDistributed>().Save(q);
            var count2 = db.DistributedTable<QuestionDistributed>().Count();
            if (count + 1 != count2)
            {
                throw new Exception("Assert failure");
            }

            var ls = db.DistributedTable<QuestionDistributed>().LastStep();
            var max_id = db.DistributedTable<QuestionDistributed>().OrderByDescending(f => f.Id).Take(1).Select(f => new { f.Id }).OrderByDescending(f => f.Id).Take(1).First().Id;

            if (ls != max_id / 1000)
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