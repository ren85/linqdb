using LinqdbClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.distributedtables;

namespace Testing.basic_tests
{
    public class DistributedGenericType : ITestDistributed
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

            TestGenerics<SomeDataDistributed>(db, new SomeDataDistributed()
            { 
                Gid = 1,
                NameSearch = "test",
                ObjectId = 5,
                PeriodId = 5,
                GroupBy = 5
            });

            if (dispose)
            {
                if (dispose) { db.Dispose(); }
            }
        }

        void TestGenerics<T>(DistributedDb db, T item) where T : ISomeDataDistributed, new()
        {
            db.DistributedTable<T>().Save(item);
            var some = db.DistributedTable<T>().Where(f => f.Gid > 0).SelectEntity();
            if (some.Count() != 1)
            {
                throw new Exception("Assert failure");
            }
            db.DistributedTable<T>().Update(f => f.ObjectId, new DistributedId(some.Single().Sid, some.Single().Id) , 5);
            some = db.DistributedTable<T>().Where(f => f.Gid > 0).SelectEntity();
            if (some.Single().ObjectId != 5)
            {
                throw new Exception("Assert failure");
            }
            
            some = db.DistributedTable<T>().Search(f => f.NameSearch, "test").SelectEntity();
            if (some.Count() != 1)
            {
                throw new Exception("Assert failure");
            }
            some = db.DistributedTable<T>().Between(f => f.ObjectId, 5, 5, BetweenBoundaries.BothInclusive).SelectEntity();
            if (some.Count() != 1)
            {
                throw new Exception("Assert failure");
            }
            some = db.DistributedTable<T>().Intersect(f => f.ObjectId, new HashSet<int?>() { 5 }).SelectEntity();
            if (some.Count() != 1)
            {
                throw new Exception("Assert failure");
            }
            some = db.DistributedTable<T>().Intersect(f => f.ObjectId, new HashSet<int?>() { 5 }).OrderBy(f => f.Gid).Take(1).SelectEntity();
            if (some.Count() != 1)
            {
                throw new Exception("Assert failure");
            }
            some = db.DistributedTable<T>().Intersect(f => f.ObjectId, new HashSet<int?>() { 5 }).OrderByDescending(f => f.Gid).Take(1).SelectEntity();
            if (some.Count() != 1)
            {
                throw new Exception("Assert failure");
            }

            int id = some.Single().Gid;
            db.DistributedTable<T>().Delete(new DistributedId(some.Single().Sid, some.Single().Id));
            some = db.DistributedTable<T>().Where(f => f.Gid > 0).SelectEntity();
            if (some.Count() != 0)
            {
                throw new Exception("Assert failure");
            }
        }
        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
