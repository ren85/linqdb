using LinqdbClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Testing.distributedtables;
using Testing.tables;

namespace Testing.basic_tests
{
    public class DistributedOr2 : ITestDistributed
    {
        public void Do(DistributedDb db)
        {
            bool dispose = false;
            if (db == null)
            {
                db = SocketTestingDistributed.GetTestDb();
                dispose = true;
            }

            var sids = db.DistributedTable<UsersItemDistributed>().Select(f => new { f.Sid, f.Id }).Select(f => new DistributedId(f.Sid, f.Id)).ToList();
            db.DistributedTable<UsersItemDistributed>().Delete(sids);


            var d = new UsersItemDistributed()
            {
                Gid = 1,
                TitleSearch = "read web",
                CodeSearch = "read web",
                RegexSearch = "read web",
                ReplaceSearch = "read web",
                UserId = 20
            };
            db.DistributedTable<UsersItemDistributed>().Save(d);
            d = new UsersItemDistributed()
            {
                Gid = 2,
                TitleSearch = "read web",
                CodeSearch = "read web",
                RegexSearch = "read web",
                ReplaceSearch = "read web",
                UserId = 4975
            };
            db.DistributedTable<UsersItemDistributed>().Save(d);


            var res = db.DistributedTable<UsersItemDistributed>().Where(f => f.UserId == 20)
                                           .Search(f => f.TitleSearch, "read").Or().Search(f => f.CodeSearch, "read").Or().Search(f => f.RegexSearch, "read").Or().Search(f => f.ReplaceSearch, "read")
                                           .Search(f => f.TitleSearch, "web").Or().Search(f => f.CodeSearch, "web").Or().Search(f => f.RegexSearch, "web").Or().Search(f => f.ReplaceSearch, "web")
                        .SelectEntity();
            if (res.Count() != 1 || res[0].Gid != 1)
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