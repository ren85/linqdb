#if (SERVER || SOCKETS)
using LinqdbClient;
using ServerLogic;
#else
using LinqDb;
#endif
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
    class Or2 : ITest
    {
        public void Do(Db db)
        {
            bool dispose = false; if (db == null) { db = new Db("DATA"); dispose = true; }
#if (SERVER)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif
#if (SOCKETS)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
#endif
#if (SOCKETS || SAMEDB || INDEXES || SERVER)
            db.Table<SomeData>().Delete(new HashSet<int>(db.Table<UsersItem>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
            var d = new UsersItem()
            {
                Id = 1,
                TitleSearch = "read web",
                CodeSearch = "read web",
                RegexSearch = "read web",
                ReplaceSearch = "read web",
                UserId = 20
            };
            db.Table<UsersItem>().Save(d);
            d = new UsersItem()
            {
                Id = 2,
                TitleSearch = "read web",
                CodeSearch = "read web",
                RegexSearch = "read web",
                ReplaceSearch = "read web",
                UserId = 4975
            };
            db.Table<UsersItem>().Save(d); 
           

            var res = db.Table<UsersItem>().Where(f => f.UserId == 20)
                                           .Search(f => f.TitleSearch, "read").Or().Search(f => f.CodeSearch, "read").Or().Search(f => f.RegexSearch, "read").Or().Search(f => f.ReplaceSearch, "read")
                                           .Search(f => f.TitleSearch, "web").Or().Search(f => f.CodeSearch, "web").Or().Search(f => f.RegexSearch, "web").Or().Search(f => f.ReplaceSearch, "web")
                        .SelectEntity();
            if (res.Count() != 1 || res[0].Id  != 1)
            {
                throw new Exception("Assert failure");
            }
#if (SERVER || SOCKETS)
            if(dispose) { Logic.Dispose(); }
#else
            if (dispose) { db.Dispose(); }
#endif
#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
            if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
#endif
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
