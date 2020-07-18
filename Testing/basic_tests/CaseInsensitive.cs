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
    class CaseInsensitive : ITest
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
            db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
            var d = new SomeData()
            {
                Id = 1,
                Normalized = 1.2,
                PeriodId = 5,
                NameSearch = "TeSt 123 AbC"
            };
            db.Table<SomeData>().Save(d); ;
            d = new SomeData()
            {
                Id = 2,
                Normalized = 0.9,
                PeriodId = 7,
                NameSearch = "test"
            };
            db.Table<SomeData>().Save(d); ;
            d = new SomeData()
            {
                Id = 3,
                Normalized = 0.5,
                PeriodId = 10
            };
            db.Table<SomeData>().Save(d); ;


            var res = db.Table<SomeData>()
                        .Search(f => f.NameSearch, "test")
                        .OrderBy(f => f.Id)
                        .Select(f => new
                        {
                            Id = f.Id,
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 2 || res[0].Id != 1 || res[1].Id != 2)
            {
                throw new Exception("Assert failure");
            }
            res = db.Table<SomeData>()
                        .Search(f => f.NameSearch, "test abc")
                        .Select(f => new
                        {
                            Id = f.Id,
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 1 || res[0].Id != 1)
            {
                throw new Exception("Assert failure");
            }


            d = new SomeData()
            {
                Id = 1,
                Normalized = 1.2,
                PeriodId = 5,
                NameSearch = "ПриВеТ Как ДелА",
                NotSearchable = "ПриВеТ Как ДелА"
            };
            db.Table<SomeData>().Save(d);

            res = db.Table<SomeData>()
                       .Search(f => f.NameSearch, "привет дела")
                       .Select(f => new
                       {
                           Id = f.Id,
                           PeriodId = f.PeriodId
                       });
            if (res.Count() != 1 || res[0].Id != 1)
            {
                throw new Exception("Assert failure");
            }


            res = db.Table<SomeData>()
                       .Where(f => f.NameSearch == "привет как дела")
                       .Select(f => new
                       {
                           Id = f.Id,
                           PeriodId = f.PeriodId
                       });
            if (res.Count() != 1 || res[0].Id != 1)
            {
                throw new Exception("Assert failure");
            }

            res = db.Table<SomeData>()
                       .Where(f => f.NameSearch == "привет как делаa")
                       .Select(f => new
                       {
                           Id = f.Id,
                           PeriodId = f.PeriodId
                       });
            if (res.Count() != 0)
            {
                throw new Exception("Assert failure");
            }

            res = db.Table<SomeData>()
                       .Where(f => f.NameSearch != "привет как дела")
                       .Select(f => new
                       {
                           Id = f.Id,
                           PeriodId = f.PeriodId
                       });
            if (res.Count() != 2)
            {
                throw new Exception("Assert failure");
            }

            var res2 = db.Table<SomeData>()
                       .Intersect(f => f.NameSearch, new HashSet<string>() { "привет как дела" })
                       .Select(f => new
                       {
                           f.Id,
                           f.PeriodId,
                           f.NameSearch,
                           f.NotSearchable
                       });
            if (res2.Count() != 1 || res2[0].Id != 1 || res2[0].NameSearch != "ПриВеТ Как ДелА" || res2[0].NotSearchable != "ПриВеТ Как ДелА")
            {
                throw new Exception("Assert failure");
            }

            res = db.Table<SomeData>()
                    .SearchPartial(f => f.NameSearch, "ПРИ КА Д")
                    .Select(f => new
                    {
                        Id = f.Id,
                        PeriodId = f.PeriodId
                    });
            if (res.Count() != 1 || res[0].Id != 1)
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