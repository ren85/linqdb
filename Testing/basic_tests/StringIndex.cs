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
    class StringIndex : ITest
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
                NameSearch = "test 123 abc"
            };
            db.Table<SomeData>().Save(d);;
            d = new SomeData()
            {
                Id = 2,
                Normalized = 0.9,
                PeriodId = 7,
                NameSearch = "test 123 abc"
            };
            db.Table<SomeData>().Save(d);;
            d = new SomeData()
            {
                Id = 3,
                Normalized = 0.5,
                PeriodId = 10,
                NameSearch = "test 123 abc"
            };
            db.Table<SomeData>().Save(d);;

            var res = db.Table<SomeData>()
                        .Search(f => f.NameSearch, "test 123 abc")
                        .OrderBy(f => f.Id)
                        .Select(f => new
                        {
                            Id = f.Id,
                            PeriodId = f.PeriodId
                        });

            if (res.Count() != 3 || res[0].Id != 1 || res[1].Id != 2 || res[2].Id != 3)
            {
                throw new Exception("Assert failure");
            }
            for (int i = 0; i <= 3; i++)
            { 
                var dic = new Dictionary<int, string>()
                {
                    {
                        i, 
                        "įšėųęįšųėęį ęėųšįūųė ėųęįšųūė"
                    }
                };
                db.Table<SomeData>().Update(f => f.NameSearch, dic);
            }

            res = db.Table<SomeData>()
                        .Search(f => f.NameSearch, "test 123 abc")
                        .OrderBy(f => f.Id)
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
                        .Search(f => f.NameSearch, "įšėųęįšųėęį ęėųšįūųė ėųęįšųūė")
                        .OrderBy(f => f.Id)
                        .Select(f => new
                        {
                            Id = f.Id,
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 3 || res[0].Id != 1 || res[1].Id != 2 || res[2].Id != 3)
            {
                throw new Exception("Assert failure");
            }


            db.Table<SomeData>().Delete(new HashSet<int>() { 0, 1, 2 });

            res = db.Table<SomeData>()
                        .Search(f => f.NameSearch, "įšėųęįšųėęį ęėųšįūųė ėųęįšųūė")
                        .OrderBy(f => f.Id)
                        .Select(f => new
                        {
                            Id = f.Id,
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 1 || res[0].Id != 3)
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
