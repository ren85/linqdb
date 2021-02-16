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
    public class SearchSpacesOnly : ITest
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
                NameSearchS = "test@abc one"
            };
            db.Table<SomeData>().Save(d); ;

            var res = db.Table<SomeData>()
                        .Search(f => f.NameSearchS, "test")
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
                        .Search(f => f.NameSearchS, "test@abc")
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
            .Search(f => f.NameSearchS, "abc")
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
                       .Search(f => f.NameSearchS, "one")
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
                      .Search(f => f.NameSearchS, "test@ABC")
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
              .Search(f => f.NameSearchS, "ONE")
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
              .SearchPartial(f => f.NameSearchS, "test@AB")
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
            if (dispose) { Logic.Dispose(); }
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
