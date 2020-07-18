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
    class TwoTables : ITest
    {
        public void Do(Db db)
        {
            bool dispose = false; if (db == null) { db = new Db("DATA"); dispose = true; }
#if (SERVER)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif
#if (SOCKETS)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
            db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.Table<Answer>().Delete(new HashSet<int>(db.Table<Answer>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.Table<BinaryData>().Delete(new HashSet<int>(db.Table<BinaryData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
#if (SOCKETS || SAMEDB || INDEXES || SERVER)
            db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.Table<Answer>().Delete(new HashSet<int>(db.Table<Answer>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.Table<BinaryData>().Delete(new HashSet<int>(db.Table<BinaryData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
            var d = new SomeData()
            {
                Id = 1,
                Normalized = 1.2,
                PeriodId = 5
            };
            db.Table<SomeData>().Save(d);;
            var a = new Answer()
            {
                Id = 1,
                Anwser = "1",
                SomeDouble = 1,
                SomeId = 1,
                TitleSearch = "1 title"
            };
            db.Table<Answer>().Save(a);
            d = new SomeData()
            {
                Id = 2,
                Normalized = 0.9,
                PeriodId = 7
            };
            db.Table<SomeData>().Save(d);;
            a = new Answer()
            {
                Id = 2,
                Anwser = "2",
                SomeDouble = 2,
                SomeId = 2,
                TitleSearch = "2 title"
            };
            db.Table<Answer>().Save(a);

            var b = new BinaryData()
            {
                Id = 1,
                Value = -1,
                Data = Encoding.UTF8.GetBytes("abc")
            };

            db.Table<BinaryData>().Save(b);

            var res = db.Table<SomeData>()
                        .Between(f => f.Normalized, 0.1, 0.9)
                        .OrderBy(f => f.PeriodId)
                        .Select(f => new
                        {
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 1 || res[0].PeriodId != 7)
            {
                throw new Exception("Assert failure");
            }

            var res2 = db.Table<Answer>()
                       .Search(f => f.TitleSearch, "title")
                       .OrderBy(f => f.Id)
                       .Select(f => new
                       {
                           SomeId = f.SomeId
                       });
            if (res2.Count() != 2 || res2[0].SomeId != 1 || res2[1].SomeId != 2)
            {
                throw new Exception("Assert failure");
            }

            var res3 = db.Table<BinaryData>()
                         .Where(f => f.Id == 1)
                         .SelectEntity();
            if (res3.Count() != 1 || res3[0].Id != 1 || res3[0].Value != -1)
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
