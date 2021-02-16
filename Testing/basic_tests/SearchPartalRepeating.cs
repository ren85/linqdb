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
    public class SearchPartalRepeating : ITest
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
                NameSearchS = "professor programming pro1 pro0 pro1 pro2 pro3 pro4 pro5 pro6 pro7 pro8 pro9 pro10 pro11 pro12 pro13 pro14 pro15 pro16 pro17 pro18 pro19 pro20 pro21 pro22 pro23 pro24 pro25 pro26 pro27 pro28 pro29 pro30 pro31 pro32 pro33 pro34 pro35 pro36 pro37 pro38 pro39 pro40 pro41 pro42 pro43 pro44 pro45 pro46 pro47 pro48 pro49 pro50 pro51 pro52 pro53 pro54 pro55 pro56 pro57 pro58 pro59 pro60 pro61 pro62 pro63 pro64 pro65 pro66 pro67 pro68 pro69 pro70 pro71 pro72 pro73 pro74 pro75 pro76 pro77 pro78 pro79 pro80 pro81 pro82 pro83 pro84 pro85 pro86 pro87 pro88 pro89 pro90 pro91 pro92 pro93 pro94 pro95 pro96 pro97 pro98 pro99"
            };
            db.Table<SomeData>().Save(d); ;
            d = new SomeData()
            {
                Id = 2,
                Normalized = 0.9,
                PeriodId = 7,
                NameSearchS = "programmming"
            };
            db.Table<SomeData>().Save(d); ;
            d = new SomeData()
            {
                Id = 3,
                Normalized = 0.5,
                PeriodId = 10
            };
            db.Table<SomeData>().Save(d); ;
            d = new SomeData()
            {
                Id = 4,
                Normalized = 0.9,
                PeriodId = 7,
                NameSearchS = "привет"
            };
            db.Table<SomeData>().Save(d); ;

            var stats = new LinqdbSelectStatistics();
            var res = db.Table<SomeData>()
                        .SearchPartial(f => f.NameSearchS, "pro")
                        .OrderBy(f => f.Id)
                        .Select(f => new
                        {
                            Id = f.Id,
                            PeriodId = f.PeriodId
                        }, stats);
            if (res.Count() != 2 || res[0].Id != 1 || res[1].Id != 2)
            {
                throw new Exception("Assert failure");
            }

            if (stats.Total != 2)
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
