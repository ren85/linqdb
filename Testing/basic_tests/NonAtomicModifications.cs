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
    class NonAtomicModifications : ITest
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

            var count = db.Table<SomeData>().Count();

            if (count != 0)
            {
                throw new Exception("Assert failure");
            }

            var list = new List<SomeData>();
            int total = 1000000;
            for (int i = 1; i <= total; i++)
            {
                list.Add(new SomeData()
                {
                    Id = i,
                    NameSearch = i.ToString() + " " + i.ToString(),
                    Normalized = i,
                    ObjectId = i,
                    PeriodId = i,
                    Value = i
                });
            }
            db.Table<SomeData>().SaveNonAtomically(list);
            count = db.Table<SomeData>().Count();

            if (count != total)
            {
                throw new Exception("Assert failure");
            }
            
            try
            {
                using (var tran = new LinqdbTransaction())
                {
                    db.Table<SomeData>(tran).SaveNonAtomically(list);
                }
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("can't be used in a transaction"))
                {
                    throw new Exception("Assert failure");
                }
            }

            var updateInfo = list.ToDictionary(f => f.Id, f => f.Id + 1);
            db.Table<SomeData>().UpdateNonAtomically(f => f.PeriodId, updateInfo);

            var res = db.Table<SomeData>().Select(f => new
            {
                f.Id,
                f.ObjectId,
                f.PeriodId
            });

            if (res.Count() == 0 || res.Any(f => f.Id != f.ObjectId || f.Id != f.PeriodId - 1))
            {
                throw new Exception("Assert failure");
            }

            var ids = db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id);
            db.Table<SomeData>().DeleteNonAtomically(new HashSet<int>(ids));

            count = db.Table<SomeData>().Count();
            if (count != 0)
            {
                throw new Exception("Assert failure");
            }

            list = new List<SomeData>();
            for (int i = 1; i <= total; i++)
            {
                list.Add(new SomeData()
                {
                    NameSearch = i.ToString() + " " + i.ToString(),
                    Normalized = i,
                    ObjectId = i,
                    PeriodId = i,
                    Value = i
                });
            }
            db.Table<SomeData>().SaveNonAtomically(list);
            count = db.Table<SomeData>().Count();
            if (count != total)
            {
                throw new Exception("Assert failure");
            }
            var idsList = db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList();
            if (idsList.Distinct().Count() != total)
            {
                throw new Exception("Assert failure");
            }

            ids = db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id);
            db.Table<SomeData>().DeleteNonAtomically(new HashSet<int>(ids));

            count = db.Table<SomeData>().Count();
            if (count != 0)
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