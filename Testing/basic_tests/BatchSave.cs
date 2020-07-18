#if (SERVER || SOCKETS)
using LinqdbClient;
using ServerLogic;
#else
using LinqDb;
#endif
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
    class BatchSave : ITest
    {
        public void Do(Db db)
        {
            bool dispose = false;
            if (db == null)
            {
                db = new Db("DATA");
                dispose = true;
            }
#if (SERVER)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif
#if (SOCKETS)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
            db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
            var list = new List<SomeData>();
            for (int i = 0; i < 3000; i++)
            { 
                list.Add(new SomeData()
                {
                    Id = i,
                    Normalized = 1.2,
                    PeriodId = 5,
                    NameSearch = "test_" + i + " 123_"+ i
                });
            }

            db.Table<SomeData>().SaveBatch(list);

            var res = db.Table<SomeData>()
                        .Search(f => f.NameSearch, "test_1500")
                        .OrderBy(f => f.Id)
                        .Select(f => new
                           {
                                Id = f.Id,
                                PeriodId = f.PeriodId
                           });
            if (res.Count() != 1 || res[0].Id != 1500)
            {
                throw new Exception("Assert failure");
            }

#if (SERVER || SOCKETS)
            if(dispose)
            {
                if(dispose) { Logic.Dispose(); }
            }
#else
            if (dispose)
            {
                if (dispose) { db.Dispose(); }
            }
#endif
#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
            if(dispose)
            {
                if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
            }
#endif
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
