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
    class IndexRemovedField : ITest
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
            db.Table<SomeType>().Delete(new HashSet<int>(db.Table<SomeType>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif

            var d2 = new Testing.tables.SomeType()
            {
                Id = 2,
                Value = 2,
                PeriodId = 2,
                Name = "2"
            };
            db.Table<Testing.tables.SomeType>().Save(d2);

            db.Table<Testing.tables.SomeType>().CreatePropertyMemoryIndex(f => f.PeriodId);

            var d = new Testing.tables4.SomeType()
            {
                Id = 1,
                Value = 1,
                Name = "1"
            };
            try
            {
                db.Table<Testing.tables4.SomeType>().Save(d);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("Can't insert row with missing column on which in-memory index is built"))
                {
                    throw new Exception("Assert failure");
                }
            }

#if (SERVER || SOCKETS)
            if (dispose) { Logic.Dispose(); }
#else
            if (dispose) { db.Dispose(); }
#endif
#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
            if (dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
#endif
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}