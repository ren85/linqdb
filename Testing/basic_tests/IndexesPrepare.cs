#if (SERVER || SOCKETS)
using LinqdbClient;
using ServerLogic;
#else
using LinqDb;
#endif
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
    class IndexesPrepare : ITest
    {
        public void Do(Db db)
        {
            if (db == null)
            {
                db = new Db("DATA");
            }
#if (SERVER)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif
#if (SOCKETS)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
#endif
            //db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            var res = db.Table<SomeData>().SelectEntity();
            if (res.Any(f => f.Id < 0))
            {
                var a = res.Where(f => f.Id <= 0).ToList();
            }
            db.Table<SomeData>().CreatePropertyMemoryIndex(f => f.PeriodId);
            db.Table<SomeData>().CreatePropertyMemoryIndex(f => f.Id);
            //db.Table<SomeData>().CreatePropertyMemoryIndex(f => f.Normalized);
            //db.Table<SomeData>().CreatePropertyMemoryIndex(f => f.ObjectId);
            db.Table<SomeData>().CreatePropertyMemoryIndex(f => f.Value);
            //db.Table<SomeData>().CreatePropertyMemoryIndex(f => f.Date);
            //db.Table<SomeData>().CreateGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
