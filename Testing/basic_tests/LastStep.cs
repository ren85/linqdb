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
    public class LastStep : ITest
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
            db.Table<Question>().Delete(new HashSet<int>(db.Table<Question>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
            var count = db.Table<Question>().Count();
            var q = new Question();
            db.Table<Question>().Save(q);
            var count2 = db.Table<Question>().Count();
            if (count + 1 != count2)
            {
                throw new Exception("Assert failure");
            }

            var ls = db.Table<Question>().LastStep();
            var max_id = db.Table<Question>().OrderByDescending(f => f.Id).Take(1).Select(f => new { f.Id }).First().Id;

            if (ls != max_id / 1000)
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
