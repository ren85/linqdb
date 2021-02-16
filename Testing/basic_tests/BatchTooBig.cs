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
    class BatchTooBig : ITest
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
            var list = new List<SomeData>();
            for (int i = 0; i < 500000; i++)
            {
                list.Add(new SomeData());
            }
            try
            {
                db.Table<SomeData>().SaveBatch(list);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("modification batch is too large"))
                {
                    throw new Exception("Assert failure");
                }
            }
            list = new List<SomeData>();
            for (int i = 0; i < 500000; i++)
            {
                list.Add(new SomeData());
                if (list.Count() > 100000)
                {
                    db.Table<SomeData>().SaveBatch(list);
                    list = new List<SomeData>();
                }
            }
            if (list.Any())
            {
                db.Table<SomeData>().SaveBatch(list);
            }
            var ids = db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList();
            try
            {
                db.Table<SomeData>().Delete(new HashSet<int>(ids));
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("modification batch is too large"))
                {
                    throw new Exception("Assert failure");
                }
            }
            for(int i=0; ; i++)
            {
                var part = ids.Skip(i * 100000).Take(100000).ToList();
                if (!part.Any())
                {
                    break;
                }
                db.Table<SomeData>().Delete(new HashSet<int>(part));
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