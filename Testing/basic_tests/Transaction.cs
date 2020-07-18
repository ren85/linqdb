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
    class Transaction : ITest
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
            db.Table<BinaryData>().Delete(new HashSet<int>(db.Table<BinaryData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.Table<Counter>().Delete(new HashSet<int>(db.Table<Counter>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
            using (var transaction = new LinqdbTransaction())
            {
                var d = new SomeData()
                {
                    Id = 1,
                    Normalized = 1.2,
                    PeriodId = 5
                };
                db.Table<SomeData>(transaction).Save(d);

                var d2 = new BinaryData()
                {
                    Id = 1,
                    Data = new List<byte>() { 1, 2, 3 }.ToArray()
                };
                db.Table<BinaryData>(transaction).Save(d2);
            }

            if (db.Table<SomeData>().SelectEntity().Any() || db.Table<BinaryData>().SelectEntity().Any())
            {
                throw new Exception("Assert failure");
            }

            using (var transaction = new LinqdbTransaction())
            {
                var d = new SomeData()
                {
                    Normalized = 1.2,
                    PeriodId = 5
                };
                db.Table<SomeData>(transaction).Save(d);

                d = new SomeData()
                {
                    Normalized = 1.2,
                    PeriodId = 5
                };
                db.Table<SomeData>(transaction).Save(d);

                var d2 = new BinaryData()
                {
                    Data = new List<byte>() { 1, 2, 3 }.ToArray()
                };
                db.Table<BinaryData>(transaction).Save(d2);


                transaction.Commit();
            }

            if (db.Table<SomeData>().SelectEntity().Count() != 2 || db.Table<BinaryData>().SelectEntity().Count() != 1)
            {
                throw new Exception("Assert failure");
            }

            if (db.Table<SomeData>().Count() != 2)
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

