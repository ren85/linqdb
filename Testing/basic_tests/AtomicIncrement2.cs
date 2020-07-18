//#if (SERVER || SOCKETS)
//using LinqdbClient;
//using ServerLogic;
//#else
//using LinqDb;
//#endif
//using System;
//using System.Collections.Generic;
//using System.IO;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;
//using Testing.tables;

//namespace Testing.basic_tests
//{
//    class AtomicIncrement2 : ITest
//    {
//        public void Do(Db db)
//        {
//            var start = DateTime.Now;

//            bool dispose = false; if (db == null) { db = new Db("DATA"); dispose = true; }
//#if (SERVER)
//            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
//#endif
//#if (SOCKETS)
//            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
//#endif
//#if (SOCKETS || SAMEDB || INDEXES || SERVER)
//            db.Table<Counter>().Delete(new HashSet<int>(db.Table<Counter>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
//#endif
//            var rg = new Random();
//            var _lock = new object();
//            int result = 0;
//            var list = new List<CounterJob2>();
//            for (int i = 0; i < 10000; i++)
//            {
//                int v = rg.Next(0, 10000000);
//                if (i % 2 == 0)
//                {
//                    v = -1 * v;
//                }
//                result += v;
//                list.Add(new CounterJob2() { Val = v });
//            }

//            Parallel.ForEach(list, f => f.Do(db));

//            var res = db.Table<Counter>().SelectEntity();
//            if (res.Count() != 1 || res[0].Value != result)
//            {
//                throw new Exception("Assert failure");
//            }

//#if (SERVER || SOCKETS)
//            if (dispose) { Logic.Dispose(); }
//#else
//            if (dispose) { db.Dispose(); }
//#endif
//#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
//            if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
//#endif

//            Console.WriteLine((DateTime.Now - start).TotalSeconds);
//        }

//        public string GetName()
//        {
//            return this.GetType().Name;
//        }
//    }

//    public class CounterJob2
//    {
//        public bool IsError { get; set; }
//        public bool Done { get; set; }
//        public int Val { get; set; }
//        public void Do(Db db)
//        {
//            try
//            {
//                var n = new Counter()
//                {
//                    Name = "test",
//                    Value = Val
//                };
//                db.Table<Counter>()
//                  .Where(f => f.Name == "test")
//                  .AtomicIncrement(f => f.Value, Val, n);
//            }
//            catch (Exception ex)
//            {
//                IsError = true;
//            }
//            finally
//            {
//                Done = true;
//            }
//        }
//    }
//}
