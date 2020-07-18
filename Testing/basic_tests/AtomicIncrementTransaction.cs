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
    class AtomicIncrementTransaction : ITest
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
            db.Table<LogEntry>().Delete(new HashSet<int>(db.Table<LogEntry>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.Table<LangCounter>().Delete(new HashSet<int>(db.Table<LangCounter>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.Table<Question>().Delete(new HashSet<int>(db.Table<Question>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
            try
            {
                using (var tran = new LinqdbTransaction())
                {
                    var n = new LangCounter()
                    {
                        Count = 1,
                        Date = DateTime.Now,
                        Lang = 1,
                        SuccessCount = 1,
                        Is_api = 1
                    };
                    db.Table<LangCounter>(tran)
                      .Where(f => f.Lang == 1)
                      .AtomicIncrement2Props(f => f.Count, z => z.SuccessCount, 1, 1, n);

                    throw new Exception("Assert failure");
                }
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("transactions are not supported with AtomicIncrement"))
                {
                    throw new Exception("Assert failure");
                }
            }

            try
            {
                using (var tran = new LinqdbTransaction())
                {
                    var n = new LangCounter()
                    {
                        Count = 1,
                        Date = DateTime.Now,
                        Lang = 1,
                        SuccessCount = 1,
                        Is_api = 1
                    };
                    db.Table<LangCounter>(tran)
                      .Where(f => f.Lang == 1)
                      .AtomicIncrement(f => f.Count, 1, n, null);

                    throw new Exception("Assert failure");
                }
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("transactions are not supported with AtomicIncrement"))
                {
                    throw new Exception("Assert failure");
                }
            }

            try
            {
                using (var tran = new LinqdbTransaction())
                {
                    var n = new LangCounter()
                    {
                        Count = 1,
                        Date = DateTime.Now,
                        Lang = 1,
                        SuccessCount = 1,
                        Is_api = 1
                    };
                    db.Table<LangCounter>(tran)
                      .Where(f => f.Lang == 1)
                      .AtomicIncrement(f => f.Count, 1, n, 0);

                    throw new Exception("Assert failure");
                }
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("transactions are not supported with AtomicIncrement"))
                {
                    throw new Exception("Assert failure");
                }
            }

            //transaction testing
            int total = 10000;
            using (var trans = new LinqdbTransaction())
            {
                var list = new List<SomeData>();
                for (int i = 1; i <= total; i++)
                {
                    list.Add(new SomeData()
                    {
                        Id = i,
                        NameSearch = i + "",
                        Normalized = i,
                        ObjectId = i,
                        PeriodId = i,
                        Date = DateTime.Now.AddDays(-i),
                        PersonId = i,
                        Value = i
                    });
                }
                db.Table<SomeData>(trans).SaveBatch(list);

                var list2 = new List<Question>();
                for (int i = 1; i <= total; i++)
                {
                    list2.Add(new Question()
                    {
                        Id = i,
                        Body = BitConverter.GetBytes(i),
                        AcceptedAnswerId = i,
                        AnswerCount = i,
                        CommentCount = i,
                        OwnerUserId = i,
                        CreationDate = DateTime.Now.AddDays(-i)
                    });
                }
                db.Table<Question>(trans).SaveBatch(list2);

                trans.Commit();
            }
            var res1 = db.Table<SomeData>().Count();
            if (res1 != total)
            {
                throw new Exception("Assert failure");
            }
            res1 = db.Table<Question>().Count();
            if (res1 != total)
            {
                throw new Exception("Assert failure");
            }

            int left = total;
            using (var trans = new LinqdbTransaction())
            {
                for (int i = 1; i <= total; i++)
                {
                    if (i % 2 == 0)
                    {
                        db.Table<SomeData>(trans).Delete(i);
                        db.Table<Question>(trans).Delete(i);
                        left--;
                    }
                }
                trans.Commit();
            }
            res1 = db.Table<SomeData>().Count();
            if (res1 != left)
            {
                throw new Exception("Assert failure");
            }
            res1 = db.Table<Question>().Count();
            if (res1 != left)
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