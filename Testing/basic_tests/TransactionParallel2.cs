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
    class TransactionsParallel2 : ITest
    {
        bool InsertInTransaction<T>(LinqdbTransaction transaction, Db db) where T : new()
        {
            try
            {
                var d = new T();
                db.Table<T>(transaction).Save(d);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }
        public void Do(Db db)
        {
            bool dispose = false; if (db == null) { db = new Db("DATA"); dispose = true; }
            using (var tr = new LinqdbTransaction())
            {
                var tasks = new List<Task<bool>>();
                var t1 = Task.Run<bool>(() =>
                {
                    return InsertInTransaction<SomeData>(tr, db);
                });
                tasks.Add(t1);
                var t2 = Task.Run<bool>(() =>
                {
                    return InsertInTransaction<BinaryData>(tr, db);
                });
                tasks.Add(t2);
                var t3 = Task.Run<bool>(() =>
                {
                    return InsertInTransaction<SomeType>(tr, db);
                });
                tasks.Add(t3);
                var t4 = Task.Run<bool>(() =>
                {
                    return InsertInTransaction<Answer>(tr, db);
                });
                tasks.Add(t4);

                tasks.ForEach(f => f.Wait());

                tr.Commit();
                if (db.Table<SomeData>().SelectEntity().Count() != 1 || db.Table<BinaryData>().SelectEntity().Count() != 1 ||
                    db.Table<SomeType>().SelectEntity().Count() != 1 || db.Table<Answer>().SelectEntity().Count() != 1)
                {
                    throw new Exception("Assert failure");
                }
            }
            if (dispose) { db.Dispose(); }
            if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
