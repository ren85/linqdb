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
    class GenericType : ITest
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

            TestGenerics<SomeData>(db, new SomeData());

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

        void TestGenerics<T>(Db db, T item) where T: ISomeData, new()
        {
            db.Table<T>().Save(item);
            var some = db.Table<T>().Where(f => f.Id > 0).SelectEntity();
            if (some.Count() != 1)
            {
                throw new Exception("Assert failure");
            }
            db.Table<T>().Update(f => f.ObjectId, some.Single().Id, 5);
            some = db.Table<T>().Where(f => f.Id > 0).SelectEntity();
            if (some.Single().ObjectId != 5)
            {
                throw new Exception("Assert failure");
            }
            int id = some.Single().Id;
            db.Table<T>().Where(f => f.Id == id).AtomicIncrement(f => f.ObjectId, 1, new T(), null);
            some = db.Table<T>().Where(f => f.Id > 0).SelectEntity();
            if (some.Single().ObjectId != 6)
            {
                throw new Exception("Assert failure");
            }
            db.Table<T>().Delete(some.Single().Id);
            some = db.Table<T>().Where(f => f.Id > 0).SelectEntity();
            if (some.Count() != 0)
            {
                throw new Exception("Assert failure");
            }
            using (var tran = new LinqdbTransaction())
            {
                item.NameSearch = "test";
                item.ObjectId = 5;
                item.PeriodId = 5;
                item.GroupBy = 5;
                db.Table<T>(tran).Save(item);
                tran.Commit();
            }
            some = db.Table<T>().Where(f => f.Id > 0).SelectEntity();
            if (some.Count() != 1)
            {
                throw new Exception("Assert failure");
            }
            some = db.Table<T>().Search(f => f.NameSearch , "test").SelectEntity();
            if (some.Count() != 1)
            {
                throw new Exception("Assert failure");
            }
            some = db.Table<T>().Between(f => f.ObjectId, 5, 5, BetweenBoundaries.BothInclusive).SelectEntity();
            if (some.Count() != 1)
            {
                throw new Exception("Assert failure");
            }
            some = db.Table<T>().Intersect(f => f.ObjectId, new HashSet<int?>() { 5 }).SelectEntity();
            if (some.Count() != 1)
            {
                throw new Exception("Assert failure");
            }
            some = db.Table<T>().Intersect(f => f.ObjectId, new HashSet<int?>() { 5 }).OrderBy(f => f.Id).Skip(0).Take(1).SelectEntity();
            if (some.Count() != 1)
            {
                throw new Exception("Assert failure");
            }
            some = db.Table<T>().Intersect(f => f.ObjectId, new HashSet<int?>() { 5 }).OrderByDescending(f => f.Id).Skip(0).Take(1).SelectEntity();
            if (some.Count() != 1)
            {
                throw new Exception("Assert failure");
            }
#if (!INDEXES)
            db.Table<T>().CreatePropertyMemoryIndex(f => f.PeriodId);
            db.Table<T>().CreateGroupByMemoryIndex(f => f.GroupBy, f => f.PeriodId);
            var res = db.Table<T>().GroupBy(f => f.GroupBy).Select(f => new { Sum = f.Sum(z => z.PeriodId) }).Select(f => f.Sum).Single();
            if (res != 5)
            {
                throw new Exception("Assert failure");
            }
            db.Table<T>().RemoveGroupByMemoryIndex(f => f.GroupBy, f => f.PeriodId);
            db.Table<T>().RemovePropertyMemoryIndex(f => f.PeriodId);
            var indexes = db.GetExistingIndexes();
            if (indexes.Count() != 0)
            {
                throw new Exception("Assert failure");
            }
#endif

        }
    }
}
