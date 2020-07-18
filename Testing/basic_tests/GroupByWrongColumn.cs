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
    class GroupByWrongColumn : ITest
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

            var d = new SomeData()
            {
                Id = 1,
                Normalized = 1.2,
                GroupBy = 5,
                NameSearch = "a"
            };
            db.Table<SomeData>().Save(d);

            d = new SomeData()
            {
                Id = 2,
                Normalized = 7,
                GroupBy = 3,
                NameSearch = "a"
            };
            db.Table<SomeData>().Save(d);

            d = new SomeData()
            {
                Id = 3,
                Normalized = 2.3,
                GroupBy = 10,
                NameSearch = "a"
            };
            db.Table<SomeData>().Save(d);
            d = new SomeData()
            {
                Id = 4,
                Normalized = 4.5,
                GroupBy = 10,
                NameSearch = "b"
            };
            db.Table<SomeData>().Save(d);


            try
            {
                db.Table<SomeData>().CreateGroupByMemoryIndex(f => f.NameSearch, z => z.Id);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("type is not supported as group by column"))
                {
                    throw new Exception("Assert failure");
                }
            }

            try
            {
                db.Table<SomeData>().CreateGroupByMemoryIndex(f => f.Value, z => z.Id);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("type is not supported as group by column"))
                {
                    throw new Exception("Assert failure");
                }
            }

            try
            {
                db.Table<SomeData>().CreateGroupByMemoryIndex(f => f.Date, z => z.Id);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("type is not supported as group by column"))
                {
                    throw new Exception("Assert failure");
                }
            }


            db.Table<SomeData>().CreateGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);
            try
            {
                db.Table<SomeData>().GroupBy(f => f.PeriodId).Select(f => new { f.Key });
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("does not exist."))
                {
                    throw new Exception("Assert failure");
                }
            }
            db.Table<SomeData>().RemovePropertyMemoryIndex(f => f.Value);
            try
            {
                var res = db.Table<SomeData>().GroupBy(f => f.GroupBy).Select(f => new { f.Key, Sum = f.Sum(z => z.Value) });
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("does not exist."))
                {
                    throw new Exception("Assert failure");
                }
            }
            try
            {
                var res = db.Table<SomeData>().GroupBy(f => f.GroupBy2).Select(f => new { f.Key, Sum = f.Sum(z => z.Normalized) });
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("does not exist."))
                {
                    throw new Exception("Assert failure");
                }
            }

            db.Table<SomeData>().RemoveGroupByMemoryIndex(f => f.GroupBy, z => z.Normalized);

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
