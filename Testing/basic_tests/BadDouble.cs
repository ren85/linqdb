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
    class BadDouble : ITest
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
                Normalized = BitConverter.ToDouble(new byte[8] { 0,0,0,0,0,0,0,128}, 0),
            };
            db.Table<SomeData>().Save(d);
            var res = db.Table<SomeData>().SelectEntity();
            var bs = BitConverter.GetBytes((double)res.First().Normalized);
            if (bs[0] != 0 || bs[1] != 0 || bs[2] != 0 || bs[3] != 0 || bs[4] != 0 || bs[5] != 0 || bs[6] != 0 || bs[7] != 0)
            {
                throw new Exception("Assert failure");
            }

            try
            {
                d = new SomeData()
                {
                    Id = 1,
                    Normalized = double.NaN,
                };
                db.Table<SomeData>().Save(d);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("double with value NaN is not supported"))
                {
                    throw new Exception("Assert failure");
                }
            }

            try
            {
                d = new SomeData()
                {
                    Id = 1,
                    Normalized = double.PositiveInfinity,
                };
                db.Table<SomeData>().Save(d);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("double with value PositiveInfinity is not supported"))
                {
                    throw new Exception("Assert failure");
                }
            }

            try
            {
                d = new SomeData()
                {
                    Id = 1,
                    Normalized = double.NegativeInfinity,
                };
                db.Table<SomeData>().Save(d);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("double with value NegativeInfinity is not supported"))
                {
                    throw new Exception("Assert failure");
                }
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
