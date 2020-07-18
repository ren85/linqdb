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
    class MaxBinaryAndString : ITest
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
#endif
            StringBuilder sb = new StringBuilder(1024 * 1024 + 10);
            for (int i = 0; i < 1024 * 1024 + 5; i++)
            {
                sb.Append("a");
            }

            var d = new SomeData()
            {
                NameSearch = sb.ToString()
            };
            try
            {
                db.Table<SomeData>().Save(d);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("max size is 1Mb"))
                {
                    throw new Exception("Assert failure");
                }
            }

            d = new SomeData()
            {
                Id = 1
            };
            db.Table<SomeData>().Save(d);
            try
            {
                db.Table<SomeData>().Update(f => f.NameSearch, 1, sb.ToString());
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("max size is 1Mb"))
                {
                    throw new Exception("Assert failure");
                }
            }

            var list = new List<byte>(1024 * 1024 + 10);
            for (int i = 0; i < 1024 * 1024 + 5; i++)
            {
                list.Add((byte)1);
            }
            var b = new BinaryData()
            {
                Data = list.ToArray()
            };
            try
            {
                db.Table<BinaryData>().Save(b);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("max size is 1Mb"))
                {
                    throw new Exception("Assert failure");
                }
            }

            b = new BinaryData()
            {
                Id = 1
            };
            db.Table<BinaryData>().Save(b);
            try
            {
                db.Table<BinaryData>().Update(f => f.Data, 1, list.ToArray());
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("max size is 1Mb"))
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