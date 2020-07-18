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
    class UpdateBinaryIndex : ITest
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
            db.Table<BinaryData>().Delete(new HashSet<int>(db.Table<BinaryData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
            var d = new BinaryData()
            {
                Id = 1,
                Data = new List<byte>() { 1 }.ToArray()
            };
            db.Table<BinaryData>().Save(d);


            var res = db.Table<BinaryData>()
                        .Where(f => f.Data != null)
                        .Select(f => new
                        {
                            Id = f.Id
                        });
            if (res.Count() != 1 || res[0].Id != 1)
            {
                throw new Exception("Assert failure");
            }

            d.Data = null;

            db.Table<BinaryData>().Save(d);
            res = db.Table<BinaryData>()
                        .Where(f => f.Data != null)
                        .Select(f => new
                        {
                            Id = f.Id
                        });
            if (res.Count() != 0)
            {
                throw new Exception("Assert failure");
            }

            res = db.Table<BinaryData>()
                        .Where(f => f.Data == null)
                        .Select(f => new
                        {
                            Id = f.Id
                        });
            if (res.Count() != 1 || res[0].Id != 1)
            {
                throw new Exception("Assert failure");
            }

            db.Table<BinaryData>().Delete(1);
            res = db.Table<BinaryData>()
                        .Where(f => f.Data == null || f.Data != null)
                        .Select(f => new
                        {
                            Id = f.Id
                        });
            if (res.Count() != 0)
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
