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
    class EmptyString : ITest
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
            
            // empty string and empty array is the same as null

            var d = new SomeData()
            {
                Id = 1,
                NameSearch = ""
            };
            db.Table<SomeData>().Save(d);
           
            var res = db.Table<SomeData>()
                        .Where(f => f.NameSearch == "")
                        .Select(f => new
                        {
                            Name = f.NameSearch
                        });
            if (res.Count() != 1 || res[0].Name != null)
            {
                throw new Exception("Assert failure");
            }
            res = db.Table<SomeData>()
                        .Where(f => f.NameSearch == null)
                        .Select(f => new
                        {
                            Name = f.NameSearch
                        });
            if (res.Count() != 1 || res[0].Name != null)
            {
                throw new Exception("Assert failure");
            }
            res = db.Table<SomeData>()
                        .Where(f => f.NameSearch != "")
                        .Select(f => new
                        {
                            Name = f.NameSearch
                        });
            if (res.Count() != 0)
            {
                throw new Exception("Assert failure");
            }
            res = db.Table<SomeData>()
                        .Where(f => f.NameSearch != null)
                        .Select(f => new
                        {
                            Name = f.NameSearch
                        });
            if (res.Count() != 0)
            {
                throw new Exception("Assert failure");
            }

            var b = new BinaryData()
            {
                Id = 1,
                Data = new byte[0]
            };
            db.Table<BinaryData>().Save(b);

            var res2 = db.Table<BinaryData>()
                        .Where(f => f.Data == null)
                        .Select(f => new
                        {
                            Data = f.Data
                        });
            if (res2.Count() != 1 || res2[0].Data != null)
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
