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
    class SelectNonAtomically : ITest
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
            db.Table<BinaryData2>().Delete(new HashSet<int>(db.Table<BinaryData2>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif
            int number = 0;
            if (!Environment.Is64BitProcess) //32-bit
            {
                number = 1800;
            }
            else
            {
                number = 3600;
            }
            var list = new List<BinaryData2>();
            int total = 100000;
            for (int i = 1; i <= total; i++)
            {
                var d = new BinaryData2()
                {
                    Id = i,
                    Data1 = Enumerable.Range(1, number / 2).Select(f => (byte)1).ToArray(),
                    Data2 = Enumerable.Range(1, number / 2).Select(f => (byte)1).ToArray(),
                };
                list.Add(d);
            }



            for (int i = 0; i <= 100000; i += 1000)
            {
                db.Table<BinaryData2>().SaveBatch(list.Skip(i).Take(1000).ToList());
            }

            var count = db.Table<BinaryData2>().Count();

            try
            {
                try
                {
                    var res = db.Table<BinaryData2>().Select(f => new { f.Id, f.Data1, f.Data2 });
                    throw new Exception("Assert failure");
                }
                catch (Exception ex)
                {
                    if (!ex.Message.Contains("the query resulted in set larger than"))
                    {
                        throw new Exception("Assert failure");
                    }
                }

                try
                {
                    var res = db.Table<BinaryData2>().SelectNonAtomically(f => new { f.Id, f.Data1, f.Data2 });
                    if (res.Count() != total)
                    {
                        throw new Exception("Assert failure");
                    }
                }
                catch (Exception ex)
                {
                    throw new Exception("Assert failure");
                }

                try
                {
                    var res = db.Table<BinaryData2>().SelectEntity();
                    throw new Exception("Assert failure");
                }
                catch (Exception ex)
                {
                    if (!ex.Message.Contains("the query resulted in set larger than"))
                    {
                        throw new Exception("Assert failure");
                    }
                }

                try
                {
                    var res = db.Table<BinaryData2>().SelectEntityNonAtomically();
                    if (res.Count() != total)
                    {
                        throw new Exception("Assert failure");
                    }
                }
                catch (Exception ex)
                {
                    throw new Exception("Assert failure");
                }
            }
            finally
            {
                db.Table<BinaryData2>().Delete(new HashSet<int>(Enumerable.Range(1, 100000).ToList()));
            }

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
