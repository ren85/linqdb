#if (SERVER)
using LinqdbClient;
using ServerLogic;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
    class Config : ITest
    {
        public void Do(Db db_)
        {
            bool dispose = true;
            //no such user
            try
            {
                var db = new Db("DATA", "reader_user_123", "re@der123");
                db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
                var res = db.Table<Testing.tables3.KaggleClass>().SelectEntity();
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("user doesn't exist"))
                {
                    throw new Exception("Assert failure");
                }
            }
            finally
            {
                if(dispose) { Logic.Dispose(); }
            }


            //bad password
            try
            {
                var db = new Db("DATA", "admin", "admina");
                db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
                var d = new SomeData()
                {
                    Id = 1,
                    Normalized = 1.2,
                    PeriodId = 5
                };
                db.Table<SomeData>().Save(d);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("insufficient permissions"))
                {
                    throw new Exception("Assert failure");
                }
            }
            finally
            {
                if(dispose) { Logic.Dispose(); }
            }


            //reader wants to write
            try
            {
                var db = new Db("DATA", "reader_user", "re@der123");
                db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
                var d = new SomeData()
                {
                    Id = 1,
                    Normalized = 1.2,
                    PeriodId = 5
                };
                db.Table<SomeData>().Save(d);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("insufficient permissions"))
                {
                    throw new Exception("Assert failure");
                }
            }
            finally
            {
                if(dispose) { Logic.Dispose(); }
            }

            //writer wants to write
            try
            {
                var db = new Db("DATA", "writer_user", "wr1ter123");
                db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
                var d = new SomeData()
                {
                    Id = 1,
                    Normalized = 1.2,
                    PeriodId = 5
                };
                db.Table<SomeData>().Save(d);
            }
            finally
            {
                if(dispose) { Logic.Dispose(); }
            }

            //reader wants to read
            try
            {
                var db = new Db("DATA", "reader_user", "re@der123");
                db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
                var count = db.Table<SomeData>().Count();
            }
            finally
            {
                if(dispose) { Logic.Dispose(); }
            }

            //writer wants to replicate
            try
            {
                var db = new Db("DATA", "writer_user", "wr1ter123");
                db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
                db.Replicate("DATA2");
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("insufficient permissions"))
                {
                    throw new Exception("Assert failure");
                }
            }
            finally
            {
                if(dispose) { Logic.Dispose(); }
            }
            

            //admin wants to replicate
            try
            {
                var db = new Db("DATA", "admin_user", "@admin123");
                db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
                if (Directory.Exists("DATA2"))
                {
                    Directory.Delete("DATA2", true);
                }
                db.Replicate("DATA2");
            }
            finally
            {
                if(dispose) { Logic.Dispose(); }
            }
            //admin wants to write
            try
            {
                var db = new Db("DATA", "admin_user", "@admin123");
                db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
                var d = new SomeData()
                {
                    Id = 1,
                    Normalized = 1.2,
                    PeriodId = 5
                };
                db.Table<SomeData>().Save(d);
            }
            finally
            {
                if(dispose) { Logic.Dispose(); }
            }


            //reader wants to modify table
            //try
            //{
            //    var db = new Db("DATA", "reader_user", "re@der123");
            //    db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
            //    var res = db.Table<Testing.tables3.SomeData>().SelectEntity();
            //    throw new Exception("Assert failure");
            //}
            //catch (Exception ex)
            //{
            //    if (!ex.Message.Contains("insufficient permissions"))
            //    {
            //        throw new Exception("Assert failure");
            //    }
            //}
            //finally
            //{
            //    if(dispose) { Logic.Dispose(); }
            //}

            //reader wants to create new type
            //try
            //{
            //    var db = new Db("DATA", "reader_user", "re@der123");
            //    db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
            //    var res = db.Table<Testing.tables3.KaggleClass>().SelectEntity();
            //    throw new Exception("Assert failure");
            //}
            //catch (Exception ex)
            //{
            //    if (!ex.Message.Contains("insufficient permissions"))
            //    {
            //        throw new Exception("Assert failure");
            //    }
            //}
            //finally
            //{
            //    if(dispose) { Logic.Dispose(); }
            //}

            if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
            Directory.Delete("DATA2", true);
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
#endif