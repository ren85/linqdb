#if (SERVER)
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

namespace Testing.basic_tests
{
#if (!SOCKETS)
    class TypeChanges : ITest
    {
        public void Do(Db db_unused)
        {
            if (db_unused != null)
            {
#if (SERVER)
                Logic.Dispose();
#else
                db_unused.Dispose();
#endif
                if (Directory.Exists("DATA"))
                {
                    ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA");
                }
            }

            var db = new Db("DATA");

#if (SERVER)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif

            var d = new Testing.tables.SomeType()
            {
                Name = "test1",
                PeriodId = 1,
                Value = 1.1
            };

#if (SERVER)
            db.Table<Testing.tables.SomeType>()._internal._db._InternalClearCache();
#endif
            db.Table<Testing.tables.SomeType>().Save(d);

            var res = db.Table<Testing.tables.SomeType>()
                        .Where(f => f.Id == d.Id)
                        .Select(f => new
                        {
                            Id = f.Id,
                            PeriodId = f.PeriodId
                        });
            if (res.Count() != 1 || res[0].Id != 1 || res[0].PeriodId != 1)
            {
                throw new Exception("Assert failure");
            }

            //remove double column

            var d_new = new Testing.tables2.SomeType()
            {
                Name = "test1",
                PeriodId = 1,
            };
#if (SERVER)
            db.Table<Testing.tables.SomeType>()._internal._db._InternalClearCache();
#endif
            db.Table<Testing.tables2.SomeType>().Save(d_new);
#if (INDEXES)
            try
            {
                db.Table<Testing.tables.SomeType>().CreatePropertyMemoryIndex(f => f.Value);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("has gaps in data"))
                {
                    throw new Exception("Assert failure");
                }
            }
            try
            {
                db.Table<Testing.tables.SomeType>().CreateGroupByMemoryIndex(f => f.Id, f => f.Value);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("has gaps in data"))
                {
                    throw new Exception("Assert failure");
                }
            }
#endif
            //add it back
            d.Id = 3;
#if (SERVER)
            db.Table<Testing.tables.SomeType>()._internal._db._InternalClearCache();
#endif
            db.Table<Testing.tables.SomeType>().Save(d);
            var res2 = db.Table<Testing.tables.SomeType>()
                        .Where(f => f.Id == 3)
                        .Select(f => new
                        {
                            Id = f.Id,
                            PeriodId = f.PeriodId,
                            Value = f.Value
                        });
            if (res2.Count() != 1 || res2[0].Id != 3 || res2[0].Value != 1.1)
            {
                throw new Exception("Assert failure");
            }
#if (INDEXES)
            try
            {
                db.Table<Testing.tables.SomeType>().CreatePropertyMemoryIndex(f => f.Value);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("has gaps in data"))
                {
                    throw new Exception("Assert failure");
                }
            }
            try
            {
                db.Table<Testing.tables.SomeType>().CreateGroupByMemoryIndex(f => f.Id, f => f.Value);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("has gaps in data"))
                {
                    throw new Exception("Assert failure");
                }
            }
#endif
            //remove string column
            var d_new_no_string = new Testing.tables3.SomeType()
            {
                PeriodId = 1,
            };
#if (SERVER)
            db.Table<Testing.tables.SomeType>()._internal._db._InternalClearCache();
#endif
            db.Table<Testing.tables3.SomeType>().Save(d_new_no_string);

            //add it back
            d.Id = 3;
#if (SERVER)
            db.Table<Testing.tables.SomeType>()._internal._db._InternalClearCache();
#endif
            db.Table<Testing.tables.SomeType>().Save(d);
            var res3 = db.Table<Testing.tables.SomeType>()
                        .Where(f => f.Id == 3)
                        .Select(f => new
                        {
                            Id = f.Id,
                            PeriodId = f.PeriodId,
                            Value = f.Value,
                            Name = f.Name
                        });
            if (res3.Count() != 1 || res3[0].Id != 3 || res3[0].Value != 1.1 || res3[0].Name != "test1")
            {
                throw new Exception("Assert failure");
            }


            //remove int column
            var d_new_no_int = new Testing.tables4.SomeType()
            {
                Name = "4",
            };
#if (SERVER)
            db.Table<Testing.tables.SomeType>()._internal._db._InternalClearCache();
#endif
            db.Table<Testing.tables4.SomeType>().Save(d_new_no_int);
#if (INDEXES)
            try
            {
                db.Table<Testing.tables.SomeType>().CreatePropertyMemoryIndex(f => f.PeriodId);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("has gaps in data"))
                {
                    throw new Exception("Assert failure");
                }
            }
            try
            {
                db.Table<Testing.tables.SomeType>().CreateGroupByMemoryIndex(f => f.PeriodId, f => f.Id);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("has gaps in data"))
                {
                    throw new Exception("Assert failure");
                }
            }
            try
            {
                db.Table<Testing.tables.SomeType>().CreateGroupByMemoryIndex(f => f.Id, f => f.PeriodId);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("has gaps in data"))
                {
                    throw new Exception("Assert failure");
                }
            }
#endif
            //add it back
            d.Id = 3;
#if (SERVER)
            db.Table<Testing.tables.SomeType>()._internal._db._InternalClearCache();
#endif
            db.Table<Testing.tables.SomeType>().Save(d);
            var res4 = db.Table<Testing.tables.SomeType>()
                        .Where(f => f.Id == 3)
                        .Select(f => new
                        {
                            Id = f.Id,
                            PeriodId = f.PeriodId,
                            Value = f.Value,
                            Name = f.Name
                        });
            if (res3.Count() != 1 || res3[0].Id != 3 || res3[0].Value != 1.1 || res3[0].PeriodId != 1)
            {
                throw new Exception("Assert failure");
            }
#if (INDEXES)
            try
            {
                db.Table<Testing.tables.SomeType>().CreatePropertyMemoryIndex(f => f.PeriodId);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("has gaps in data"))
                {
                    throw new Exception("Assert failure");
                }
            }
            try
            {
                db.Table<Testing.tables.SomeType>().CreateGroupByMemoryIndex(f => f.PeriodId, f => f.Id);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("has gaps in data"))
                {
                    throw new Exception("Assert failure");
                }
            }
            try
            {
                db.Table<Testing.tables.SomeType>().CreateGroupByMemoryIndex(f => f.Id, f => f.PeriodId);
                throw new Exception("Assert failure");
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("has gaps in data"))
                {
                    throw new Exception("Assert failure");
                }
            }
#endif

#if (SERVER)
            Logic.Dispose();
#else
            db.Dispose();
#endif
            ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); 
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
#endif
}