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
    class DeletedColumnThenAdded : ITest
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

            var d = new Testing.tables2.KaggleClass()
            {
                Id = 1,
                CommonCount = 1,
                CommonPercentage = 0.5,
                Is_duplicate = 1,
                NormalizedPercentage = 1,
                Q1 = "sadasd",
                Q2 = "dsfdjfkldf",
                Qid1 = 1,
                Qid2 = 2,
                TotalLength = 5,
                SomeNameSearch = "abc def1"
            };
            db.Table<Testing.tables2.KaggleClass>().Save(d);
            var d2 = new Testing.tables2.KaggleClass()
            {
                Id = 2,
                CommonCount = 1,
                CommonPercentage = 0.5,
                Is_duplicate = 1,
                NormalizedPercentage = 1,
                Q1 = "sadasd",
                Q2 = "dsfdjfkldf",
                Qid1 = 1,
                Qid2 = 2,
                TotalLength = 5,
                SomeNameSearch = "abc def2"
            };
            db.Table<Testing.tables2.KaggleClass>().Save(d2);

#if (SERVER)
            db.Table<Testing.tables.SomeType>()._internal._db._InternalClearCache();
#endif

            //delete changed type
            var ids = db.Table<Testing.tables.KaggleClass>().Select(f => new { f.Id }).Select(f => f.Id);
            db.Table<Testing.tables.KaggleClass>().Delete(new HashSet<int>(ids));

            //back to first type
            d = new Testing.tables2.KaggleClass()
            {
                Id = 3,
                CommonCount = 1,
                CommonPercentage = 0.5,
                Is_duplicate = 1,
                NormalizedPercentage = 1,
                Q1 = "sadasd",
                Q2 = "dsfdjfkldf",
                Qid1 = 1,
                Qid2 = 2,
                TotalLength = 5,
                SomeNameSearch = "abc def3"
            };
#if (SERVER)
            db.Table<Testing.tables.SomeType>()._internal._db._InternalClearCache();
#endif
            db.Table<Testing.tables2.KaggleClass>().Save(d);
            d2 = new Testing.tables2.KaggleClass()
            {
                Id = 4,
                CommonCount = 1,
                CommonPercentage = 0.5,
                Is_duplicate = 1,
                NormalizedPercentage = 1,
                Q1 = "sadasd",
                Q2 = "dsfdjfkldf",
                Qid1 = 1,
                Qid2 = 2,
                TotalLength = 5,
                SomeNameSearch = "abc def4"
            };
            db.Table<Testing.tables2.KaggleClass>().Save(d2);

            var total = db.Table<Testing.tables2.KaggleClass>().Count();
            

            var res = db.Table<Testing.tables2.KaggleClass>().Search(f => f.SomeNameSearch, "abc").SelectEntity();
            if (res.Count() != total || total != 2 || res.First(f => f.Id == 4).CommonCount != 1 || res.First(f => f.Id == 4).SomeNameSearch != "abc def4")
            {
                throw new Exception("Assert failure");
            }

            res = db.Table<Testing.tables2.KaggleClass>().SelectEntity();
            if (res.Count() != total || res.First(f => f.Id == 4).CommonCount != 1 || res.First(f => f.Id == 4).SomeNameSearch != "abc def4")
            {
                throw new Exception("Assert failure");
            }

            //back to second type
            var d_ = new Testing.tables.KaggleClass()
            {
                Id = 5,
                CommonCount = 1,
                CommonPercentage = 0.5,
                Is_duplicate = 1,
                NormalizedPercentage = 1,
                Q1 = "sadasd",
                Q2 = "dsfdjfkldf",
                Qid1 = 5,
                Qid2 = 5,
                TotalLength = 5
            };
#if (SERVER)
            db.Table<Testing.tables.SomeType>()._internal._db._InternalClearCache();
#endif
            db.Table<Testing.tables.KaggleClass>().Save(d_);
            var d2_ = new Testing.tables.KaggleClass()
            {
                Id = 6,
                CommonCount = 1,
                CommonPercentage = 0.5,
                Is_duplicate = 1,
                NormalizedPercentage = 1,
                Q1 = "sadasd",
                Q2 = "dsfdjfkldf",
                Qid1 = 6,
                Qid2 = 6,
                TotalLength = 6
            };
            db.Table<Testing.tables.KaggleClass>().Save(d2_);

#if (SERVER)
            db.Table<Testing.tables.SomeType>()._internal._db._InternalClearCache();
#endif
            total = db.Table<Testing.tables2.KaggleClass>().Count();
            res = db.Table<Testing.tables2.KaggleClass>().SelectEntity();
            if (res.Count() != total || total != 4 || res.First(f => f.Id == 4).CommonCount != 1 || res.First(f => f.Id == 4).SomeNameSearch != "abc def4" ||
                res.First(f => f.Id == 5).SomeNameSearch != null || res.First(f => f.Id == 5).Qid1 != 5 ||
                res.First(f => f.Id == 6).SomeNameSearch != null || res.First(f => f.Id == 6).Qid1 != 6)
            {
                throw new Exception("Assert failure");
            }

            d2 = new Testing.tables2.KaggleClass()
            {
                Id = 7,
                CommonCount = 1,
                CommonPercentage = 0.5,
                Is_duplicate = 1,
                NormalizedPercentage = 1,
                Q1 = "sadasd",
                Q2 = "dsfdjfkldf",
                Qid1 = 7,
                Qid2 = 7,
                TotalLength = 5,
                SomeNameSearch = "abc def7"
            };
            db.Table<Testing.tables2.KaggleClass>().Save(d2);

            total = db.Table<Testing.tables2.KaggleClass>().Count();
            res = db.Table<Testing.tables2.KaggleClass>().SelectEntity();
            if (res.Count() != total || total != 5 || res.First(f => f.Id == 4).CommonCount != 1 || res.First(f => f.Id == 4).SomeNameSearch != "abc def4" ||
                res.First(f => f.Id == 5).SomeNameSearch != null || res.First(f => f.Id == 5).Qid1 != 5 ||
                res.First(f => f.Id == 6).SomeNameSearch != null || res.First(f => f.Id == 6).Qid1 != 6 ||
                res.First(f => f.Id == 7).Qid1 != 7 || res.First(f => f.Id == 7).SomeNameSearch != "abc def7")
            {
                throw new Exception("Assert failure");
            }


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