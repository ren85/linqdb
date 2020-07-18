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
    class NewColumn2 : ITest
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
                AvgWordLengthDifference = 5.5
            };
            db.Table<Testing.tables2.KaggleClass>().Save(d);
            var d2 = new Testing.tables4.KaggleClass()
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
                AvgWordLengthDifferenceInt = 5
            };
#if (SERVER)
            db.Table<Testing.tables.SomeType>()._internal._db._InternalClearCache();
#endif
            db.Table<Testing.tables4.KaggleClass>().Save(d2);
            var res = db.Table<Testing.tables4.KaggleClass>().SelectEntity();
            if (res.Count() != 2 || res[0].AvgWordLengthDifference != 5.5 || res[0].AvgWordLengthDifferenceInt != 0 || res[1].AvgWordLengthDifference != null || res[1].AvgWordLengthDifferenceInt != 5)
            {
                throw new Exception("Assert failure");
            }
#if (SERVER)
            db.Table<Testing.tables.SomeType>()._internal._db._InternalClearCache();
#endif
            var res2 = db.Table<Testing.tables2.KaggleClass>().SelectEntity();

            if (res2.Count() != 2 || res2[0].AvgWordLengthDifference != 5.5 || res2[1].AvgWordLengthDifference != 0)
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
