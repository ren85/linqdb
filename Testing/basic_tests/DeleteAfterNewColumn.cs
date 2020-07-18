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
    class DeleteAfterNewColumn : ITest
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

            var d = new Testing.tables.KaggleClass()
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
                TotalLength = 5
            };
            db.Table<Testing.tables.KaggleClass>().Save(d);
            var d2 = new Testing.tables.KaggleClass()
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
            };
            db.Table<Testing.tables.KaggleClass>().Save(d2);

#if (SERVER)
            db.Table<Testing.tables.SomeType>()._internal._db._InternalClearCache();
#endif

            var ids = db.Table<Testing.tables2.KaggleClass>().Select(f => new { f.Id }).Select(f => f.Id);
            db.Table<Testing.tables2.KaggleClass>().Delete(new HashSet<int>(ids));

            var res = db.Table<Testing.tables2.KaggleClass>().SelectEntity();
            if (res.Count() != 0)
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
