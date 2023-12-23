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

namespace Testing.basic_tests.diff
{
    public class DiffTest
    {
        public void Do(Db db)
        {
            var res = db.Table<SomeData>().Select(f => new { f.Id }).ToList();
        }
    }
}
