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

namespace Testing2
{
    public class DiffTest
    {
        public void Do(Db db)
        {
            var res = db.Table<SomeData>().Select(f => new { f.Id }).ToList();
        }
    }

    public class SomeData 
    {
        public int Id { get; set; }
        public int PeriodId { get; set; }
        public int? ObjectId { get; set; }
        public double Value { get; set; }
        public double? Normalized { get; set; }
        public int? PersonId { get; set; }
        public DateTime? Date { get; set; }
        public string NameSearch { get; set; }
        public string NameSearchS { get; set; }
        public int GroupBy { get; set; }
        public int GroupBy2 { get; set; }
        public string NotSearchable { get; set; }
    }
}
