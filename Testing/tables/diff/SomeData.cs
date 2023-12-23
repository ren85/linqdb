using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Testing.tables.diff
{
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
