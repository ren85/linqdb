using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Testing.tables2
{
    public class SomeData
    {
        public int Id { get; set; }
        public double PeriodId { get; set; } //changed type
        public int? ObjectId { get; set; }
        public double Value { get; set; }
        public double? Normalized { get; set; }
        public int? PersonId { get; set; }
        public DateTime? Date { get; set; }
        public string Name { get; set; }
    }
}
