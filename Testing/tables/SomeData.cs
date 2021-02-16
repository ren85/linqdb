using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Testing.tables
{
    public class SomeData : ISomeData
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

    public interface ISomeData
    {
        int Id { get; set; }
        int PeriodId { get; set; }
        int? ObjectId { get; set; }
        double Value { get; set; }
        double? Normalized { get; set; }
        int? PersonId { get; set; }
        DateTime? Date { get; set; }
        string NameSearch { get; set; }
        int GroupBy { get; set; }
        int GroupBy2 { get; set; }
    }
}
