using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Testing.tables
{
    class KaggleClass
    {
        public int Id { get; set; }
        public int Qid1 { get; set; }
        public int Qid2 { get; set; }
        public string Q1 { get; set; }
        public string Q2 { get; set; }
        public int? Is_duplicate { get; set; }

        public int TotalLength { get; set; }
        public int CommonCount { get; set; }
        public double CommonPercentage { get; set; }
        public double NormalizedPercentage { get; set; }
    }
}

namespace Testing.tables2
{
    class KaggleClass
    {
        public int Id { get; set; }
        public int Qid1 { get; set; }
        public int Qid2 { get; set; }
        public string Q1 { get; set; }
        public string Q2 { get; set; }
        public int? Is_duplicate { get; set; }

        public int TotalLength { get; set; }
        public int CommonCount { get; set; }
        public double CommonPercentage { get; set; }
        public double NormalizedPercentage { get; set; }
        public double AvgWordLengthDifference { get; set; }
        public string SomeNameSearch { get; set; }
    }
}

namespace Testing.tables3
{
    class KaggleClass
    {
        public int Id { get; set; }
        public int Qid1 { get; set; }
        public int Qid2 { get; set; }
        public string Q1 { get; set; }
        public string Q2 { get; set; }
        public int? Is_duplicate { get; set; }

        public int TotalLength { get; set; }
        public int CommonCount { get; set; }
        public double CommonPercentage { get; set; }
        public double NormalizedPercentage { get; set; }
        public double? AvgWordLengthDifference { get; set; }
    }
}
namespace Testing.tables4
{
    class KaggleClass
    {
        public int Id { get; set; }
        public int Qid1 { get; set; }
        public int Qid2 { get; set; }
        public string Q1 { get; set; }
        public string Q2 { get; set; }
        public int? Is_duplicate { get; set; }

        public int TotalLength { get; set; }
        public int CommonCount { get; set; }
        public double CommonPercentage { get; set; }
        public double NormalizedPercentage { get; set; }
        public double? AvgWordLengthDifference { get; set; }
        public int AvgWordLengthDifferenceInt { get; set; }
    }
}