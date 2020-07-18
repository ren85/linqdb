using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Testing.tables
{
    public class LogEntry
    {
        public int Id { get; set; }
        public string DataSearch { get; set; }
        public string Result { get; set; }
        public string Input { get; set; }
        public string Compiler_args { get; set; }
        public int Lang { get; set; }
        public int Is_api { get; set; }
        public DateTime Time { get; set; }
        public int Is_success { get; set; }
        public string Lang_string_ { get; set; }
        public string Is_api_string_ { get; set; }
    }
    public class LangCounter
    {
        public int Id { get; set; }
        public int Lang { get; set; }
        public DateTime Date { get; set; }
        public int Count { get; set; }
        public int SuccessCount { get; set; }
        public int Is_api { get; set; }
    }
}
