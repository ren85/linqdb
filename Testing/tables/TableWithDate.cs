using System;
using System.Collections.Generic;
using System.Text;

namespace Testing.tables
{
    public class TableWithDate : ITable
    {
        public int Id { get; set; }
        public int CommonIntValue { get; set; }
        public string CommonStringValue { get; set; }

        public DateTime Date { get; set; }
    }
}
