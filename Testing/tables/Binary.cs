using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Testing.tables
{
    public class BinaryData
    {
        public int Id { get; set; }
        public int? Value { get; set; }
        public byte[] Data { get; set; }
    }

    public class BinaryData2
    {
        public int Id { get; set; }
        public int? Value { get; set; }
        public byte[] Data1 { get; set; }
        public byte[] Data2 { get; set; }
    }
}
