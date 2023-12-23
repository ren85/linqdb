using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Testing.distributedtables
{
    public class BinaryDataDistributed
    {
        public int Id { get; set; }
        public int Sid { get; set; }
        public int Gid { get; set; }
        public int? Value { get; set; }
        public byte[] Data { get; set; }
    }
}
