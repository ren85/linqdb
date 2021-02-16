using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Testing.Queues
{
    [ProtoContract]
    public class QueueA
    {
        [ProtoMember(1)]
        public string SomeString { get; set; }
        [ProtoMember(2)]
        public List<int> SomeArray { get; set; }
    }
}
