using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbClientInternal
{
    public class IDbQueueQueryable<T>
    {
        public Ldb _db { get; set; }
        public List<ClientResult> Result = new List<ClientResult>();
    }
}
