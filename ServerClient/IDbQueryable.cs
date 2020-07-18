using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbClientInternal
{
    public class IDbQueryable<T>
    {
        public Ldb _db { get; set; }
        public List<ClientResult> Result = new List<ClientResult>();
        public LinqdbTransactionInternal LDBTransaction { get; set; }
    }

    public class IDbOrderedQueryable<T>
    {
        public Ldb _db { get; set; }
        public List<ClientResult> Result = new List<ClientResult>();
    }

    public class IDbGroupedQueryable<T>
    {
        public Ldb _db { get; set; }
        public List<ClientResult> Result = new List<ClientResult>();
    }
}
