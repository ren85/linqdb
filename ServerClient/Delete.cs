using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace LinqDbClientInternal
{
    public partial class Ldb
    {
        public ClientResult DeleteBatch(HashSet<int> ids)
        {
            var res = new ClientResult();
            res.DeleteIds = ids;
            res.Type = "delete";
            return res;
        }
    }
}
