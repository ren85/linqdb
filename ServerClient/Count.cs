using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbClientInternal
{
    public partial class Ldb
    {
        public ClientResult Count<T>()
        {
            var res = new ClientResult();
            res.Type = "count";
            return res;
        }
    }
}
