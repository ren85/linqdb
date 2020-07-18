using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbClientInternal
{   
    public static class Ldb_ext
    {
        public static ClientResult Or<T>()
        {
            var res = new ClientResult();
            res.Type = "or";
            return res;
        }
    }
}
