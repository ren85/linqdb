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
        public string User { get; set; }
        public string Pass { get; set; }
        public ClientResult GetIds<T>()
        {
            var res = new ClientResult();
            res.Type = "getids";
            return res;
        }
    }
}
