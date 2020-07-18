using ServerSharedData;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbClientInternal
{
    public partial class Ldb
    {
        public ClientResult Replicate(string path)
        {
            var res = new ClientResult();
            res.Type = "replicate";
            res.Replicate = path;
            return res;
        }
    }
}
