using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbClientInternal
{
    public partial class Ldb
    {
        public ClientResult GroupBy<T, TKey>(Expression<Func<T, TKey>> keySelector)
        {
            var res = new ClientResult();
            res.Type = "groupby";
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            res.Selector = name;
            return res;
        }
    }
}
