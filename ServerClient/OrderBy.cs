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
        public ClientResult OrderBy<T, TKey>(Expression<Func<T, TKey>> keySelector)
        {
            var res = new ClientResult();
            res.Type = "order";
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            res.Selector = name;
            return res;
        }
        public ClientResult OrderByDescending<T, TKey>(Expression<Func<T, TKey>> keySelector)
        {
            var res = new ClientResult();
            res.Type = "orderdesc";
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            res.Selector = name;
            return res;
        }
       
        public ClientResult Skip<T>(int count)
        {
            var res = new ClientResult();
            res.Type = "skip";
            res.Skip = count;
            return res;
        }
        public ClientResult Take<T>(int count)
        {
            var res = new ClientResult();
            res.Type = "take";
            res.Take = count;
            return res;
        }
    }
}
