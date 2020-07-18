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
        public ClientResult CreateGroupByMemoryIndex<T, TKey1, TKey2>(Expression<Func<T, TKey1>> groupPropertySelector, Expression<Func<T, TKey2>> valuePropertySelector)
        {
            var res = new ClientResult();
            res.Type = "groupindex";
            var par = groupPropertySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(groupPropertySelector.Body.ToString());
            res.Selector = name;

            par = valuePropertySelector.Parameters.First();
            name = SharedUtils.GetPropertyName(valuePropertySelector.Body.ToString());
            res.Query = name;

            return res;
        }

        public ClientResult RemoveGroupByMemoryIndex<T, TKey1, TKey2>(Expression<Func<T, TKey1>> groupPropertySelector, Expression<Func<T, TKey2>> valuePropertySelector)
        {
            var res = new ClientResult();
            res.Type = "removegroupindex";
            var par = groupPropertySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(groupPropertySelector.Body.ToString());
            res.Selector = name;

            par = valuePropertySelector.Parameters.First();
            name = SharedUtils.GetPropertyName(valuePropertySelector.Body.ToString());
            res.Query = name;

            return res;
        }
    }
}
