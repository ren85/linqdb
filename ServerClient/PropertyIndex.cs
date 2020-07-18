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
        public ClientResult CreatePropertyIndex<T, TKey>(Expression<Func<T, TKey>> valuePropertySelector)
        {
            var res = new ClientResult();
            res.Type = "propertyindex";
            var par = valuePropertySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(valuePropertySelector.Body.ToString());
            res.Selector = name;
            return res;
        }

        public ClientResult RemovePropertyIndex<T, TKey>(Expression<Func<T, TKey>> valuePropertySelector)
        {
            var res = new ClientResult();
            res.Type = "removepropertyindex";
            var par = valuePropertySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(valuePropertySelector.Body.ToString());
            res.Selector = name;
            return res;
        }
    }
}
