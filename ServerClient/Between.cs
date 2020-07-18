using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbClientInternal
{
    public partial class Ldb
    {
        public ClientResult Between<T, TKey>(Expression<Func<T, TKey>> keySelector, double from, double to, BetweenBoundariesInternal boundaries = BetweenBoundariesInternal.BothInclusive)
        {
            var res = new ClientResult();
            res.Type = "between";
            res.Boundaries = (short)boundaries;
            res.From = from;
            res.To = to;
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            res.Selector = name;
            return res;
        }
    }
    public enum BetweenBoundariesInternal : int
    {
        BothInclusive,
        FromInclusiveToExclusive,
        FromExclusiveToInclusive,
        BothExclusive
    }
}
