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
        public ClientResult Search<T, TKey>(Expression<Func<T, TKey>> keySelector, string search_query, bool partial, int? start_step, int? steps)
        {
            if (start_step != null && steps == null || start_step == null && steps != null)
            {
                throw new LinqDbException("Linqdb: start_step and steps parameters must be used together.");
            }

            var res = new ClientResult();
            res.Type = "search";
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            res.Selector = name;
            res.Query = search_query;
            res.Start_step = start_step;
            res.Steps = steps;
            res.Double_null = partial;
            return res;            
        }
    }
}
