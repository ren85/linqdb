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
        public ClientResult AtomicIncrement<T, TKey>(Expression<Func<T, TKey>> keySelector, int value, T item, Dictionary<string, string> def, Dictionary<string, short> order, int? only_if_old_equal)
        {
            var res = new ClientResult();
            res.Type = "increment";
            res.Data = GetData<T>(new List<T>() { item }, def, order);
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            res.Selector = name;
            res.Inc_val = value;
            res.Inc_old_val = only_if_old_equal;
            return res;
        }

        //public ClientResult AtomicIncrement<T, TKey>(Expression<Func<T, TKey>> keySelector, int value, T item, Dictionary<string, string> def, Dictionary<string, short> order)
        //{
        //    var res = new ClientResult();
        //    res.Type = "increment1";
        //    res.Data = GetData<T>(new List<T>() { item }, def, order);
        //    var par = keySelector.Parameters.First();
        //    var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
        //    res.Selector = name;
        //    res.Inc_val = value;
        //    res.Inc_old_val = null;
        //    return res;
        //}

        public ClientResult AtomicIncrement2Props<T, TKey1, TKey2>(Expression<Func<T, TKey1>> keySelector1, Expression<Func<T, TKey2>> keySelector2, int value1, int value2, T item, Dictionary<string, string> def, Dictionary<string, short> order)
        {
            var res = new ClientResult();
            res.Type = "increment2";
            res.Data = GetData<T>(new List<T>() { item }, def, order);
            var par1 = keySelector1.Parameters.First();
            var name1 = SharedUtils.GetPropertyName(keySelector1.Body.ToString());
            var par2 = keySelector2.Parameters.First();
            var name2 = SharedUtils.GetPropertyName(keySelector2.Body.ToString());
            res.Selector = name1+"|"+name2+"|"+value1+"|"+value2;
            return res;
        }
    }
}
