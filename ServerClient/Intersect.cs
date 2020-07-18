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
        public ClientResult Intersect<T, TKey>(Expression<Func<T, TKey>> keySelector, HashSet<int?> set)
        {
            var res = new ClientResult();
            res.Int_set = new HashSet<int>();
            foreach (var s in set)
            {
                if (s == null)
                {
                    res.Int_null = true;
                }
                else
                {
                    res.Int_set.Add((int)s);
                }
            }
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            res.Selector = name;
            res.Type = "intersect";
            return res;
        }
        public ClientResult Intersect<T, TKey>(Expression<Func<T, TKey>> keySelector, HashSet<int> set)
        {
            var res = new ClientResult();
            res.Int_set = new HashSet<int>();
            foreach (var s in set)
            {
                res.Int_set.Add(s);
            }
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            res.Selector = name;
            res.Type = "intersect";
            return res;
        }
        public ClientResult Intersect<T, TKey>(Expression<Func<T, TKey>> keySelector, HashSet<double?> set)
        {
            var res = new ClientResult();
            res.Double_set = new HashSet<double>();
            foreach (var s in set)
            {
                if (s == null)
                {
                    res.Double_null = true;
                }
                else
                {
                    res.Double_set.Add((double)s);
                }
            }
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            res.Selector = name;
            res.Type = "intersect";
            return res;
        }
        public ClientResult Intersect<T, TKey>(Expression<Func<T, TKey>> keySelector, HashSet<double> set)
        {
            var res = new ClientResult();
            res.Double_set = new HashSet<double>();
            foreach (var s in set)
            {
                res.Double_set.Add(s);
            }
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            res.Selector = name;
            res.Type = "intersect";
            return res;
        }
        public ClientResult Intersect<T, TKey>(Expression<Func<T, TKey>> keySelector, HashSet<DateTime?> set)
        {
            var res = new ClientResult();
            res.Date_set = new HashSet<double>();
            foreach (var s in set)
            {
                if (s == null)
                {
                    res.Date_null = true;
                }
                else
                {
                    var val = ((DateTime)s - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
                    res.Date_set.Add(val);
                }
            }
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            res.Selector = name;
            res.Type = "intersect";
            return res;
        }
        public ClientResult Intersect<T, TKey>(Expression<Func<T, TKey>> keySelector, HashSet<DateTime> set)
        {
            var res = new ClientResult();
            res.Date_set = new HashSet<double>();
            foreach (var s in set)
            {
                var val = (s - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
                res.Date_set.Add(val);
            }
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            res.Selector = name;
            res.Type = "intersect";
            return res;
        }
        public ClientResult Intersect<T, TKey>(Expression<Func<T, TKey>> keySelector, HashSet<string> set)
        {
            var res = new ClientResult();
            res.String_set = new HashSet<string>();
            foreach (var s in set)
            {
                if (s == null)
                {
                    res.String_null = true;
                }
                else
                {
                    res.String_set.Add(s);
                }
            }
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            res.Selector = name;
            res.Type = "intersect";
            return res;
        }
    }
}
