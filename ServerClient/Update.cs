using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LinqDbClientInternal
{
    public partial class Ldb
    {
        public ClientResult Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, int?> values)
        {
            var data = new Dictionary<int, byte[]>();
            foreach (var val in values)
            {
                data[val.Key] = val.Value == null ? null : BitConverter.GetBytes((int)val.Value);
            }
            return GenericUpdate<T, TKey>(keySelector, data);
        }
        public ClientResult Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, int> values)
        {
            var data = new Dictionary<int, byte[]>();
            foreach (var val in values)
            {
                data[val.Key] = BitConverter.GetBytes(val.Value);
            }
            return GenericUpdate<T, TKey>(keySelector, data);
        }
        public ClientResult Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, double?> values)
        {
            var data = new Dictionary<int, byte[]>();
            foreach (var val in values)
            {
                data[val.Key] = val.Value == null ? null : BitConverter.GetBytes((double)val.Value);
            }
            return GenericUpdate<T, TKey>(keySelector, data);
        }
        public ClientResult Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, double> values)
        {
            var data = new Dictionary<int, byte[]>();
            foreach (var val in values)
            {
                data[val.Key] = BitConverter.GetBytes(val.Value);
            }
            return GenericUpdate<T, TKey>(keySelector, data);
        }
        public ClientResult Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, DateTime?> values)
        {
            var data = new Dictionary<int, byte[]>();
            foreach (var val in values)
            {
                data[val.Key] = val.Value == null ? null : BitConverter.GetBytes(((DateTime)val.Value - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds);
            }
            return GenericUpdate<T, TKey>(keySelector, data);
        }
        public ClientResult Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, DateTime> values)
        {
            var data = new Dictionary<int, byte[]>();
            foreach (var val in values)
            {
                data[val.Key] = BitConverter.GetBytes((val.Value - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds);
            }
            return GenericUpdate<T, TKey>(keySelector, data);
        }
        public ClientResult Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, byte[]> values)
        {
            var data = new Dictionary<int, byte[]>();
            foreach (var val in values)
            {
                data[val.Key] = val.Value == null ? null : val.Value;
            }
            return GenericUpdate<T, TKey>(keySelector, data);
        }
        public ClientResult Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, string> values)
        {
            var data = new Dictionary<int, byte[]>();
            foreach (var val in values)
            {
                data[val.Key] = val.Value == null ? null : Encoding.UTF8.GetBytes((string)val.Value);
            }
            return GenericUpdate<T, TKey>(keySelector, data);
        }
        public ClientResult GenericUpdate<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, byte[]> UpdateData)
        {
            var res = new ClientResult();
            StringBuilder up_sb = new StringBuilder();
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            res.Selector = name;
            res.Type = "update";
            res.UpdateData = UpdateData;
            return res;
        }

        public ClientResult GenericUpdate(string keySelector, Dictionary<int, byte[]> UpdateData)
        {
            var res = new ClientResult();
            StringBuilder up_sb = new StringBuilder();
            res.Selector = keySelector;
            res.Type = "update";
            res.UpdateData = UpdateData;
            return res;
        }
    }
}
