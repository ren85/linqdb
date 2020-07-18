using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public IDbGroupedQueryable<T, TKey> GroupBy<T, TKey>(IDbQueryable<T> source, Expression<Func<T, TKey>> keySelector)
        {
            CheckTableInfo<T>();
            var res = new IDbGroupedQueryable<T, TKey>() { _db = this };
            res.LDBTree = source.LDBTree;
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            var table_info = GetTableInfo(typeof(T).Name);
            res.LDBTree.GroupingInfo = new GroupByInfo();
            res.LDBTree.GroupingInfo.TableNumber = table_info.TableNumber;
            res.LDBTree.GroupingInfo.ColumnNumber = table_info.ColumnNumbers[name];
            res.LDBTree.GroupingInfo.ColumnType = table_info.Columns[name];
            res.LDBTree.GroupingInfo.ColumnName = name;
            return res;
        }
    }

    public class GroupByInfo
    {
        public short TableNumber { get; set; }
        public short ColumnNumber { get; set; }
        public LinqDbTypes ColumnType { get; set; }
        public string ColumnName { get; set; }
    }
}
