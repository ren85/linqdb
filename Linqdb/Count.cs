using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public int Count<T>(QueryTree tree)
        {
            if((tree.BetweenInfo == null || !tree.BetweenInfo.Any()) &&
               (tree.IntersectInfo == null || !tree.IntersectInfo.Any()) && 
               (tree.SearchInfo == null || !tree.SearchInfo.Any()) && 
               (tree.WhereInfo == null || !tree.WhereInfo.Any()))
            {
                CheckTableInfo<T>();
                var table_info = GetTableInfo(typeof(T).Name);
                return GetTableRowCount(table_info, null);
            }
            else
            {
                CheckTableInfo<T>();
                using (var snapshot = leveld_db.CreateSnapshot())
                {
                    var ro = new ReadOptions().SetSnapshot(snapshot);
                    var table_info = GetTableInfo(typeof(T).Name);
                    var where_res = CalculateWhereResult<T>(tree, table_info, ro);
                    where_res = FindBetween(tree, where_res, ro);
                    where_res = Intersect(tree, where_res, table_info, tree.QueryCache, ro);
                    where_res = Search(tree, where_res, ro);
                    var fres = CombineData(where_res);
                    return fres.All ? GetTableRowCount(table_info, ro) : fres.ResIds.Count();
                }
            }
        }
    }
}
