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
        public Tuple<List<int>, bool> GetIds<T>(QueryTree tree)
        {
            if ((tree.BetweenInfo == null || !tree.BetweenInfo.Any()) &&
               (tree.IntersectInfo == null || !tree.IntersectInfo.Any()) &&
               (tree.SearchInfo == null || !tree.SearchInfo.Any()) &&
               (tree.WhereInfo == null || !tree.WhereInfo.Any()))
            {
                //.Where(f => f.Id > 0)
                CheckTableInfo<T>();
                var table_info = GetTableInfo(typeof(T).Name);
                var op1 = new Oper()
                {
                    ColumnName = null,
                    ColumnNumber = 0,
                    ColumnType = LinqDbTypes.int_,
                    IsDb = false,
                    IsOperator = true,
                    IsResult = false,
                    NonDbValue = null,
                    Result = null,
                    TableName = table_info.Name,
                    TableNumber = table_info.TableNumber,
                    Type = System.Linq.Expressions.ExpressionType.GreaterThan
                };
                var op2 = new Oper()
                {
                    ColumnName = "Id",
                    ColumnNumber = table_info.ColumnNumbers["Id"],
                    ColumnType = LinqDbTypes.int_,
                    IsDb = true,
                    IsOperator = false,
                    IsResult = false,
                    NonDbValue = null,
                    Result = null,
                    TableName = table_info.Name,
                    TableNumber = table_info.TableNumber,
                    Type = System.Linq.Expressions.ExpressionType.MemberAccess
                };
                var op3 = new Oper()
                {
                    ColumnName = null,
                    ColumnNumber = 0,
                    ColumnType = LinqDbTypes.int_,
                    IsDb = false,
                    IsOperator = false,
                    IsResult = false,
                    NonDbValue = 0,
                    Result = null
                };
                tree.WhereInfo = new List<WhereInfo>();
                tree.WhereInfo.Add(new WhereInfo()
                {
                    Id = 1,
                    Opers = new Stack<Oper>()
                });
                tree.WhereInfo[0].Opers.Push(op1);
                tree.WhereInfo[0].Opers.Push(op2);
                tree.WhereInfo[0].Opers.Push(op3);
            }

            CheckTableInfo<T>();
            using (var snapshot = leveld_db.CreateSnapshot())
            {
                var ro = new ReadOptions().SetSnapshot(snapshot);
                var table_info = GetTableInfo(typeof(T).Name);
                var where_res = CalculateWhereResult<T>(tree, table_info, ro);
                where_res = FindBetween(tree, where_res, ro);
                where_res = Intersect(tree, where_res, table_info, tree.QueryCache, ro);
                double? searchPercentile = null;
                where_res = Search(tree, where_res, ro, out searchPercentile);
                var fres = CombineData(where_res);

                var total = GetTableRowCount(table_info, ro);
                var all = total == fres.ResIds.Count();
                return new Tuple<List<int>, bool>(fres.ResIds.ToList(), all);
            }
            
        }
    }
}
