using RocksDbSharp;
using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public IDbQueryable<T> Between<T, TKey>(IDbQueryable<T> source, Expression<Func<T, TKey>> keySelector, double from, double to, BetweenBoundariesInternal boundaries = BetweenBoundariesInternal.BothInclusive)
        {
            CheckTableInfo<T>();
            if (source.LDBTree == null)
            {
                source.LDBTree = new QueryTree();
            }
            var tree = source.LDBTree;
            if (tree.BetweenInfo == null)
            {
                tree.BetweenInfo = new List<BetweenInfo>();
            }
            var info = new BetweenInfo();
            tree.BetweenInfo.Add(info);
            info.BetweenBoundaries = boundaries;
            info.From = from;
            info.To = to;

            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            var table_info = GetTableInfo(typeof(T).Name);
            info.TableNumber = table_info.TableNumber;
            info.ColumnNumber = table_info.ColumnNumbers[name];
            info.ColumnType = table_info.Columns[name];
            info.ColumnName = name;
            info.TableName = table_info.Name;
            info.IdNumber = table_info.ColumnNumbers["Id"];


            source.LDBTree.Prev = info;
            source.LDBTree.Prev.Id = source.LDBTree.Counter + 1;
            source.LDBTree.Counter++;

            return source;
        }

        public List<OperResult> FindBetween(QueryTree tree, List<OperResult> oper_list, ReadOptions ro)
        {
            if (tree.BetweenInfo == null)
            {
                return oper_list;
            }
            
            foreach (var bet in tree.BetweenInfo)
            {
                var res = FindOneBetween(bet, tree.QueryCache, ro);
                var oper_res = new OperResult()
                {
                    All = false,
                    ResIds = res
                };
                oper_res.Id = bet.Id;
                oper_res.OrWith = bet.OrWith;
                oper_list.Add(oper_res);
            }

            return oper_list;
        }
        public List<int> FindOneBetween(BetweenInfo bet, Dictionary<long, byte[]> cache, ReadOptions ro)
        {
            var result = new List<int>();

            var skey = MakeSnapshotKey(bet.TableNumber, bet.ColumnNumber);
            var snapid = leveld_db.Get(skey, null, ro);
            string snapshot_id = null;
            string id_snapshot_id = null;
            if (snapid != null)
            {
                snapshot_id = Encoding.UTF8.GetString(snapid);
                skey = MakeSnapshotKey(bet.TableNumber, bet.IdNumber);
                snapid = leveld_db.Get(skey, null, ro);
                id_snapshot_id = Encoding.UTF8.GetString(snapid);
                if (indexes.ContainsKey(bet.TableName + "|" + bet.ColumnName + "|" + snapshot_id))
                {
                    var op = new Oper()
                    {
                        ColumnNumber = bet.ColumnNumber,
                        ColumnType = bet.ColumnType,
                        TableNumber = bet.TableNumber,
                        ColumnName = bet.ColumnName,
                        TableName = bet.TableName
                    };

                    var val = EncodeValue(op.ColumnType, bet.From);
                    var index_res = GreaterThanOperatorWithIndex(op, val, bet.BetweenBoundaries == BetweenBoundariesInternal.BothInclusive || bet.BetweenBoundaries == BetweenBoundariesInternal.FromInclusiveToExclusive, ro, snapshot_id, id_snapshot_id, bet.To, bet.BetweenBoundaries == BetweenBoundariesInternal.BothInclusive || bet.BetweenBoundaries == BetweenBoundariesInternal.FromExclusiveToInclusive);
                    return index_res;
                }
            }

            if (bet.From >= 0 && bet.To >= 0)
            { 
                var op  = new Oper()
                {
                    ColumnNumber = bet.ColumnNumber,
                    ColumnType = bet.ColumnType,
                    TableNumber = bet.TableNumber,
                    ColumnName = bet.ColumnName,
                    TableName = bet.TableName
                };

                var val = EncodeValue(op.ColumnType, bet.From);
                var result_tmp = GreaterThanOperator(op, val, bet.BetweenBoundaries == BetweenBoundariesInternal.BothInclusive || bet.BetweenBoundaries == BetweenBoundariesInternal.FromInclusiveToExclusive, cache, ro, snapshot_id, id_snapshot_id,
                                                     bet.To, bet.BetweenBoundaries == BetweenBoundariesInternal.BothInclusive || bet.BetweenBoundaries == BetweenBoundariesInternal.FromExclusiveToInclusive);
                result = result_tmp;
            }
            else if (bet.From < 0 && bet.To < 0)
            {
                var op  = new Oper()
                {
                    ColumnNumber = (short)(-1 * bet.ColumnNumber),
                    ColumnType = bet.ColumnType,
                    TableNumber = bet.TableNumber,
                    TableName = bet.TableName,
                    ColumnName = bet.ColumnName
                };

                var val = EncodeValue(op.ColumnType, bet.To);
                if(val.Type == LinqDbTypes.double_)
                {
                    val.DoubleVal *= -1;
                }
                else if (val.Type == LinqDbTypes.int_)
                {
                    val.IntVal *= -1;
                }
                var result_tmp = GreaterThanOperator(op, val, bet.BetweenBoundaries == BetweenBoundariesInternal.BothInclusive || bet.BetweenBoundaries == BetweenBoundariesInternal.FromExclusiveToInclusive, cache, ro, snapshot_id, id_snapshot_id,
                                                     - 1*bet.From, bet.BetweenBoundaries == BetweenBoundariesInternal.BothInclusive || bet.BetweenBoundaries == BetweenBoundariesInternal.FromInclusiveToExclusive);
                result = result_tmp;
            }
            else
            {
                var op  = new Oper()
                {
                    ColumnNumber = (short)(-1 * bet.ColumnNumber),
                    ColumnType = bet.ColumnType,
                    TableNumber = bet.TableNumber,
                    TableName = bet.TableName,
                    ColumnName = bet.ColumnName
                };

                var val = EncodeValue(op.ColumnType, 0);
                var result_tmp = GreaterThanOperator(op, val, true, cache, ro, snapshot_id, id_snapshot_id,
                                                     - 1*bet.From, bet.BetweenBoundaries == BetweenBoundariesInternal.BothInclusive || bet.BetweenBoundaries == BetweenBoundariesInternal.FromInclusiveToExclusive);
                result = result_tmp;

                op  = new Oper()
                {
                    ColumnNumber = bet.ColumnNumber,
                    ColumnType = bet.ColumnType,
                    TableNumber = bet.TableNumber,
                    TableName = bet.TableName,
                    ColumnName = bet.ColumnName
                };


                val = EncodeValue(op.ColumnType, 0);
                var result_pos = GreaterThanOperator(op, val, false, cache, ro, snapshot_id, id_snapshot_id,
                                                     bet.To, bet.BetweenBoundaries == BetweenBoundariesInternal.BothInclusive || bet.BetweenBoundaries == BetweenBoundariesInternal.FromExclusiveToInclusive);

                result = result.MyUnion(result_pos);
            }

            return result;
        }
    }
    public enum BetweenBoundariesInternal : int
    {
        BothInclusive,
        FromInclusiveToExclusive,
        FromExclusiveToInclusive,
        BothExclusive
    }

    public class BetweenInfo : BaseInfo
    {
        public double From { get; set; }
        public double To { get; set; }
        public BetweenBoundariesInternal BetweenBoundaries { get; set; }

        public short TableNumber { get; set; }
        public short ColumnNumber { get; set; }
        public LinqDbTypes ColumnType { get; set; }
        public string ColumnName { get; set; }
        public string TableName { get; set; }
        public short IdNumber { get; set; }
    }
}
