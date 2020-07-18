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
        public IDbQueryable<T> Intersect<T, TKey>(IDbQueryable<T> source, Expression<Func<T, TKey>> keySelector, HashSet<int?> set)
        {
            CheckTableInfo<T>();
            var new_set = new List<EncodedValue>();
            foreach (var s in set)
            {
                new_set.Add(EncodeValue(LinqDbTypes.int_, s));
            }
            return Intersect(source, keySelector, new_set, source.LDBTree.QueryCache);
        }
        public IDbQueryable<T> Intersect<T, TKey>(IDbQueryable<T> source, Expression<Func<T, TKey>> keySelector, HashSet<int> set)
        {
            CheckTableInfo<T>();
            var new_set = new List<EncodedValue>();
            foreach (var s in set)
            {
                new_set.Add(EncodeValue(LinqDbTypes.int_, s));
            }
            return Intersect(source, keySelector, new_set, source.LDBTree.QueryCache);
        }
        public IDbQueryable<T> Intersect<T, TKey>(IDbQueryable<T> source, Expression<Func<T, TKey>> keySelector, HashSet<double?> set)
        {
            CheckTableInfo<T>();
            var new_set = new List<EncodedValue>();
            foreach (var s in set)
            {
                new_set.Add(EncodeValue(LinqDbTypes.double_, s));
            }
            return Intersect(source, keySelector, new_set, source.LDBTree.QueryCache);
        }
        public IDbQueryable<T> Intersect<T, TKey>(IDbQueryable<T> source, Expression<Func<T, TKey>> keySelector, HashSet<double> set)
        {
            CheckTableInfo<T>();
            var new_set = new List<EncodedValue>();
            foreach (var s in set)
            {
                new_set.Add(EncodeValue(LinqDbTypes.double_, s));
            }
            return Intersect(source, keySelector, new_set, source.LDBTree.QueryCache);
        }
        public IDbQueryable<T> Intersect<T, TKey>(IDbQueryable<T> source, Expression<Func<T, TKey>> keySelector, HashSet<DateTime?> set)
        {
            CheckTableInfo<T>();
            var new_set = new List<EncodedValue>();
            foreach (var s in set)
            {
                new_set.Add(EncodeValue(LinqDbTypes.DateTime_, s));
            }
            return Intersect(source, keySelector, new_set, source.LDBTree.QueryCache);
        }
        public IDbQueryable<T> Intersect<T, TKey>(IDbQueryable<T> source, Expression<Func<T, TKey>> keySelector, HashSet<DateTime> set)
        {
            CheckTableInfo<T>();
            var new_set = new List<EncodedValue>();
            foreach (var s in set)
            {
                new_set.Add(EncodeValue(LinqDbTypes.DateTime_, s));
            }
            return Intersect(source, keySelector, new_set, source.LDBTree.QueryCache);
        }
        public IDbQueryable<T> Intersect<T, TKey>(IDbQueryable<T> source, Expression<Func<T, TKey>> keySelector, HashSet<string> set)
        {
            CheckTableInfo<T>();
            var new_set = new List<EncodedValue>();
            foreach (var s in set)
            {
                new_set.Add(EncodeValue(LinqDbTypes.string_, s));
            }
            return Intersect(source, keySelector, new_set, source.LDBTree.QueryCache);
        }

        IDbQueryable<T> Intersect<T, TKey>(IDbQueryable<T> source, Expression<Func<T, TKey>> keySelector, List<EncodedValue> set, Dictionary<long, byte[]> cache)
        {
            if (source.LDBTree == null)
            {
                source.LDBTree = new QueryTree();
            }
            var tree = source.LDBTree;
            if (tree.IntersectInfo == null)
            {
                tree.IntersectInfo = new List<IntersectInfo>();
            }
            var info = new IntersectInfo();
            tree.IntersectInfo.Add(info);
            info.Set = set;

            string name = "Id";
            if (keySelector != null)
            {
                var par = keySelector.Parameters.First();
                name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            }
            var table_info = GetTableInfo(typeof(T).Name);
            info.TableNumber = table_info.TableNumber;
            info.ColumnNumber = table_info.ColumnNumbers[name];
            info.ColumnType = table_info.Columns[name];

            source.LDBTree.Prev = info;
            source.LDBTree.Prev.Id = source.LDBTree.Counter + 1;
            source.LDBTree.Counter++;

            return source;
        }

        public List<OperResult> Intersect(QueryTree tree, List<OperResult> oper_list, TableInfo table_info, Dictionary<long, byte[]> cache, ReadOptions ro)
        {
            if (tree.IntersectInfo == null)
            {
                return oper_list;
            }

            foreach (var inter in tree.IntersectInfo)
            {
                var res = IntersectOne(inter, table_info, cache, ro);
                var oper_res = new OperResult()
                {
                    All = false,
                    ResIds = res
                };
                oper_res.Id = inter.Id;
                oper_res.OrWith = inter.OrWith;
                oper_list.Add(oper_res);
            }

            return oper_list;
        }

        public List<int> IntersectOne(IntersectInfo info, TableInfo table_info, Dictionary<long, byte[]> cache, ReadOptions ro)
        {
            var result = new List<int>();


            //if (info.Set.Count() > 50)
            //{
            Oper odb = new Oper()
            {
                TableNumber = info.TableNumber,
                ColumnType = info.ColumnType,
                ColumnNumber = info.ColumnNumber,
                ColumnName = table_info.ColumnNumbers.First(f => f.Value == info.ColumnNumber).Key,
                TableName = table_info.Name
            };

            var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[odb.ColumnName]);
            var snapid = leveld_db.Get(skey, null, ro);
            if (snapid != null)
            {
                var snapshot_id = Encoding.UTF8.GetString(snapid);
                skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers["Id"]);
                snapid = leveld_db.Get(skey, null, ro);
                string id_snapshot_id = Encoding.UTF8.GetString(snapid);

                var index_res = IntersectWithIndex(odb, info.Set, ro, snapshot_id, id_snapshot_id);
                if (index_res != null)
                {
                    return index_res;
                }
            }

            foreach (var val in info.Set)
            {
                Oper odb2 = new Oper()
                {
                    TableNumber = info.TableNumber,
                    ColumnType = info.ColumnType,
                    ColumnNumber = info.ColumnNumber,
                    ColumnName = table_info.ColumnNumbers.First(f => f.Value == info.ColumnNumber).Key,
                    TableName = table_info.Name
                };
                result.AddRange(EqualOperator(odb2, val, table_info, cache, ro, null, null));
            }

            return result;
            //}

            //foreach (var val in info.Set)
            //{
            //    Oper odb = new Oper()
            //    {
            //        TableNumber = info.TableNumber,
            //        ColumnType = info.ColumnType,
            //        ColumnNumber = info.ColumnNumber,
            //        ColumnName = table_info.ColumnNumbers.First(f => f.Value == info.ColumnNumber).Key,
            //        TableName = table_info.Name
            //    };
            //    result.AddRange(EqualOperator(odb, val, table_info, cache, ro, null, null));            
            //}

            //return result;
        }

        List<int> IntersectWithIndex(Oper odb, List<EncodedValue> val, ReadOptions ro, string snapshot_id, string id_snapshot_id)
        {
            var result = new List<int>();

            if (string.IsNullOrEmpty(snapshot_id))
            {
                return null;
            }
            if (string.IsNullOrEmpty(odb.TableName) || string.IsNullOrEmpty(odb.ColumnName))
            {
                throw new LinqDbException("Linqdb: bad indexes.");
            }
            if (!indexes.ContainsKey(odb.TableName + "|" + odb.ColumnName + "|" + snapshot_id))
            {
                return null;
            }
            var index = indexes[odb.TableName + "|" + odb.ColumnName + "|" + snapshot_id];
            var ids_index = indexes[odb.TableName + "|Id|" + id_snapshot_id];
            if (index.IndexType == IndexType.GroupOnly)
            {
                return null;
            }
            int bloom_max = 1000000;
            switch (odb.ColumnType)
            {
                case LinqDbTypes.int_:
                    HashSet<int> ivals = new HashSet<int>(val.Where(f => !f.IsNull).Select(f => f.IntVal));
                    List<bool> bloom_int = new List<bool>(bloom_max);

                    for (int i = 0; i < bloom_max; i++)
                    {
                        bloom_int.Add(false);
                    }
                    foreach (var int_val in ivals)
                    {
                        bloom_int[int_val % bloom_max] = true;
                    }

                    int icount = index.Parts.Count();
                    for (int i = 0; i < icount; i++)
                    {
                        var ids = ids_index.Parts[i].IntValues;
                        var iv = index.Parts[i].IntValues;
                        int jcount = iv.Count();
                        for (int j = 0; j < jcount; j++)
                        {
                            int id = ids[j];
                            if (bloom_int[iv[j] % bloom_max])
                            {
                                if (ivals.Contains(iv[j]))
                                {
                                    result.Add(id);
                                }
                            }
                        }
                    }
                    break;
                case LinqDbTypes.double_:
                case LinqDbTypes.DateTime_:
                    HashSet<double> dvals = new HashSet<double>(val.Where(f => !f.IsNull).Select(f => f.DoubleVal));
                    List<bool> bloom_double = new List<bool>(bloom_max);
                    for (int i = 0; i < bloom_max; i++)
                    {
                        bloom_double.Add(false);
                    }
                    foreach (var d_val in dvals)
                    {
                        bloom_double[Math.Abs(d_val.GetHashCode()) % bloom_max] = true;
                    }
                    int dcount = index.Parts.Count();
                    for (int i = 0; i < dcount; i++)
                    {
                        var ids = ids_index.Parts[i].IntValues;
                        var iv = index.Parts[i].DoubleValues;
                        int jcount = iv.Count();
                        for (int j = 0; j < jcount; j++)
                        {
                            int id = ids[j];
                            if (bloom_double[Math.Abs(iv[j].GetHashCode()) % bloom_max])
                            {
                                if (dvals.Contains(iv[j]))
                                {
                                    result.Add(id);
                                }
                            }
                        }
                    }
                    break;
                default:
                    return null;
            }

            return result;
        }

    }

    public class IntersectInfo : BaseInfo
    {
        public List<EncodedValue> Set { get; set; }

        public short TableNumber { get; set; }
        public short ColumnNumber { get; set; }
        public LinqDbTypes ColumnType { get; set; }
    }
}
