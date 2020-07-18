using RocksDbSharp;
using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public IDbOrderedQueryable<T> OrderBy<T, TKey>(IDbQueryable<T> source, Expression<Func<T, TKey>> keySelector)
        {
            CheckTableInfo<T>();
            var res = new IDbOrderedQueryable<T>() { _db = this };
            res.LDBTree = source.LDBTree;
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            var table_info = GetTableInfo(typeof(T).Name);
            res.LDBTree.OrderingInfo = new OrderingInfo();
            res.LDBTree.OrderingInfo.Orderings = new List<OrderByInfo>();
            res.LDBTree.OrderingInfo.Orderings.Add(new OrderByInfo()
            {
                TableNumber = table_info.TableNumber,
                ColumnNumber = table_info.ColumnNumbers[name],
                ColumnType = table_info.Columns[name],
                IsDescending = false,
                ColumnName = name
            });
            return res;
        }
        public IDbOrderedQueryable<T> OrderByDescending<T, TKey>(IDbQueryable<T> source, Expression<Func<T, TKey>> keySelector)
        {
            CheckTableInfo<T>();
            var res = new IDbOrderedQueryable<T>() { _db = this };
            res.LDBTree = source.LDBTree;
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            var table_info = GetTableInfo(typeof(T).Name);
            res.LDBTree.OrderingInfo = new OrderingInfo();
            res.LDBTree.OrderingInfo.Orderings = new List<OrderByInfo>();
            res.LDBTree.OrderingInfo.Orderings.Add(new OrderByInfo()
            {
                TableNumber = table_info.TableNumber,
                ColumnNumber = table_info.ColumnNumbers[name],
                ColumnType = table_info.Columns[name],
                IsDescending = true,
                ColumnName = name
            });
            return res;
        }
        public IDbOrderedQueryable<T> Skip<T>(IDbOrderedQueryable<T> source, int count)
        {
            source.LDBTree.OrderingInfo.Skip = count;
            return source;
        }
        public IDbOrderedQueryable<T> Take<T>(IDbOrderedQueryable<T> source, int count)
        {
            source.LDBTree.OrderingInfo.Take = count;
            return source;
        }

        public OperResult OrderData(OperResult oper, QueryTree tree, int row_count, ReadOptions ro, TableInfo table_info)
        {
            if (tree.OrderingInfo == null)
            {
                return oper;
            }

            var ord = tree.OrderingInfo.Orderings.First();
            Dictionary<int, int> data = new Dictionary<int, int>();
            if (!ord.IsDescending)
            {
                if (oper.All)
                {
                    data = OrderValuesWithNegatives(ord, oper, tree.OrderingInfo.Take, tree.OrderingInfo.Skip, true, ro);
                }
                else if (ord.ColumnName == "Id")
                {
                    data = OrderSomeValues(ord, oper, tree.OrderingInfo.Take, tree.OrderingInfo.Skip, false, tree.QueryCache, ro, table_info);
                }
                else if ((oper.ResIds.Count() / (double)row_count) > 0.40 && oper.ResIds.Count() > 500000 || (tree.OrderingInfo.Take != null && oper.ResIds.Count() / (double)row_count > 0.05))
                {
                    data = OrderValuesWithNegatives(ord, oper, tree.OrderingInfo.Take, tree.OrderingInfo.Skip, false, ro);
                }
                else
                {
                    data = OrderSomeValues(ord, oper, tree.OrderingInfo.Take, tree.OrderingInfo.Skip, false, tree.QueryCache, ro, table_info);
                }
            }
            else
            {
                if (oper.All)
                {
                    data = OrderValuesByDescendingWithNegatives(ord, oper, tree.OrderingInfo.Take, tree.OrderingInfo.Skip, true, ro);
                }
                else if (ord.ColumnName == "Id")
                {
                    data = OrderSomeValues(ord, oper, tree.OrderingInfo.Take, tree.OrderingInfo.Skip, true, tree.QueryCache, ro, table_info);
                }
                else if ((oper.ResIds.Count() / (double)row_count) > 0.40 && oper.ResIds.Count() > 500000 || (tree.OrderingInfo.Take != null && oper.ResIds.Count() / (double)row_count > 0.05))
                {
                    data = OrderValuesByDescendingWithNegatives(ord, oper, tree.OrderingInfo.Take, tree.OrderingInfo.Skip, false, ro);
                }
                else
                {
                    data = OrderSomeValues(ord, oper, tree.OrderingInfo.Take, tree.OrderingInfo.Skip, true, tree.QueryCache, ro, table_info);
                }
            }

            oper.All = false;
            oper.ResIds = data.Keys.ToList<int>();

            oper.OrderedIds = data;
            oper.IsOrdered = true;
            return oper;
        }

        //todo: maybe later
        //public Tuple<OperResult, bool> OrderDataWithIndex(OperResult oper, QueryTree tree, int row_count, ReadOptions ro)
        //{
        //    bool done = false;

        //    var ord = tree.OrderingInfo.Orderings.First();

        //    var skey = MakeSnapshotKey(ord.TableNumber, ord.ColumnNumber);
        //    var snapid = leveld_db.Get(skey, ro);
        //    if (snapid == null)
        //    {
        //        return new Tuple<OperResult, bool>(oper, done);
        //    }

        //    var snapshot_id = Encoding.UTF8.GetString(snapid);
        //    var index = indexes[ord.TableNumber + "|" + ord.ColumnNumber + "|" + snapshot_id];

        //    switch (ord.ColumnType)
        //    {
        //        case LinqDbTypes.int_:
        //            done = true;
        //            break;
        //        case LinqDbTypes.double_:
        //            done = true;
        //            break;
        //        case LinqDbTypes.DateTime_:
        //            done = true;
        //            break;
        //        default:
        //            return null;
        //    }

        //    return new Tuple<OperResult, bool>(oper, done);
        //}

        Dictionary<int, int> OrderValuesWithNegatives(OrderByInfo order_info, OperResult res_set, int? take, int? skip, bool all, ReadOptions ro)
        {
            order_info.ColumnNumber *= -1;
            var neg_res = OrderValuesByDescending(order_info, res_set, take, skip, all, ro);
            order_info.ColumnNumber *= -1;
            int? take_left = neg_res.Take == null ? null : (neg_res.Take - neg_res.Data.Count);
            if (take_left == null || take_left > 0)
            {
                var pos_res = OrderValues(order_info, res_set, take_left, neg_res.Skip, all, ro);
                var neg_count = neg_res.Data.Count();
                foreach (var pv in pos_res.Data)
                {
                    neg_res.Data[pv.Key] = pv.Value + neg_count;
                }
            }
            return neg_res.Data;
        }
        Dictionary<int, int> OrderValuesByDescendingWithNegatives(OrderByInfo order_info, OperResult res_set, int? take, int? skip, bool all, ReadOptions ro)
        {
            var pos_res = OrderValuesByDescending(order_info, res_set, take, skip, all, ro);

            int? take_left = pos_res.Take == null ? null : (pos_res.Take - pos_res.Data.Count);
            if (take_left == null || take_left > 0)
            {
                order_info.ColumnNumber *= -1;
                var neg_res = OrderValues(order_info, res_set, take_left, pos_res.Skip, all, ro);
                order_info.ColumnNumber *= -1;
                var pos_count = pos_res.Data.Count();
                foreach (var pv in neg_res.Data)
                {
                    pos_res.Data[pv.Key] = pv.Value + pos_count;
                }
            }
            return pos_res.Data;
        }
        OrderedTmpResult OrderValues(OrderByInfo order_info, OperResult res_set, int? take, int? skip, bool all, ReadOptions ro)
        {
            OrderedTmpResult res = new OrderedTmpResult();
            res.Data = new Dictionary<int, int>();
            res.Take = take;
            res.Skip = skip;
            var key = MakeIndexSearchKey(new IndexKeyInfo()
            {
                TableNumber = order_info.TableNumber,
                ColumnNumber = order_info.ColumnNumber,
                ColumnType = order_info.ColumnType,
                Val = PreNull
            });
            int count = 0;
            using (var it = leveld_db.NewIterator(null, ro))
            {
                it.Seek(key);
                if (!it.Valid())
                {
                    return res;
                }

                var k = it.Key();
                if (k == null)
                {
                    return res;
                }
                var info = GetIndexKey(k);
                var contains = new ContainsInfo()
                {
                    ids = res_set.ResIds
                };
                bool has = false;
                if (!info.NotKey && info.TableNumber == order_info.TableNumber && info.ColumnNumber == order_info.ColumnNumber)
                {
                    if (!all)
                    {
                        has = EfficientContains(contains, info.Id);
                        if (contains.is_sorted)
                        {
                            res_set.ResIds = contains.ids;
                        }
                    }
                    if (all || has)
                    {
                        if (res.Skip == null || res.Skip == 0)
                        {
                            res.Data[info.Id] = count;
                            count++;
                            if (res.Take != null && count == res.Take)
                            {
                                return res;
                            }
                        }
                        else
                        {
                            res.Skip--;
                        }
                    }
                }
                else
                {
                    return res;
                }
                while (true)
                {
                    it.Next();
                    if (!it.Valid())
                    {
                        return res;
                    }

                    k = it.Key();
                    if (k == null)
                    {
                        return res;
                    }
                    info = GetIndexKey(k);
                    if (!info.NotKey && info.TableNumber == order_info.TableNumber && info.ColumnNumber == order_info.ColumnNumber)
                    {
                        if (!all)
                        {
                            has = EfficientContains(contains, info.Id);
                        }
                        if (all || has)
                        {
                            if (res.Skip == null || res.Skip == 0)
                            {
                                res.Data[info.Id] = count;
                                count++;
                                if (res.Take != null && count == res.Take)
                                {
                                    return res;
                                }
                            }
                            else
                            {
                                res.Skip--;
                            }
                        }
                    }
                    else
                    {
                        return res;
                    }
                }
            }
        }

        OrderedTmpResult OrderValuesByDescending(OrderByInfo order_info, OperResult res_set, int? take, int? skip, bool all, ReadOptions ro)
        {
            OrderedTmpResult res = new OrderedTmpResult();
            res.Data = new Dictionary<int, int>();
            res.Take = take;
            res.Skip = skip;
            var key = MakeIndexSearchKey(new IndexKeyInfo()
            {
                TableNumber = order_info.TableNumber,
                ColumnNumber = order_info.ColumnNumber,
                ColumnType = order_info.ColumnType,
                Val = PostEverything
            });
            int count = 0;
            using (var it = leveld_db.NewIterator(null, ro))
            {
                it.Seek(key);
                if (!it.Valid())
                {
                    return res;
                }
                var k = it.Key();
                var info = GetIndexKey(k);
                if (info.NotKey || info.TableNumber != order_info.TableNumber || info.ColumnNumber != order_info.ColumnNumber)
                {
                    it.Prev();
                    if (!it.Valid())
                    {
                        return res;
                    }
                    k = it.Key();
                    if (k == null)
                    {
                        return res;
                    }
                    info = GetIndexKey(k);
                }
                var contains = new ContainsInfo()
                {
                    ids = res_set.ResIds
                };
                bool has = false;
                if (!info.NotKey && info.TableNumber == order_info.TableNumber && info.ColumnNumber == order_info.ColumnNumber)
                {
                    if (!all)
                    {
                        has = EfficientContains(contains, info.Id);
                        if (contains.is_sorted)
                        {
                            res_set.ResIds = contains.ids;
                        }
                    }
                    if (all || has)
                    {
                        if (res.Skip == null || res.Skip == 0)
                        {
                            res.Data[info.Id] = count;
                            count++;
                            if (res.Take != null && count == res.Take)
                            {
                                return res;
                            }
                        }
                        else
                        {
                            res.Skip--;
                        }
                    }
                }
                else
                {
                    return res;
                }
                while (true)
                {
                    it.Prev();
                    if (!it.Valid())
                    {
                        return res;
                    }

                    k = it.Key();
                    if (k == null)
                    {
                        return res;
                    }
                    info = GetIndexKey(k);
                    if (!info.NotKey && info.TableNumber == order_info.TableNumber && info.ColumnNumber == order_info.ColumnNumber)
                    {
                        if (!all)
                        {
                            has = EfficientContains(contains, info.Id);
                        }
                        if (all || has)
                        {
                            if (res.Skip == null || res.Skip == 0)
                            {
                                res.Data[info.Id] = count;
                                count++;
                                if (res.Take != null && count == res.Take)
                                {
                                    return res;
                                }
                            }
                            else
                            {
                                res.Skip--;
                            }
                        }
                    }
                    else
                    {
                        return res;
                    }
                }
            }
        }

        Dictionary<int, int> OrderSomeValuesWithIndex(OrderByInfo order_info, OperResult res_set, int? take, int? skip, bool desc, Dictionary<long, byte[]> cache, ReadOptions ro, TableInfo table_info)
        {
            var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[order_info.ColumnName]);
            var snapid = leveld_db.Get(skey, null, ro);
            if (snapid == null)
            {
                return null;
            }
            var snapshot_id = Encoding.UTF8.GetString(snapid);
            if (string.IsNullOrEmpty(table_info.Name) || string.IsNullOrEmpty(order_info.ColumnName))
            {
                throw new LinqDbException("Linqdb: bad indexes.");
            }
            if (!indexes.ContainsKey(table_info.Name + "|" + order_info.ColumnName + "|" + snapshot_id))
            {
                return null;
            }

            var index = indexes[table_info.Name + "|" + order_info.ColumnName + "|" + snapshot_id];
            skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers["Id"]);
            snapid = leveld_db.Get(skey, null, ro);
            var id_snapshot_id = Encoding.UTF8.GetString(snapid);
            var ids_index = indexes[table_info.Name + "|Id|" + id_snapshot_id];
            if (index.IndexType == IndexType.GroupOnly)
            {
                return null;
            }


            Dictionary<int, int> res = new Dictionary<int, int>();
            var matched = MatchesFromIndex(res_set.ResIds, index, ids_index, table_info.Columns[order_info.ColumnName]);
            if (matched.Item3 != null)
            {
                res_set.ResIds = matched.Item3;
            }

            if (table_info.Columns[order_info.ColumnName] == LinqDbTypes.int_)
            {
                var int_vals = matched.Item1.OrderBy(f => f.Value).ToList();

                if (!desc)
                {
                    if (skip == null)
                    {
                        skip = 0;
                    }
                    int icount = 0;
                    for (int i = 0; i < int_vals.Count(); i++)
                    {
                        if (skip > 0)
                        {
                            skip--;
                            continue;
                        }
                        if (take == null || take > 0)
                        {
                            if (take > 0)
                            {
                                take--;
                            }
                            res[int_vals[i].Key] = icount;
                            icount++;
                        }
                        else if (take == 0)
                        {
                            break;
                        }
                    }
                }
                else
                {
                    if (skip == null)
                    {
                        skip = 0;
                    }
                    int icount = 0;
                    for (int i = int_vals.Count() - 1; i >= 0; i--)
                    {
                        if (skip > 0)
                        {
                            skip--;
                            continue;
                        }
                        if (take == null || take > 0)
                        {
                            if (take > 0)
                            {
                                take--;
                            }
                            res[int_vals[i].Key] = icount;
                            icount++;
                        }
                        else if (take == 0)
                        {
                            break;
                        }
                    }
                }
            }
            else
            {
                var double_vals = matched.Item2.OrderBy(f => f.Value).ToList();

                if (!desc)
                {
                    if (skip == null)
                    {
                        skip = 0;
                    }
                    int icount = 0;
                    for (int i = 0; i < double_vals.Count(); i++)
                    {
                        if (skip > 0)
                        {
                            skip--;
                            continue;
                        }
                        if (take == null || take > 0)
                        {
                            if (take > 0)
                            {
                                take--;
                            }
                            res[double_vals[i].Key] = icount;
                            icount++;
                        }
                        else if (take == 0)
                        {
                            break;
                        }
                    }
                }
                else
                {
                    if (skip == null)
                    {
                        skip = 0;
                    }
                    int icount = 0;
                    for (int i = double_vals.Count() - 1; i >= 0; i--)
                    {
                        if (skip > 0)
                        {
                            skip--;
                            continue;
                        }
                        if (take == null || take > 0)
                        {
                            if (take > 0)
                            {
                                take--;
                            }
                            res[double_vals[i].Key] = icount;
                            icount++;
                        }
                        else if (take == 0)
                        {
                            break;
                        }
                    }
                }
            }

            return res;
        }


        bool IsSorted(List<int> list)
        {
            if (!list.Any())
            {
                return true;
            }
            int curr = list[0];
            foreach (var l in list)
            {
                if (curr > l)
                {
                    return false;
                }
                curr = l;
            }
            return true;
        }

        Tuple<List<int>, List<double>> EfficientContainsOneCall(IndexGeneric ids, IndexGeneric data, List<int> res_ids, bool group_values, bool is_double)
        {
            List<int> res_int = null;
            List<double> res_double = null;
            if (group_values || !is_double)
            {
                res_int = new List<int>(res_ids.Count());
            }
            else
            {
                res_double = new List<double>(res_ids.Count());
            }
            var res = new Tuple<List<int>, List<double>>(res_int, res_double);

            int max = Int32.MinValue;
            int min = Int32.MaxValue;
            for (int i = 0; i < res_ids.Count(); i++)
            {
                if (max < res_ids[i])
                {
                    max = res_ids[i];
                }
                if (min > res_ids[i])
                {
                    min = res_ids[i];
                }
            }
            int max_size = 300000000;
            var size = max - min + 1;
            if (size > max_size)
            {
                throw new LinqDbException("Linqdb: id too large...");
            }
            var check_list = new List<bool>(size);
            for (int i = 0; i < size; i++)
            {
                check_list.Add(false);
            }
            foreach (var id in res_ids)
            {
                check_list[id - min] = true;
            }

            if (group_values)
            {
                for (int i = 0; i < data.Parts.Count(); i++)
                {
                    int icount = data.Parts[i].GroupValues.Count();
                    var idss = ids.Parts[i].IntValues;
                    var gv = data.Parts[i].GroupValues;
                    for (int j = 0; j < icount; j++)
                    {
                        int id = idss[j] - min;
                        if (id >= 0 && id < size && check_list[id])
                        {
                            res_int.Add(gv[j]);
                        }
                    }
                }
            }
            else if (!is_double)
            {
                for (int i = 0; i < data.Parts.Count(); i++)
                {
                    int icount = data.Parts[i].IntValues.Count();
                    var idss = ids.Parts[i].IntValues;
                    var gv = data.Parts[i].IntValues;
                    for (int j = 0; j < icount; j++)
                    {
                        int id = idss[j] - min;
                        if (id >= 0 && id < size && check_list[id])
                        {
                            res_int.Add(gv[j]);
                        }
                    }
                }
            }
            else
            {
                for (int i = 0; i < data.Parts.Count(); i++)
                {
                    int icount = data.Parts[i].DoubleValues.Count();
                    var idss = ids.Parts[i].IntValues;
                    var gv = data.Parts[i].DoubleValues;
                    for (int j = 0; j < icount; j++)
                    {
                        int id = idss[j] - min;
                        if (id >= 0 && id < size && check_list[id])
                        {
                            res_double.Add(gv[j]);
                        }
                    }
                }
            }

            return res;
        }
        Tuple<List<Tuple<int, int>>, List<Tuple<int, double>>> EfficientContainsOneCall(IndexGeneric ids, IndexGeneric data, List<int> res_ids, bool is_double)
        {
            List<Tuple<int, int>> res_int = null;
            List<Tuple<int, double>> res_double = null;
            if (!is_double)
            {
                res_int = new List<Tuple<int, int>>();
            }
            else
            {
                res_double = new List<Tuple<int, double>>();
            }
            var res = new Tuple<List<Tuple<int, int>>, List<Tuple<int, double>>>(res_int, res_double);

            int max = Int32.MinValue;
            int min = Int32.MaxValue;
            for (int i = 0; i < res_ids.Count(); i++)
            {
                if (max < res_ids[i])
                {
                    max = res_ids[i];
                }
                if (min > res_ids[i])
                {
                    min = res_ids[i];
                }
            }
            int max_size = 300000000;
            var size = max - min + 1;
            if (size > max_size)
            {
                throw new LinqDbException("Linqdb: id too large...");
            }
            var check_list = new List<bool>(size);
            for (int i = 0; i < size; i++)
            {
                check_list.Add(false);
            }
            foreach (var id in res_ids)
            {
                check_list[id - min] = true;
            }

            if (!is_double)
            {
                for (int i = 0; i < data.Parts.Count(); i++)
                {
                    int icount = data.Parts[i].IntValues.Count();
                    var idss = ids.Parts[i].IntValues;
                    var gv = data.Parts[i].IntValues;
                    for (int j = 0; j < icount; j++)
                    {
                        int id = idss[j] - min;
                        if (id >= 0 && id < size && check_list[id])
                        {
                            res_int.Add(new Tuple<int, int>(idss[j], gv[j]));
                        }
                    }
                }
            }
            else
            {
                for (int i = 0; i < data.Parts.Count(); i++)
                {
                    int icount = data.Parts[i].DoubleValues.Count();
                    var idss = ids.Parts[i].IntValues;
                    var gv = data.Parts[i].DoubleValues;
                    for (int j = 0; j < icount; j++)
                    {
                        int id = idss[j] - min;
                        if (id >= 0 && id < size && check_list[id])
                        {
                            res_double.Add(new Tuple<int, double>(idss[j], gv[j]));
                        }
                    }
                }
            }

            return res;
        }

        bool EfficientContains(ContainsInfo contains, int check_id)
        {
            contains.is_sorted = false;
            int bloom_size = 3000000;

            if (!contains.small && contains.bloom_filter == null && contains.check_list == null)
            {
                int max = Int32.MinValue;
                bool is_sorted = true;
                int curr = Int32.MinValue;
                var ids = contains.ids;
                for (int i = 0; i < ids.Count(); i++)
                {
                    if (max < ids[i])
                    {
                        max = ids[i];
                    }
                    if (is_sorted && curr > ids[i])
                    {
                        is_sorted = false;
                    }
                    curr = ids[i];
                }

                contains.max = max;
                if (ids.Count() > 100000)
                {
                    contains.small = false;
                    int max_size = 300000000;
                    if (max < max_size)
                    {
                        List<bool> check_list = new List<bool>(max + 1);
                        contains.check_list = check_list;
                        for (int i = 0; i < max + 1; i++)
                        {
                            check_list.Add(false);
                        }
                        foreach (var id in ids)
                        {
                            check_list[id] = true;
                        }

                    }
                    else
                    {
                        List<bool> bloom_filter = new List<bool>(bloom_size);
                        contains.bloom_filter = bloom_filter;
                        for (int i = 0; i < bloom_size; i++)
                        {
                            bloom_filter.Add(false);
                        }
                        foreach (var id in ids)
                        {
                            bloom_filter[id % bloom_size] = true;
                        }
                        if (!is_sorted)
                        {
                            contains.ids = ids.OrderBy(f => f).ToList();
                            contains.is_sorted = true;
                        }
                    }
                }
                else
                {
                    contains.small = true;
                    if (!is_sorted)
                    {
                        contains.ids = ids.OrderBy(f => f).ToList();
                        contains.is_sorted = true;
                    }
                }
            }
            if (check_id > contains.max)
            {
                return false;
            }
            if (contains.small)
            {
                return contains.ids.BinarySearch(check_id) >= 0;
            }
            if (contains.check_list != null)
            {
                return contains.check_list[check_id];
            }
            else
            {
                if (contains.bloom_filter[check_id % bloom_size] && contains.ids.BinarySearch(check_id) >= 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        Tuple<List<KeyValuePair<int, int>>, List<KeyValuePair<int, double>>, List<int>> MatchesFromIndex(List<int> ids, IndexGeneric index, IndexGeneric ids_index, LinqDbTypes type)
        {
            List<KeyValuePair<int, int>> res_int = null;
            List<KeyValuePair<int, double>> res_double = null;

            int max = Int32.MinValue;
            bool is_sorted = true;
            int curr = Int32.MinValue;
            for (int i = 0; i < ids.Count(); i++)
            {
                if (max < ids[i])
                {
                    max = ids[i];
                }
                if (is_sorted && curr > ids[i])
                {
                    is_sorted = false;
                }
                curr = ids[i];
            }

            int max_size = 300000000;
            if (max < max_size)
            {
                List<bool> check_list = new List<bool>(max + 1);
                for (int i = 0; i < max + 1; i++)
                {
                    check_list.Add(false);
                }
                foreach (var id in ids)
                {
                    check_list[id] = true;
                }

                if (type == LinqDbTypes.int_)
                {
                    res_int = new List<KeyValuePair<int, int>>(ids.Count());
                    for (int i = 0; i < index.Parts.Count(); i++)
                    {
                        var val_part = index.Parts[i];
                        var iids = ids_index.Parts[i].IntValues;
                        int icount = iids.Count();
                        for (int j = 0; j < icount; j++)
                        {
                            var id = iids[j];
                            if (id <= max && check_list[id])
                            {
                                res_int.Add(new KeyValuePair<int, int>(id, val_part.IntValues[j]));
                            }
                        }
                    }
                }
                else
                {
                    res_double = new List<KeyValuePair<int, double>>(ids.Count());
                    for (int i = 0; i < index.Parts.Count(); i++)
                    {
                        var val_part = index.Parts[i];
                        var iids = ids_index.Parts[i].IntValues;
                        int icount = iids.Count();
                        for (int j = 0; j < icount; j++)
                        {
                            var id = iids[j];
                            if (id <= max && check_list[id])
                            {
                                res_double.Add(new KeyValuePair<int, double>(id, val_part.DoubleValues[j]));
                            }
                        }
                    }
                }
                return new Tuple<List<KeyValuePair<int, int>>, List<KeyValuePair<int, double>>, List<int>>(res_int, res_double, null);
            }
            else
            {
                int bloom_size = 30000000;
                List<bool> bloom_filter = new List<bool>(bloom_size);
                for (int i = 0; i < bloom_size; i++)
                {
                    bloom_filter.Add(false);
                }
                foreach (var id in ids)
                {
                    bloom_filter[id % bloom_size] = true;
                }

                if (!is_sorted)
                {
                    ids = ids.OrderBy(f => f).ToList();
                }

                if (type == LinqDbTypes.int_)
                {
                    res_int = new List<KeyValuePair<int, int>>(ids.Count());
                    for (int i = 0; i < ids_index.Parts.Count(); i++)
                    {
                        var val_part = index.Parts[i];
                        var iids = ids_index.Parts[i].IntValues;
                        int icount = iids.Count();
                        for (int j = 0; j < icount; j++)
                        {
                            var id = iids[j];
                            if (bloom_filter[id % bloom_size] && ids.BinarySearch(id) >= 0)
                            {
                                res_int.Add(new KeyValuePair<int, int>(id, val_part.IntValues[j]));
                            }
                        }
                    }
                }
                else
                {
                    res_double = new List<KeyValuePair<int, double>>(ids.Count());
                    for (int i = 0; i < ids_index.Parts.Count(); i++)
                    {
                        var val_part = index.Parts[i];
                        var iids = ids_index.Parts[i].IntValues;
                        int icount = iids.Count();
                        for (int j = 0; j < icount; j++)
                        {
                            var id = iids[j];
                            if (bloom_filter[id % bloom_size] && ids.BinarySearch(id) >= 0)
                            {
                                res_double.Add(new KeyValuePair<int, double>(id, val_part.DoubleValues[j]));
                            }
                        }
                    }
                }
                return new Tuple<List<KeyValuePair<int, int>>, List<KeyValuePair<int, double>>, List<int>>(res_int, res_double, ids);
            }
        }

        Dictionary<int, int> OrderSomeValuesById(OrderByInfo order_info, OperResult res_set, int? take, int? skip, bool desc, Dictionary<long, byte[]> cache, ReadOptions ro, TableInfo table_info)
        {
            Dictionary<int, int> res = new Dictionary<int, int>();
            var ids = res_set.ResIds;
            if (!IsSorted(ids))
            {
                ids = ids.OrderBy(f => f).ToList();
            }

            if (!desc)
            {
                if (skip == null)
                {
                    skip = 0;
                }
                int count = 0;
                for (int i = 0; i < ids.Count(); i++)
                {
                    if (skip > 0)
                    {
                        skip--;
                        continue;
                    }
                    if (take == null || take > 0)
                    {
                        if (take > 0)
                        {
                            take--;
                        }
                        res[ids[i]] = count;
                        count++;
                    }
                    else if (take == 0)
                    {
                        break;
                    }
                }
            }
            else
            {
                if (skip == null)
                {
                    skip = 0;
                }
                int count = 0;
                for (int i = ids.Count() - 1; i >= 0; i--)
                {
                    if (skip > 0)
                    {
                        skip--;
                        continue;
                    }
                    if (take == null || take > 0)
                    {
                        if (take > 0)
                        {
                            take--;
                        }
                        res[ids[i]] = count;
                        count++;
                    }
                    else if (take == 0)
                    {
                        break;
                    }
                }
            }
            return res;
        }
        Dictionary<int, int> OrderSomeValues(OrderByInfo order_info, OperResult res_set, int? take, int? skip, bool desc, Dictionary<long, byte[]> cache, ReadOptions ro, TableInfo table_info)
        {
            if (order_info.ColumnName != "Id")
            {
                var resindex = OrderSomeValuesWithIndex(order_info, res_set, take, skip, desc, cache, ro, table_info);
                if (resindex != null)
                {
                    return resindex;
                }
            }
            else
            {
                return OrderSomeValuesById(order_info, res_set, take, skip, desc, cache, ro, table_info);
            }

            Dictionary<int, int> res = new Dictionary<int, int>();
            List<KeyValuePair<object, int>> tmp_res = new List<KeyValuePair<object, int>>(res_set.ResIds.Count());

            foreach (var r in res_set.ResIds)
            {
                if (order_info.ColumnName == "Id")
                {
                    tmp_res.Add(new KeyValuePair<object, int>(r, r));
                }
                else
                {
                    byte[] val = null;
                    //if (!GetFromCache(order_info.ColumnNumber, r, cache, out val))
                    //{
                    if (order_info.ColumnType == LinqDbTypes.string_)
                    {
                        var key = MakeStringValueKey(new IndexKeyInfo()
                        {
                            TableNumber = order_info.TableNumber,
                            ColumnNumber = order_info.ColumnNumber,
                            Id = r
                        });
                        val = leveld_db.Get(key, null, ro);
                    }
                    else
                    {
                        var key = MakeValueKey(new IndexKeyInfo()
                        {
                            TableNumber = order_info.TableNumber,
                            ColumnNumber = order_info.ColumnNumber,
                            Id = r
                        });
                        val = leveld_db.Get(key, null, ro);
                    }
                    //}

                    if (order_info.ColumnType != LinqDbTypes.string_ && ValsEqual(val, NullConstant) ||
                        order_info.ColumnType == LinqDbTypes.string_ && ValsEqual(val, BinaryOrStringValuePrefix))
                    {
                        tmp_res.Add(new KeyValuePair<object, int>(null, r));
                    }
                    else
                    {
                        if (order_info.ColumnType == LinqDbTypes.int_)
                        {
                            tmp_res.Add(new KeyValuePair<object, int>(BitConverter.ToInt32(val.MyReverseWithCopy(), 0), r));
                        }
                        else if (order_info.ColumnType == LinqDbTypes.double_ || order_info.ColumnType == LinqDbTypes.DateTime_)
                        {
                            tmp_res.Add(new KeyValuePair<object, int>(BitConverter.ToDouble(val.MyReverseWithCopy(), 0), r));
                        }
                        else if (order_info.ColumnType == LinqDbTypes.string_)
                        {
                            tmp_res.Add(new KeyValuePair<object, int>(Encoding.Unicode.GetString(val), r));
                        }
                    }
                }
            }

            List<KeyValuePair<object, int>> t = null;
            if (!desc)
            {
                t = tmp_res.OrderBy(f => f.Key).ToList();
            }
            else
            {
                t = tmp_res.OrderByDescending(f => f.Key).ToList();
            }
            if (skip != null)
            {
                t = t.Skip((int)skip).ToList();
            }
            if (take != null)
            {
                t = t.Take((int)take).ToList();
            }

            int count = 0;
            foreach (var v in t)
            {
                res[v.Value] = count;
                count++;
            }
            return res;
        }

    }

    public class ContainsInfo
    {
        public List<bool> check_list { get; set; }
        public List<bool> bloom_filter { get; set; }
        public List<int> ids { get; set; }
        public bool is_sorted { get; set; }
        public int max { get; set; }
        public bool small { get; set; }
    }
    public class OrderedTmpResult
    {
        public Dictionary<int, int> Data { get; set; }
        public int? Skip { get; set; }
        public int? Take { get; set; }
    }
    public class OrderByInfo
    {
        public short TableNumber { get; set; }
        public short ColumnNumber { get; set; }
        public LinqDbTypes ColumnType { get; set; }
        public bool IsDescending { get; set; }
        public string ColumnName { get; set; }
    }

    public class OrderingInfo
    {
        public List<OrderByInfo> Orderings { get; set; }
        public int? Skip { get; set; }
        public int? Take { get; set; }
    }


}
