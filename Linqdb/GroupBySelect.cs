using RocksDbSharp;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public List<R> SelectGrouped<TKey, T, R>(QueryTree tree, Expression<Func<IGrouping<TKey, T>, R>> predicate, out int total) where T : new()
        {
            using (var snapshot = leveld_db.CreateSnapshot())
            {
                CompiledInfo<R> cinfo = new CompiledInfo<R>();
                var ro = new ReadOptions().SetSnapshot(snapshot);
                DateTime start = DateTime.Now;
                var table_info = GetTableInfo(typeof(T).Name);
                var where_res = CalculateWhereResult<T>(tree, table_info, ro);
                where_res = FindBetween(tree, where_res, ro);
                where_res = Intersect(tree, where_res, table_info, tree.QueryCache, ro);
                double? searchPercentile = null;
                where_res = Search(tree, where_res, ro, out searchPercentile);
                int row_count = GetTableRowCount(table_info, ro);

                var fres = CombineData(where_res);
                total = fres.All ? row_count : fres.ResIds.Count();
                fres = OrderData(fres, tree, row_count, ro, table_info);
                int count = fres.All ? row_count : fres.ResIds.Count();
                var body = predicate.Body;
                var new_expr = body as NewExpression;
                if (new_expr == null)
                {
                    throw new LinqDbException("Linqdb: Select must be with anonymous type, for example, .Select(f => new { Id = f.Id })");
                }

                List<Tuple<string, string>> aggregates = new List<Tuple<string, string>>();
                foreach (var a in new_expr.Arguments)
                {
                    var parts = a.ToString().Split(".".ToCharArray());
                    var function = parts[1];
                    string field = null;
                    if (parts.Count() > 2)
                    {
                        field = parts[2].Trim(" (),;".ToCharArray());
                    }
                    aggregates.Add(new Tuple<string, string>(function, field));
                }


                HashSet<int> distinct_groups = new HashSet<int>();
                var cdata = SelectGrouppedCommon(distinct_groups, fres, aggregates, table_info, tree.GroupingInfo.ColumnNumber, tree.GroupingInfo.ColumnName, ro, count);

                List<R> result = new List<R>();
                foreach (var gr in distinct_groups)
                {
                    var cresult = CreateType<R>(cinfo, cdata[gr]);
                    result.Add(cresult);
                }

                return result;
            }
        }

        public Dictionary<int, List<object>> SelectGrouppedCommon(HashSet<int> distinct_groups, OperResult fres, List<Tuple<string, string>> aggregates, TableInfo table_info, short column_number, string column_name, ReadOptions ro, int count)
        {
            Dictionary<int, List<object>> cdata = new Dictionary<int, List<object>>();
            //var max_id = 0;
            //List<byte> check_list = null;
            //int bloom_max = 2000000;
            //List<byte> bloom_int = null;

            List<int> groups = new List<int>();

            var skey = MakeSnapshotKey(table_info.TableNumber, /*tree.GroupingInfo.ColumnNumber*/column_number);
            var snapid = leveld_db.Get(skey, null, ro);
            if (snapid == null || !existing_indexes.ContainsKey(table_info.Name) || !existing_indexes[table_info.Name].Contains(column_name))
            {
                throw new LinqDbException("Linqdb: group index on property " + /*tree.GroupingInfo.ColumnName*/column_name + " does not exist.");
            }
            string snapshot_id = Encoding.UTF8.GetString(snapid);

            skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers["Id"]);
            snapid = leveld_db.Get(skey, null, ro);
            string id_snapshot_id = Encoding.UTF8.GetString(snapid);
            if (!indexes.ContainsKey(table_info.Name + "|" + /*tree.GroupingInfo.ColumnName*/column_name + "|" + snapshot_id))
            {
                throw new LinqDbException("Linqdb: group index on property " + /*tree.GroupingInfo.ColumnName*/column_name + " does not exist.");
            }
            var group_index = indexes[table_info.Name + "|" + /*tree.GroupingInfo.ColumnName*/column_name + "|" + snapshot_id];
            if (group_index.IndexType == IndexType.PropertyOnly)
            {
                throw new LinqDbException("Linqdb: group index on property " + /*tree.GroupingInfo.ColumnName*/column_name + " does not exist.");
            }
            var ids_index = indexes[table_info.Name + "|Id|" + id_snapshot_id];

            if (!fres.All)
            {
                var tmp = new IndexGeneric()
                {
                    ColumnName = group_index.ColumnName,
                    ColumnType = group_index.ColumnType,
                    TypeName = group_index.TypeName,
                    GroupListMapping = new ConcurrentDictionary<int, int>(group_index.GroupListMapping),
                    Parts = new List<IndexPart>() { new IndexPart() { GroupValues = new List<int>(count) } },
                    IndexType = group_index.IndexType
                };
                tmp.Parts[0].GroupValues = EfficientContainsOneCall(ids_index, group_index, fres.ResIds, true, false).Item1;
                group_index = tmp;
            }
            var prop_indexes = new Dictionary<string, IndexGeneric>();
            foreach (var agr in aggregates)
            {
                if (!string.IsNullOrEmpty(agr.Item2))
                {
                    var prop = agr.Item2;

                    if (!prop_indexes.ContainsKey(prop))
                    {
                        skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[prop]);
                        snapid = leveld_db.Get(skey, null, ro);
                        if (snapid == null || !existing_indexes.ContainsKey(table_info.Name) || !existing_indexes[table_info.Name].Contains(prop))
                        {
                            throw new LinqDbException("Linqdb: group index on property " + prop + " does not exist.");
                        }
                        snapshot_id = Encoding.UTF8.GetString(snapid);

                        var tmpk = table_info.Name + "|" + prop + "|" + snapshot_id;
                        if (!indexes.ContainsKey(tmpk))
                        {
                            throw new LinqDbException("Linqdb: group index on property " + prop + " does not exist.");
                        }

                        prop_indexes[prop] = indexes[tmpk];

                        if (!fres.All)
                        {
                            var tmp = new IndexGeneric()
                            {
                                ColumnName = prop_indexes[prop].ColumnName,
                                ColumnType = prop_indexes[prop].ColumnType,
                                TypeName = prop_indexes[prop].TypeName,
                                Parts = new List<IndexPart>() { new IndexPart() { IntValues = new List<int>(fres.ResIds.Count()), DoubleValues = new List<double>(fres.ResIds.Count()) } },
                            };
                            switch (tmp.ColumnType)
                            {
                                case LinqDbTypes.int_:
                                    tmp.Parts[0].IntValues = EfficientContainsOneCall(ids_index, prop_indexes[prop], fres.ResIds, false, false).Item1;
                                    break;
                                case LinqDbTypes.double_:
                                case LinqDbTypes.DateTime_:
                                    tmp.Parts[0].DoubleValues = EfficientContainsOneCall(ids_index, prop_indexes[prop], fres.ResIds, false, true).Item2;
                                    break;
                                default:
                                    break;
                            }
                            prop_indexes[prop] = tmp;
                        }
                    }
                }
            }

            int max = group_index.GroupListMapping.Max(f => f.Value) + 1;
            var group_check_list = new List<bool>(max);
            for (int i = 0; i < max; i++)
            {
                group_check_list.Add(false);
            }
            for (int i = 0; i < group_index.Parts.Count(); i++)
            {
                int icount = group_index.Parts[i].GroupValues.Count();
                var gr = group_index.Parts[i].GroupValues;
                for (int j = 0; j < icount; j++)
                {
                    group_check_list[gr[j]] = true;
                }
            }
            for (int i = 0; i < group_check_list.Count(); i++)
            {
                if (group_check_list[i])
                {
                    distinct_groups.Add(i);
                }
            }

            foreach (var group in distinct_groups)
            {
                cdata[group] = new List<object>();
            }
            foreach (var agr in aggregates)
            {
                string prop = "";
                if (!string.IsNullOrEmpty(agr.Item2))
                {
                    prop = agr.Item2;
                }
                if (agr.Item1.StartsWith("Key"))
                {
                    foreach (var gr in cdata)
                    {
                        gr.Value.Add(group_index.GroupListMapping.Where(f => f.Value == gr.Key).First().Key);
                    }
                }
                else if (agr.Item1.StartsWith("CountDistinct"))
                {
                    int max_val = group_index.GroupListMapping.Max(f => f.Value) + 1;
                    int bloom_size = 100000;

                    switch (prop_indexes[prop].ColumnType)
                    {
                        case LinqDbTypes.int_:
                            var bloom_int = new List<List<bool>>(max_val);
                            var last_ints = new List<List<int>>(max_val);
                            for (int i = 0; i < max_val; i++)
                            {
                                var last_bloom = new List<bool>(bloom_size);
                                bloom_int.Add(last_bloom);
                                var last_int = new List<int>(bloom_size);
                                last_ints.Add(last_int);
                                for (int j = 0; j < bloom_size; j++)
                                {
                                    last_bloom.Add(false);
                                    last_int.Add(0);
                                }
                            }
                            var list_int = new List<HashSet<int>>(max_val);
                            for (int i = 0; i < max_val; i++)
                            {
                                list_int.Add(new HashSet<int>());
                            }
                            var int_values = prop_indexes[prop];
                            for (int i = 0; i < group_index.Parts.Count(); i++)
                            {
                                int icount = group_index.Parts[i].GroupValues.Count();
                                var iv = int_values.Parts[i].IntValues;
                                var gv = group_index.Parts[i].GroupValues;
                                for (int j = 0; j < icount; j++)
                                {
                                    int val = iv[j];
                                    var bl = bloom_int[gv[j]];
                                    var li = last_ints[gv[j]];
                                    var hash = val % bloom_size;
                                    if (!bl[hash] || li[hash] != val)
                                    {
                                        list_int[gv[j]].Add(val);
                                        bl[hash] = true;
                                        li[hash] = val;
                                    }
                                }
                            }
                            foreach (var gr in cdata)
                            {
                                gr.Value.Add(list_int[gr.Key].Count());
                            }
                            break;
                        case LinqDbTypes.double_:
                            var bloom_double = new List<List<bool>>(max_val);
                            var last_doubles = new List<List<double>>(max_val);
                            for (int i = 0; i < max_val; i++)
                            {
                                var last_bloom = new List<bool>(bloom_size);
                                bloom_double.Add(last_bloom);
                                var last_double = new List<double>(bloom_size);
                                last_doubles.Add(last_double);
                                for (int j = 0; j < bloom_size; j++)
                                {
                                    last_bloom.Add(false);
                                    last_double.Add(0);
                                }
                            }

                            var list_double = new List<HashSet<double>>(max_val);
                            for (int i = 0; i < max_val; i++)
                            {
                                list_double.Add(new HashSet<double>());
                            }
                            var double_values = prop_indexes[prop];
                            for (int i = 0; i < group_index.Parts.Count(); i++)
                            {
                                int icount = group_index.Parts[i].GroupValues.Count();
                                var dv = double_values.Parts[i].DoubleValues;
                                var gv = group_index.Parts[i].GroupValues;
                                for (int j = 0; j < icount; j++)
                                {
                                    double val = dv[j];
                                    var bl = bloom_double[gv[j]];
                                    var li = last_doubles[gv[j]];
                                    var hash = Math.Abs(val.GetHashCode()) % bloom_size;
                                    if (!bl[hash] || li[hash] != val)
                                    {
                                        list_double[gv[j]].Add(val);
                                        bl[hash] = true;
                                        li[hash] = val;
                                    }
                                }
                            }
                            foreach (var gr in cdata)
                            {
                                gr.Value.Add(list_double[gr.Key].Count());
                            }
                            break;
                        default:
                            break;
                    }
                }
                else if (agr.Item1.StartsWith("Count"))
                {
                    int max_val = group_index.GroupListMapping.Max(f => f.Value) + 1;

                    var list = new List<int>(max_val);
                    for (int i = 0; i < max_val; i++)
                    {
                        list.Add(0);
                    }
                    for (int i = 0; i < group_index.Parts.Count(); i++)
                    {
                        int icount = group_index.Parts[i].GroupValues.Count();
                        var gv = group_index.Parts[i].GroupValues;
                        for (int j = 0; j < icount; j++)
                        {
                            list[gv[j]]++;
                        }
                    }
                    foreach (var gr in cdata)
                    {
                        gr.Value.Add(list[(int)gr.Key]);
                    }
                }
                else if (agr.Item1.StartsWith("Sum"))
                {
                    int max_val = group_index.GroupListMapping.Max(f => f.Value) + 1;

                    var list_int = new List<int>(max_val);
                    for (int i = 0; i < max_val; i++)
                    {
                        list_int.Add(0);
                    }
                    var list_double = new List<double>(max_val);
                    for (int i = 0; i < max_val; i++)
                    {
                        list_double.Add(0);
                    }

                    switch (prop_indexes[prop].ColumnType)
                    {
                        case LinqDbTypes.int_:
                            var int_values = prop_indexes[prop];
                            for (int i = 0; i < group_index.Parts.Count(); i++)
                            {
                                int icount = group_index.Parts[i].GroupValues.Count();
                                var iv = int_values.Parts[i].IntValues;
                                var gv = group_index.Parts[i].GroupValues;
                                for (int j = 0; j < icount; j++)
                                {
                                    list_int[gv[j]] += iv[j];
                                }
                            }
                            foreach (var gr in cdata)
                            {
                                gr.Value.Add(list_int[gr.Key]);
                            }
                            break;
                        case LinqDbTypes.double_:
                            var double_values = prop_indexes[prop];
                            for (int i = 0; i < group_index.Parts.Count(); i++)
                            {
                                int icount = group_index.Parts[i].GroupValues.Count();
                                var dv = double_values.Parts[i].DoubleValues;
                                var gv = group_index.Parts[i].GroupValues;
                                for (int j = 0; j < icount; j++)
                                {
                                    list_double[gv[j]] += dv[j];
                                }
                            }
                            foreach (var gr in cdata)
                            {
                                gr.Value.Add(list_double[gr.Key]);
                            }
                            break;
                        default:
                            break;
                    }
                }
                else if (agr.Item1.StartsWith("Max"))
                {
                    int max_val = group_index.GroupListMapping.Max(f => f.Value) + 1;

                    var list_int = new List<int?>(max_val);
                    for (int i = 0; i < max_val; i++)
                    {
                        list_int.Add(null);
                    }
                    var list_double = new List<double?>(max_val);
                    for (int i = 0; i < max_val; i++)
                    {
                        list_double.Add(null);
                    }
                    switch (prop_indexes[prop].ColumnType)
                    {
                        case LinqDbTypes.int_:
                            var int_values = prop_indexes[prop];
                            int? igval = null;
                            for (int i = 0; i < group_index.Parts.Count(); i++)
                            {
                                int icount = group_index.Parts[i].GroupValues.Count();
                                var iv = int_values.Parts[i].IntValues;
                                var gv = group_index.Parts[i].GroupValues;
                                for (int j = 0; j < icount; j++)
                                {
                                    int g = gv[j];
                                    int ij = iv[j];
                                    igval = list_int[g];
                                    if (igval == null || igval < ij)
                                    {
                                        list_int[g] = ij;
                                    }
                                }
                            }
                            foreach (var gr in cdata)
                            {
                                gr.Value.Add(list_int[gr.Key]);
                            }
                            break;
                        case LinqDbTypes.double_:
                            var double_values = prop_indexes[prop];
                            double? gval = null;
                            for (int i = 0; i < group_index.Parts.Count(); i++)
                            {
                                int icount = group_index.Parts[i].GroupValues.Count();
                                var dv = double_values.Parts[i].DoubleValues;
                                var gv = group_index.Parts[i].GroupValues;
                                for (int j = 0; j < icount; j++)
                                {
                                    int g = gv[j];
                                    double d = dv[j];
                                    gval = list_double[g];
                                    if (gval == null || gval < d)
                                    {
                                        list_double[g] = d;
                                    }
                                }
                            }
                            foreach (var gr in cdata)
                            {
                                gr.Value.Add(list_double[gr.Key]);
                            }
                            break;
                        default:
                            break;
                    }
                }
                else if (agr.Item1.StartsWith("Min"))
                {
                    int max_val = group_index.GroupListMapping.Max(f => f.Value) + 1;

                    var list_int = new List<int?>(max_val);
                    for (int i = 0; i < max_val; i++)
                    {
                        list_int.Add(null);
                    }
                    var list_double = new List<double?>(max_val);
                    for (int i = 0; i < max_val; i++)
                    {
                        list_double.Add(null);
                    }
                    switch (prop_indexes[prop].ColumnType)
                    {
                        case LinqDbTypes.int_:
                            var int_values = prop_indexes[prop];
                            int? igval = null;
                            for (int i = 0; i < group_index.Parts.Count(); i++)
                            {
                                int icount = group_index.Parts[i].GroupValues.Count();
                                var iv = int_values.Parts[i].IntValues;
                                var gv = group_index.Parts[i].GroupValues;
                                for (int j = 0; j < icount; j++)
                                {
                                    int g = gv[j];
                                    igval = list_int[g];
                                    if (igval == null || igval > iv[j])
                                    {
                                        list_int[g] = iv[j];
                                    }
                                }
                            }
                            foreach (var gr in cdata)
                            {
                                gr.Value.Add(list_int[gr.Key]);
                            }
                            break;
                        case LinqDbTypes.double_:
                            var double_values = prop_indexes[prop];
                            double? gval = null;
                            for (int i = 0; i < group_index.Parts.Count(); i++)
                            {
                                int icount = group_index.Parts[i].GroupValues.Count();
                                var dv = double_values.Parts[i].DoubleValues;
                                var gv = group_index.Parts[i].GroupValues;
                                for (int j = 0; j < icount; j++)
                                {
                                    int g = gv[j];
                                    gval = list_double[g];
                                    if (gval == null || gval > dv[j])
                                    {
                                        list_double[g] = dv[j];
                                    }
                                }
                            }
                            foreach (var gr in cdata)
                            {
                                gr.Value.Add(list_double[gr.Key]);
                            }
                            break;
                        default:
                            break;
                    }
                }
                else if (agr.Item1.StartsWith("Average"))
                {
                    int max_val = group_index.GroupListMapping.Max(f => f.Value) + 1;

                    var total_count = new List<int>(max_val);
                    for (int i = 0; i < max_val; i++)
                    {
                        total_count.Add(0);
                    }
                    var list_int = new List<int>(max_val);
                    for (int i = 0; i < max_val; i++)
                    {
                        list_int.Add(0);
                    }
                    var list_double = new List<double>(max_val);
                    for (int i = 0; i < max_val; i++)
                    {
                        list_double.Add(0);
                    }
                    switch (prop_indexes[prop].ColumnType)
                    {
                        case LinqDbTypes.int_:
                            var int_values = prop_indexes[prop];
                            for (int i = 0; i < group_index.Parts.Count(); i++)
                            {
                                int icount = group_index.Parts[i].GroupValues.Count();
                                var iv = int_values.Parts[i].IntValues;
                                var gv = group_index.Parts[i].GroupValues;
                                for (int j = 0; j < icount; j++)
                                {
                                    int g = gv[j];
                                    list_int[g] += iv[j];
                                    total_count[g]++;
                                }
                            }
                            foreach (var gr in cdata)
                            {
                                if (total_count[gr.Key] != 0)
                                {
                                    gr.Value.Add(list_int[gr.Key] / (double)total_count[gr.Key]);
                                }
                                else
                                {
                                    gr.Value.Add(list_int[gr.Key]);
                                }
                            }
                            break;
                        case LinqDbTypes.double_:
                            var double_values = prop_indexes[prop];
                            for (int i = 0; i < group_index.Parts.Count(); i++)
                            {
                                int icount = group_index.Parts[i].GroupValues.Count();
                                var dv = double_values.Parts[i].DoubleValues;
                                var gv = group_index.Parts[i].GroupValues;
                                for (int j = 0; j < icount; j++)
                                {
                                    int g = gv[j];
                                    list_double[g] += dv[j];
                                    total_count[g]++;
                                }
                            }
                            foreach (var gr in cdata)
                            {
                                if (total_count[gr.Key] != 0)
                                {
                                    gr.Value.Add(list_double[gr.Key] / (double)total_count[gr.Key]);
                                }
                                else
                                {
                                    gr.Value.Add(list_double[gr.Key]);
                                }
                            }
                            break;
                        default:
                            break;
                    }
                }
                else
                {
                    throw new LinqDbException("Linqdb: group aggregation function " + agr.Item1 + " is not supported.");
                }
            }

            return cdata;
        }
    }
}
