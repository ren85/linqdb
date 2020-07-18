using RocksDbSharp;
using ServerSharedData;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public void CreateGroupByIndexString(string table_name, string group_name, string value_name)
        {
            var _write_lock = GetTableWriteLock(table_name);
            lock (_write_lock)
            {

                using (var snapshot = leveld_db.CreateSnapshot())
                {
                    var table_info = GetTableInfo(table_name);
                    if (table_info.Columns[value_name] == LinqDbTypes.binary_ || table_info.Columns[value_name] == LinqDbTypes.string_)
                    {
                        throw new LinqDbException("Linqdb: Property type is not supported as memory index: " + value_name);
                    }
                    if (table_info.Columns[group_name] != LinqDbTypes.int_)
                    {
                        throw new LinqDbException("Linqdb: Property type is not supported as group by column: " + group_name);
                    }

                    string snapshot_id = Ldb.GetNewSpnapshotId();
                    var ro = new ReadOptions().SetSnapshot(snapshot);
                    int total = GetTableRowCount(table_info, ro);
                    List<int> ids = null;
                    if (!existing_indexes.ContainsKey(table_name) || !existing_indexes[table_name].Contains("Id"))
                    {
                        IndexGeneric index = new IndexGeneric()
                        {
                            ColumnName = "Id",
                            ColumnType = LinqDbTypes.int_,
                            TypeName = table_name,
                            Parts = new List<IndexPart>(),
                            IndexType = IndexType.PropertyOnly
                        };
                        ids = ReadAllIds(table_info, ro, total);
                        int counter = 0;
                        IndexPart cpart = null;
                        foreach (var id in ids)
                        {
                            if (counter % 1000 == 0)
                            {
                                cpart = new IndexPart() { IntValues = new List<int>(1000) };
                                index.Parts.Add(cpart);
                            }
                            cpart.IntValues.Add(id);
                            counter++;
                        }
                        if (!existing_indexes.ContainsKey(table_name))
                        {
                            existing_indexes[table_name] = new HashSet<string>() { "Id" };
                        }
                        else
                        {
                            existing_indexes[table_name].Add("Id");
                        }
                        indexes[table_name + "|Id|" + snapshot_id] = index;
                        latest_snapshots[table_name + "|Id"] = snapshot_id;
                        snapshots_alive.TryAdd(table_name + "|Id", new List<Tuple<bool, string>>() { new Tuple<bool, string>(false, table_name + "|Id|" + snapshot_id) });
                        last_cleanup.TryAdd(table_name + "|Id", DateTime.Now);

                        //
                        var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers["Id"]);
                        leveld_db.Put(skey, Encoding.UTF8.GetBytes(snapshot_id));
                    }
                    else
                    {
                        ids = new List<int>(total);
                        var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers["Id"]);
                        var snapid = leveld_db.Get(skey, null, ro);
                        var id_snapshot_id = Encoding.UTF8.GetString(snapid);
                        var index = indexes[table_name + "|Id|" + id_snapshot_id];
                        for (int i = 0; i < index.Parts.Count(); i++)
                        {
                            ids.AddRange(index.Parts[i].IntValues);
                        }
                    }


                    if (!existing_indexes[table_name].Contains(group_name))
                    {
                        IndexGeneric index = new IndexGeneric()
                        {
                            ColumnName = group_name,
                            ColumnType = LinqDbTypes.int_,
                            TypeName = table_name,
                            GroupListMapping = new ConcurrentDictionary<int, int>(),
                            Parts = new List<IndexPart>(),
                            IndexType = IndexType.GroupOnly
                        };
                        int totalex = 0;
                        if ((ids.Any() ? ids.Max() : 0) < 250000000)
                        {
                            var ivalues = ReadAllIntValuesList(index.ColumnName, table_info, ro, ids.Any() ? ids.Max() : 0, out totalex);
                            if (totalex != ids.Count())
                            {
                                throw new LinqDbException("Linqdb: column " + index.ColumnName + " has gaps in data. Prior to building an index gaps must be updated with values. (" + totalex + " != " + ids.Count() + ")");
                            }
                            int map = 0;
                            foreach (var id in ids)
                            {
                                if (!index.GroupListMapping.ContainsKey(ivalues[id]))
                                {
                                    index.GroupListMapping[ivalues[id]] = map;
                                    map++;
                                }
                            }

                            var latest_snapshot_id = latest_snapshots[table_name + "|Id"];
                            var id_index = indexes[table_name + "|Id|" + latest_snapshot_id];
                            for (int j = 0; j < id_index.Parts.Count(); j++)
                            {
                                var part = id_index.Parts[j];
                                var new_part = new IndexPart() { GroupValues = new List<int>(part.IntValues.Count()) };
                                index.Parts.Add(new_part);
                                for (int k = 0; k < part.IntValues.Count(); k++)
                                {
                                    var ival = ivalues[part.IntValues[k]];
                                    var val = index.GroupListMapping[ival];
                                    new_part.GroupValues.Add(val);
                                }
                            }
                        }
                        else
                        {
                            var ivalues = ReadAllIntValuesDic(index.ColumnName, table_info, ro, ids.Any() ? ids.Max() : 0, total, out totalex);
                            if (totalex != ids.Count())
                            {
                                throw new LinqDbException("Linqdb: column " + index.ColumnName + " has gaps in data. Prior to building an index gaps must be updated with values. (" + totalex + " != " + ids.Count() + ")");
                            }
                            int map = 0;
                            foreach (var id in ids)
                            {
                                if (!index.GroupListMapping.ContainsKey(ivalues[id]))
                                {
                                    index.GroupListMapping[ivalues[id]] = map;
                                    map++;
                                }
                            }

                            var latest_snapshot_id = latest_snapshots[table_name + "|Id"];
                            var id_index = indexes[table_name + "|Id|" + latest_snapshot_id];
                            for (int j = 0; j < id_index.Parts.Count(); j++)
                            {
                                var part = id_index.Parts[j];
                                var new_part = new IndexPart() { GroupValues = new List<int>(part.IntValues.Count()) };
                                index.Parts.Add(new_part);
                                for (int k = 0; k < part.IntValues.Count(); k++)
                                {
                                    var ival = ivalues[part.IntValues[k]];
                                    var val = index.GroupListMapping[ival];
                                    new_part.GroupValues.Add(val);
                                }
                            }
                        }

                        existing_indexes[table_name].Add(group_name);
                        indexes[table_name + "|" + group_name + "|" + snapshot_id] = index;
                        latest_snapshots[table_name + "|" + group_name] = snapshot_id;
                        snapshots_alive.TryAdd(table_name + "|" + group_name, new List<Tuple<bool, string>>() { new Tuple<bool, string>(false, table_name + "|" + group_name + "|" + snapshot_id) });
                        last_cleanup.TryAdd(table_name + "|" + group_name, DateTime.Now);

                        //
                        var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[group_name]);
                        leveld_db.Put(skey, Encoding.UTF8.GetBytes(snapshot_id));
                    }
                    else //could be property index, we need a group one
                    {
                        var latest_snapshot_id = latest_snapshots[table_name + "|" + group_name];
                        var index = indexes[table_name + "|" + group_name + "|" + latest_snapshot_id];
                        if (index.IndexType == IndexType.PropertyOnly)
                        {
                            index.GroupListMapping = new ConcurrentDictionary<int, int>();
                            int map = 0;
                            for (int i = 0; i < index.Parts.Count(); i++)
                            {
                                int icount = index.Parts[i].IntValues.Count();
                                index.Parts[i].GroupValues = new List<int>(icount);
                                var gv = index.Parts[i].GroupValues;
                                for (int j = 0; j < icount; j++)
                                {
                                    var val = index.Parts[i].IntValues[j];
                                    var ival = (int)val;
                                    if (!index.GroupListMapping.ContainsKey(ival))
                                    {
                                        index.GroupListMapping[ival] = map;
                                        map++;
                                    }
                                    gv.Add(index.GroupListMapping[ival]);
                                }
                            }
                            index.IndexType = IndexType.Both;
                        }
                    }

                    if (!existing_indexes[table_name].Contains(value_name))
                    {
                        IndexGeneric index = new IndexGeneric()
                        {
                            ColumnName = value_name,
                            TypeName = table_name,
                            Parts = new List<IndexPart>(),
                            IndexType = IndexType.PropertyOnly
                        };

                        if (ids == null)
                        {
                            ids = ReadAllValues("Id", table_info, new OperResult() { All = true }, ro, new ReadByteCount() { read_size_bytes = -Int32.MaxValue }, total).Item1;
                        }


                        var latest_snapshot_id = latest_snapshots[table_name + "|Id"];
                        switch (table_info.Columns[value_name])
                        {
                            case LinqDbTypes.int_:
                                index.ColumnType = LinqDbTypes.int_;
                                int totalex = 0;
                                if ((ids.Any() ? ids.Max() : 0) < 250000000)
                                {
                                    var ivalues = ReadAllIntValuesList(index.ColumnName, table_info, ro, ids.Any() ? ids.Max() : 0, out totalex);
                                    if (totalex != ids.Count())
                                    {
                                        throw new LinqDbException("Linqdb: column " + index.ColumnName + " has gaps in data. Prior to building an index gaps must be updated with values. (" + totalex + " != " + ids.Count() + ")");
                                    }

                                    var id_index = indexes[table_name + "|Id|" + latest_snapshot_id];
                                    for (int j = 0; j < id_index.Parts.Count(); j++)
                                    {
                                        var part = id_index.Parts[j];
                                        var new_part = new IndexPart() { IntValues = new List<int>(part.IntValues.Count()) };
                                        index.Parts.Add(new_part);
                                        for (int k = 0; k < part.IntValues.Count(); k++)
                                        {
                                            var val = ivalues[(int)part.IntValues[k]];
                                            new_part.IntValues.Add(val);
                                        }
                                    }
                                }
                                else
                                {
                                    var ivalues = ReadAllIntValuesDic(index.ColumnName, table_info, ro, ids.Any() ? ids.Max() : 0, ids.Count(), out totalex);
                                    if (totalex != ids.Count())
                                    {
                                        throw new LinqDbException("Linqdb: column " + index.ColumnName + " has gaps in data. Prior to building an index gaps must be updated with values. (" + totalex + " != " + ids.Count() + ")");
                                    }

                                    var id_index = indexes[table_name + "|Id|" + latest_snapshot_id];
                                    for (int j = 0; j < id_index.Parts.Count(); j++)
                                    {
                                        var part = id_index.Parts[j];
                                        var new_part = new IndexPart() { IntValues = new List<int>(part.IntValues.Count()) };
                                        index.Parts.Add(new_part);
                                        for (int k = 0; k < part.IntValues.Count(); k++)
                                        {
                                            var val = ivalues[(int)part.IntValues[k]];
                                            new_part.IntValues.Add(val);
                                        }
                                    }
                                }
                                break;
                            case LinqDbTypes.double_:
                            case LinqDbTypes.DateTime_:
                                index.ColumnType = LinqDbTypes.double_;
                                int totalexd = 0;
                                if ((ids.Any() ? ids.Max() : 0) < 250000000)
                                {
                                    var dvalues = ReadAllDoubleValuesList(index.ColumnName, table_info, ro, ids.Any() ? ids.Max() : 0, out totalexd);
                                    if (totalexd != ids.Count())
                                    {
                                        throw new LinqDbException("Linqdb: column " + index.ColumnName + " has gaps in data. Prior to building an index gaps must be updated with values. (" + totalexd + " != " + ids.Count() + ")");
                                    }

                                    var id_index = indexes[table_name + "|Id|" + latest_snapshot_id];
                                    for (int j = 0; j < id_index.Parts.Count(); j++)
                                    {
                                        var part = id_index.Parts[j];
                                        var new_part = new IndexPart() { DoubleValues = new List<double>(part.IntValues.Count()) };
                                        index.Parts.Add(new_part);
                                        for (int k = 0; k < part.IntValues.Count(); k++)
                                        {
                                            var val = dvalues[(int)part.IntValues[k]];
                                            new_part.DoubleValues.Add(val);
                                        }
                                    }
                                }
                                else
                                {
                                    var dvalues = ReadAllDoubleValuesDic(index.ColumnName, table_info, ro, ids.Any() ? ids.Max() : 0, ids.Count(), out totalexd);
                                    if (totalexd != ids.Count())
                                    {
                                        throw new LinqDbException("Linqdb: column " + index.ColumnName + " has gaps in data. Prior to building an index gaps must be updated with values. (" + totalexd + " != " + ids.Count() + ")");
                                    }

                                    var id_index = indexes[table_name + "|Id|" + latest_snapshot_id];
                                    for (int j = 0; j < id_index.Parts.Count(); j++)
                                    {
                                        var part = id_index.Parts[j];
                                        var new_part = new IndexPart() { DoubleValues = new List<double>(part.IntValues.Count()) };
                                        index.Parts.Add(new_part);
                                        for (int k = 0; k < part.IntValues.Count(); k++)
                                        {
                                            var val = dvalues[(int)part.IntValues[k]];
                                            new_part.DoubleValues.Add(val);
                                        }
                                    }
                                }
                                break;
                        }

                        existing_indexes[table_name].Add(value_name);
                        indexes[table_name + "|" + value_name + "|" + snapshot_id] = index;
                        latest_snapshots[table_name + "|" + value_name] = snapshot_id;
                        snapshots_alive.TryAdd(table_name + "|" + value_name, new List<Tuple<bool, string>>() { new Tuple<bool, string>(false, table_name + "|" + value_name + "|" + snapshot_id) });
                        last_cleanup.TryAdd(table_name + "|" + value_name, DateTime.Now);

                        //
                        var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[value_name]);
                        leveld_db.Put(skey, Encoding.UTF8.GetBytes(snapshot_id));
                    }

                }

            }
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
        public void CreateGroupByIndex<T, TKey1, TKey2>(Expression<Func<T, TKey1>> groupPropertySelector, Expression<Func<T, TKey2>> valuePropertySelector)
        {
            CheckTableInfo<T>();
            var group_par = groupPropertySelector.Parameters.First();
            var group_name = SharedUtils.GetPropertyName(groupPropertySelector.Body.ToString());
            var value_par = valuePropertySelector.Parameters.First();
            var value_name = SharedUtils.GetPropertyName(valuePropertySelector.Body.ToString());
            CreateGroupByIndexString(typeof(T).Name, group_name, value_name);
            AddIndexToFile(typeof(T).Name, group_name, value_name);
        }

        static object _snapshot_lock = new object();
        static string GetNewSpnapshotId()
        {
            lock (_snapshot_lock)
            {
                Guid snapshot_id = Guid.NewGuid();
                return snapshot_id.ToString();
            }
        }
    }
}
