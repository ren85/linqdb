using RocksDbSharp;
using ServerSharedData;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public void CreatePropIndex(string type_name, string value_name)
        {
            var _write_lock = GetTableWriteLock(type_name);
            lock (_write_lock)
            {
                    using (var snapshot = leveld_db.CreateSnapshot())
                    {
                        var table_info = GetTableInfo(type_name);
                        if (table_info.Columns[value_name] == LinqDbTypes.binary_ || table_info.Columns[value_name] == LinqDbTypes.string_)
                        {
                            throw new LinqDbException("Linqdb: Property type is not supported as memory index: " + value_name);
                        }
                        string snapshot_id = Ldb.GetNewSpnapshotId();
                        var ro = new ReadOptions().SetSnapshot(snapshot);
                        int total = GetTableRowCount(table_info, ro);
                        List<int> ids = null;
                        if (!existing_indexes.ContainsKey(type_name) || !existing_indexes[type_name].Contains("Id"))
                        {
                            IndexGeneric index = new IndexGeneric()
                            {
                                ColumnName = "Id",
                                ColumnType = LinqDbTypes.int_,
                                TypeName = type_name,
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
                            if (!existing_indexes.ContainsKey(type_name))
                            {
                                existing_indexes[type_name] = new HashSet<string>() { "Id" };
                            }
                            else
                            {
                                existing_indexes[type_name].Add("Id");
                            }
                            indexes[type_name + "|Id|" + snapshot_id] = index;
                            latest_snapshots[type_name + "|Id"] = snapshot_id;
                            snapshots_alive.TryAdd(type_name + "|Id", new List<Tuple<bool, string>>() { new Tuple<bool, string>(false, type_name + "|Id|" + snapshot_id) });
                            last_cleanup.TryAdd(type_name + "|Id", DateTime.Now);

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
                            var index = indexes[type_name + "|Id|" + id_snapshot_id];
                            for (int i = 0; i < index.Parts.Count(); i++)
                            {
                                ids.AddRange(index.Parts[i].IntValues);
                            }
                        }

                        if (!existing_indexes[type_name].Contains(value_name))
                        {
                            IndexGeneric index = new IndexGeneric()
                            {
                                ColumnName = value_name,
                                TypeName = type_name,
                                Parts = new List<IndexPart>(),
                                IndexType = IndexType.PropertyOnly
                            };


                            var latest_snapshot_id = latest_snapshots[type_name + "|Id"];
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
                                            throw new LinqDbException("Linqdb: column " + index.ColumnName + " has gaps in data. Prior to building an index gaps must be updated with values.");
                                        }

                                        var id_index = indexes[type_name + "|Id|" + latest_snapshot_id];
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
                                            throw new LinqDbException("Linqdb: column " + index.ColumnName + " has gaps in data. Prior to building an index gaps must be updated with values.");
                                        }

                                        var id_index = indexes[type_name + "|Id|" + latest_snapshot_id];
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
                                            throw new LinqDbException("Linqdb: column " + index.ColumnName + " has gaps in data. Prior to building an index gaps must be updated with values.");
                                        }

                                        var id_index = indexes[type_name + "|Id|" + latest_snapshot_id];
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
                                            throw new LinqDbException("Linqdb: column " + index.ColumnName + " has gaps in data. Prior to building an index gaps must be updated with values.");
                                        }

                                        var id_index = indexes[type_name + "|Id|" + latest_snapshot_id];
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

                            existing_indexes[type_name].Add(value_name);
                            indexes[type_name + "|" + value_name + "|" + snapshot_id] = index;
                            latest_snapshots[type_name + "|" + value_name] = snapshot_id;
                            snapshots_alive.TryAdd(type_name + "|" + value_name, new List<Tuple<bool, string>>() { new Tuple<bool, string>(false, type_name + "|" + value_name + "|" + snapshot_id) });
                            last_cleanup.TryAdd(type_name + "|" + value_name, DateTime.Now);

                            //
                            var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[value_name]);
                            leveld_db.Put(skey, Encoding.UTF8.GetBytes(snapshot_id));
                        }
                        else //could be group index, we need property one
                        {
                            var latest_snapshot_id = latest_snapshots[type_name + "|" + value_name];
                            var index = indexes[type_name + "|" + value_name + "|" + latest_snapshot_id];
                            if (index.IndexType == IndexType.GroupOnly)
                            {
                                var rdic = new Dictionary<int, int>(index.GroupListMapping.Count());
                                foreach (var d in index.GroupListMapping)
                                {
                                    rdic[d.Value] = d.Key;
                                }
                                for (int i = 0; i < index.Parts.Count(); i++)
                                {
                                    int icount = index.Parts[i].GroupValues.Count();
                                    var gv = index.Parts[i].GroupValues;
                                    index.Parts[i].IntValues = new List<int>(icount);
                                    var iv = index.Parts[i].IntValues;
                                    for (int j = 0; j < icount; j++)
                                    {
                                        iv.Add(rdic[gv[j]]);
                                    }
                                }
                                index.IndexType = IndexType.Both;
                            }
                        }
                    }
            }

            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        public void DeletePropIndex(string type_name, string prop_name)
        {
            if (!existing_indexes.ContainsKey(type_name) || !existing_indexes[type_name].Contains(prop_name))
            {
                return;
            }

            var _write_lock = GetTableWriteLock(type_name);
            lock (_write_lock)
            {

                    var alive = snapshots_alive[type_name + "|" + prop_name];
                    var new_list = new List<Tuple<bool, string>>();
                    foreach (var sn in alive)
                    {
                        IndexGeneric val = indexes[sn.Item2];
                        if (val.IndexType == IndexType.PropertyOnly)
                        {
                            existing_indexes[type_name].Remove(prop_name);
                            indexes.TryRemove(sn.Item2, out val);
                            string ls = null;
                            latest_snapshots.TryRemove(type_name + "|" + prop_name, out ls);
                        }
                        else
                        {
                            val.IndexType = IndexType.GroupOnly;
                            foreach (var p in val.Parts)
                            {
                                p.DoubleValues = null;
                                p.Ids = null;
                                p.IntValues = null;
                            }
                            new_list.Add(sn);
                        }
                    }
                    if (new_list.Any())
                    {
                        snapshots_alive[type_name + "|" + prop_name] = new_list;
                    }
                    else
                    {
                        List<Tuple<bool, string>> val = null;
                        snapshots_alive.TryRemove(type_name + "|" + prop_name, out val);
                    }

            }
        }

        public void DeleteGroupIndex(string type_name, string group_name)
        {
            var _write_lock = GetTableWriteLock(type_name);
            lock (_write_lock)
            {
                if (!snapshots_alive.ContainsKey(type_name + "|" + group_name))
                {
                    return;
                }
                var alive = snapshots_alive[type_name + "|" + group_name];
                var new_list = new List<Tuple<bool, string>>();
                foreach (var sn in alive)
                {
                    IndexGeneric val = indexes[sn.Item2];
                    if (val.IndexType == IndexType.GroupOnly)
                    {
                        existing_indexes[type_name].Remove(group_name);
                        indexes.TryRemove(sn.Item2, out val);
                        string ls = null;
                        latest_snapshots.TryRemove(type_name + "|" + group_name, out ls);
                    }
                    else
                    {
                        val.IndexType = IndexType.PropertyOnly;
                        val.GroupListMapping = null;
                        foreach (var p in val.Parts)
                        {
                            p.GroupValues = null;
                        }
                        new_list.Add(sn);
                    }
                }
                if (new_list.Any())
                {
                    snapshots_alive[type_name + "|" + group_name] = new_list;
                }
                else
                {
                    List<Tuple<bool, string>> val = null;
                    snapshots_alive.TryRemove(type_name + "|" + group_name, out val);
                }
            }
        }

        public void CreateIndex<T, TKey>(Expression<Func<T, TKey>> valuePropertySelector)
        {
            CheckTableInfo<T>();
            var value_par = valuePropertySelector.Parameters.First();
            var value_name = SharedUtils.GetPropertyName(valuePropertySelector.Body.ToString());

            CreatePropIndex(typeof(T).Name, value_name);
            AddIndexToFile(typeof(T).Name, null, value_name);

        }
        public void RemoveIndex<T, TKey>(Expression<Func<T, TKey>> valuePropertySelector)
        {
            CheckTableInfo<T>();
            var value_par = valuePropertySelector.Parameters.First();
            var value_name = SharedUtils.GetPropertyName(valuePropertySelector.Body.ToString());

            RemoveIndexFromFile(typeof(T).Name, null, value_name);
            RemoveIndex(typeof(T).Name, null, value_name);
        }

        public void RemoveGroupByIndex<T, TKey1, TKey2>(Expression<Func<T, TKey1>> groupPropertySelector, Expression<Func<T, TKey2>> valuePropertySelector)
        {
            CheckTableInfo<T>();
            var group_par = groupPropertySelector.Parameters.First();
            var group_name = SharedUtils.GetPropertyName(groupPropertySelector.Body.ToString());
            var value_par = valuePropertySelector.Parameters.First();
            var value_name = SharedUtils.GetPropertyName(valuePropertySelector.Body.ToString());

            RemoveIndexFromFile(typeof(T).Name, group_name, value_name);
            RemoveIndex(typeof(T).Name, group_name, value_name);
        }

        static object _index_file_lock = new object();
        public void AddIndexToFile(string tableName, string group, string prop)
        {
            lock (_index_file_lock)
            {
                var line_to_put = tableName + " " + (!string.IsNullOrEmpty(group) ? (group + " ") : "") + prop + "|";
                var existing = GetExistingIndexes();
                if (existing.Contains(line_to_put))
                {
                    return;
                }
                var indexes = struct_db.Get(":indexes:");
                if (string.IsNullOrEmpty(indexes))
                {
                    struct_db.Put(":indexes:", line_to_put);
                }
                else
                {
                    indexes += line_to_put;
                    struct_db.Put(":indexes:", indexes);
                }
            }
        }
        public void RemoveIndexFromFile(string tableName, string group, string prop)
        {
            lock (_index_file_lock)
            {
                var line_to_remove = tableName + " " + (!string.IsNullOrEmpty(group) ? (group + " ") : "") + prop + "|";
                var indexes = struct_db.Get(":indexes:");
                if (string.IsNullOrEmpty(indexes))
                {
                    return;
                }
                else
                {
                    indexes = indexes.Replace(line_to_remove, "");
                    struct_db.Put(":indexes:", indexes);
                }
            }
        }

        public void RemoveIndex(string tableName, string group, string prop)
        {
            var indexes = struct_db.Get(":indexes:") + "";
            var line_to_remove = tableName + " " + (!string.IsNullOrEmpty(group) ? (group + " ") : "") + prop + "|";
            indexes = indexes.Replace(line_to_remove, "");
            var lines = indexes.Split("|".ToCharArray(), StringSplitOptions.RemoveEmptyEntries).ToList();

            bool can_remove = true;
            bool can_remove_id = true;
            bool can_remove_group = true;
            foreach (var line in lines)
            {
                var parts = line.Split(" ".ToCharArray(), StringSplitOptions.RemoveEmptyEntries).ToList();
                string group_name = null;
                string prop_name = null;
                if (parts.Count() == 2)
                {
                    prop_name = parts[1];
                }
                else
                {
                    group_name = parts[1];
                    prop_name = parts[2];
                }
                if (parts[0] == tableName)
                {
                    can_remove_id = false;
                }
                if (parts[0] == tableName && prop_name == prop)
                {
                    can_remove = false;
                }
                if (parts[0] == tableName && group_name == group && group != null)
                {
                    can_remove_group = false;
                }
            }
            if (can_remove)
            {
                DeletePropIndex(tableName, prop);
            }
            if (can_remove_id)
            {
                DeletePropIndex(tableName, "Id");
            }
            if (!string.IsNullOrEmpty(group) && can_remove_group)
            {
                DeleteGroupIndex(tableName, group);
            }
        }

        public void ServerBuildIndexesOnStart()
        {
            List<string> lines = new List<string>();
            lock (_index_file_lock)
            {
                var indexes = struct_db.Get(":indexes:");
                if (!string.IsNullOrEmpty(indexes))
                {
                    lines = indexes.Split("|".ToCharArray(), StringSplitOptions.RemoveEmptyEntries).ToList();
                }
            }

            foreach (var line in lines)
            {
                var parts = line.Split(" ".ToCharArray(), StringSplitOptions.RemoveEmptyEntries).ToArray();
                if (parts.Count() == 2)
                {
                    CreatePropIndex(parts[0].Trim(), parts[1].Trim());
                }
                else
                {
                    CreateGroupByIndexString(parts[0].Trim(), parts[1].Trim(), parts[2].Trim());
                }
            }
        }

        public string GetExistingIndexes()
        {
            List<string> lines = new List<string>();
            lock (_index_file_lock)
            {
                var indexes = struct_db.Get(":indexes:") + "";
                return indexes;
            }
        }
        public void RemoveAllFileIndexes()
        {
            struct_db.Put(":indexes:", "");
        }
    }
}
