using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        //key: type name|prop name|snapshot id
        ConcurrentDictionary<string, IndexGeneric> indexes = new ConcurrentDictionary<string, IndexGeneric>();
        //key: type name, value: prop names
        ConcurrentDictionary<string, HashSet<string>> existing_indexes = new ConcurrentDictionary<string, HashSet<string>>();
        //key: type name|prop name
        ConcurrentDictionary<string, string> latest_snapshots = new ConcurrentDictionary<string, string>();
        //key: type name|prop name
        ConcurrentDictionary<string, List<Tuple<bool, string>>> snapshots_alive = new ConcurrentDictionary<string, List<Tuple<bool, string>>>();
        ConcurrentDictionary<string, DateTime> last_cleanup = new ConcurrentDictionary<string, DateTime>();
        //key: type name
        ConcurrentDictionary<string, int> compaction_count = new ConcurrentDictionary<string, int>();

        public Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> BuildMetaOnIndex(TableInfo table_info)
        {
            var res = new Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>>();
            if (existing_indexes.ContainsKey(table_info.Name))
            {
                foreach (var prop in existing_indexes[table_info.Name])
                {
                    if (!table_info.Columns.ContainsKey(prop))
                    {
                        throw new LinqDbException("Linqdb: Can't insert row with missing column on which in-memory index is built: " + prop);
                    }
                    else
                    {
                        res[prop] = new Tuple<IndexNewData, IndexDeletedData, IndexChangedData>(
                            new IndexNewData(prop, table_info.Columns[prop]),
                            new IndexDeletedData(prop, table_info.Columns[prop]),
                            new IndexChangedData(prop, table_info.Columns[prop]));
                    }
                }
                return res;
            }
            else
            {
                return res;
            }
        }

        public Dictionary<string, string> InsertIndexChanges(TableInfo table_info, Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> changes)
        {
            var res = new Dictionary<string, string>();
            foreach (var property in changes.OrderBy(f => f.Key == "Id" ? 1 : 0))
            {
                if (existing_indexes[table_info.Name].Contains(property.Key))
                {
                    var snapshot_id = Ldb.GetNewSpnapshotId();
                    res[property.Key] = snapshot_id;
                    MakeNewPropSnapshot(table_info.Name, property.Key, snapshot_id, property.Value.Item1, property.Value.Item2, property.Value.Item3);
                }
            }
            if (changes.Any())
            {
                CompactData(table_info.Name);
            }
            return res;
        }

        public void MakeNewPropSnapshot(string type, string prop, string snapshot_id, IndexNewData index_new, IndexDeletedData index_deleted, IndexChangedData index_changed)
        {
            var latest_key = type + "|" + prop;
            var latest_snapshot_id = latest_snapshots[latest_key];
            var key = type + "|" + prop + "|" + latest_snapshot_id;
            var index = indexes[key];
            var ids_index = indexes[type + "|Id|" + latest_snapshots[type + "|Id"]];
            var parts = new List<IndexPart>();
            var new_index = new IndexGeneric()
            {
                ColumnName = index.ColumnName,
                ColumnType = index.ColumnType,
                GroupListMapping = index.GroupListMapping,
                IndexType = index.IndexType,
                Parts = parts,
                TypeName = index.TypeName
            };
            bool has_changed_int = index_changed != null && index_changed.IntValues != null && index_changed.IntValues.Any();
            bool has_changed_double = index_changed != null && index_changed.DoubleValues != null && index_changed.DoubleValues.Any();
            bool has_deleted = index_deleted != null && index_deleted.Ids.Any();
            //var dbloomsize = 100000;
            //var deleted_bloom = new List<bool>(dbloomsize);
            //for (int i = 0; i < dbloomsize; i++)
            //{
            //    deleted_bloom.Add(false);
            //}
            //if (has_deleted)
            //{
            //    foreach (var did in index_deleted.Ids)
            //    {
            //        deleted_bloom[did % dbloomsize] = true;
            //    }
            //}
            //var changed_int_bloom = new List<bool>(dbloomsize);
            //for (int i = 0; i < dbloomsize; i++)
            //{
            //    changed_int_bloom.Add(false);
            //}
            //if (has_changed_int)
            //{
            //    foreach (var cid in index_changed.IntValues)
            //    {
            //        changed_int_bloom[cid.Key % dbloomsize] = true;
            //    }
            //}
            //var changed_double_bloom = new List<bool>(dbloomsize);
            //for (int i = 0; i < dbloomsize; i++)
            //{
            //    changed_double_bloom.Add(false);
            //}
            //if (has_changed_double)
            //{
            //    foreach (var cid in index_changed.DoubleValues)
            //    {
            //        changed_double_bloom[cid.Key % dbloomsize] = true;
            //    }
            //}

            switch (index.ColumnType)
            {
                case LinqDbTypes.int_:
                    if (index.IndexType == IndexType.PropertyOnly) //property index only
                    {
                        for (int i = 0; i < index.Parts.Count; i++)
                        {
                            var p = index.Parts[i];
                            p.Ids = ids_index.Parts[i].IntValues;
                            IndexPart np = null;
                            HashSet<int> removed = null;
                            int icount = p.IntValues.Count;
                            for (int j = 0; j < icount; j++)
                            {
                                var cid = p.Ids[j];
                                if (has_changed_int /*&& changed_int_bloom[cid % dbloomsize]*/ && index_changed.IntValues.ContainsKey(cid))
                                {
                                    if (np == null)
                                    {
                                        np = new IndexPart()
                                        {
                                            Ids = new List<int>(p.Ids),
                                            IntValues = new List<int>(p.IntValues)
                                        };
                                    }
                                    np.IntValues[j] = index_changed.IntValues[cid];
                                }
                                //if (j >= p.Ids.Count())
                                //{
                                //    var a = 5;
                                //}
                                if (has_deleted /*&& deleted_bloom[cid % dbloomsize]*/ && index_deleted.Ids.Contains(cid))
                                {
                                    if (removed == null)
                                    {
                                        removed = new HashSet<int>();
                                    }
                                    removed.Add((int)cid);
                                }
                            }
                            if (removed != null)
                            {
                                if (np == null)
                                {
                                    np = new IndexPart()
                                    {
                                        Ids = new List<int>(p.Ids),
                                        IntValues = new List<int>(p.IntValues)
                                    };
                                }
                                var npp = new IndexPart()
                                {
                                    Ids = new List<int>(),
                                    IntValues = new List<int>()
                                };
                                for (int z = 0; z < np.Ids.Count; z++)
                                {
                                    if (!removed.Contains((int)np.Ids[z]))
                                    {
                                        npp.Ids.Add(np.Ids[z]);
                                        npp.IntValues.Add(np.IntValues[z]);
                                    }
                                }
                                np = npp;
                            }
                            if (np != null)
                            {
                                p = np; //wow, finally!
                            }
                            parts.Add(p);
                        }
                        if (index_new != null)
                        {
                            IndexPart lastp = null;
                            if (parts.Any())
                            {
                                lastp = parts.LastOrDefault();
                            }
                            if (lastp == null)
                            {
                                lastp = new IndexPart()
                                {
                                    Ids = new List<int>(),
                                    IntValues = new List<int>()
                                };
                                parts.Add(lastp);
                            }
                            else
                            {
                                lastp = new IndexPart()
                                {
                                    Ids = new List<int>(lastp.Ids),
                                    IntValues = new List<int>(lastp.IntValues)
                                };
                                parts[parts.Count() - 1] = lastp;
                            }
                            for (int i = 0; i < index_new.IntValues.Count; i++)
                            {
                                if (lastp.Ids.Count == 1000)
                                {
                                    lastp = new IndexPart()
                                    {
                                        Ids = new List<int>(),
                                        IntValues = new List<int>()
                                    };
                                    parts.Add(lastp);
                                }
                                lastp.IntValues.Add(index_new.IntValues[i]);
                                lastp.Ids.Add(index_new.Ids[i]);
                            }
                        }
                        for (int i = 0; i < index.Parts.Count(); i++)
                        {
                            index.Parts[i].Ids = null;
                        }
                    }
                    else if (index.IndexType == IndexType.GroupOnly) //group index only
                    {
                        int map = 0;
                        if (index.GroupListMapping.Any())
                        {
                            map = index.GroupListMapping.Max(f => f.Value);
                            map++;
                        }
                        for (int i = 0; i < index.Parts.Count; i++)
                        {
                            var p = index.Parts[i];
                            p.Ids = ids_index.Parts[i].IntValues;
                            IndexPart np = null;
                            HashSet<int> removed = null;
                            int icount = p.GroupValues.Count;
                            for (int j = 0; j < icount; j++)
                            {
                                int cid = p.Ids[j];
                                if (has_changed_int /*&& changed_int_bloom[cid % dbloomsize]*/ && index_changed.IntValues.ContainsKey(cid))
                                {
                                    if (np == null)
                                    {
                                        np = new IndexPart()
                                        {
                                            Ids = new List<int>(p.Ids),
                                            GroupValues = new List<int>(p.GroupValues)
                                        };
                                    }
                                    if (!index.GroupListMapping.ContainsKey(index_changed.IntValues[cid]))
                                    {
                                        index.GroupListMapping[index_changed.IntValues[cid]] = map;
                                        map++;
                                    }
                                    np.GroupValues[j] = index.GroupListMapping[index_changed.IntValues[cid]];
                                }
                                if (has_deleted /*&& deleted_bloom[cid % dbloomsize]*/ && index_deleted.Ids.Contains(cid))
                                {
                                    if (removed == null)
                                    {
                                        removed = new HashSet<int>();
                                    }
                                    removed.Add(cid);
                                }
                            }
                            if (removed != null)
                            {
                                if (np == null)
                                {
                                    np = new IndexPart()
                                    {
                                        Ids = new List<int>(p.Ids),
                                        GroupValues = new List<int>(p.GroupValues)
                                    };
                                }
                                var npp = new IndexPart()
                                {
                                    Ids = new List<int>(),
                                    GroupValues = new List<int>()
                                };
                                for (int z = 0; z < np.Ids.Count; z++)
                                {
                                    if (!removed.Contains((int)np.Ids[z]))
                                    {
                                        npp.Ids.Add(np.Ids[z]);
                                        npp.GroupValues.Add(np.GroupValues[z]);
                                    }
                                }
                                np = npp;
                            }
                            if (np != null)
                            {
                                p = np; //wow, finally!
                            }
                            parts.Add(p);
                        }
                        if (index_new != null)
                        {
                            IndexPart lastp = null;
                            if (parts.Any())
                            {
                                lastp = parts.LastOrDefault();
                            }
                            if (lastp == null)
                            {
                                lastp = new IndexPart()
                                {
                                    Ids = new List<int>(),
                                    GroupValues = new List<int>()
                                };
                                parts.Add(lastp);
                            }
                            else
                            {
                                lastp = new IndexPart()
                                {
                                    Ids = new List<int>(lastp.Ids),
                                    GroupValues = new List<int>(lastp.GroupValues)
                                };
                                parts[parts.Count() - 1] = lastp;
                            }
                            for (int i = 0; i < index_new.IntValues.Count; i++)
                            {
                                if (lastp.Ids.Count == 1000)
                                {
                                    lastp = new IndexPart()
                                    {
                                        Ids = new List<int>(),
                                        GroupValues = new List<int>()
                                    };
                                    parts.Add(lastp);
                                }
                                if (!index.GroupListMapping.ContainsKey((int)index_new.IntValues[i]))
                                {
                                    index.GroupListMapping[(int)index_new.IntValues[i]] = map;
                                    map++;
                                }
                                lastp.GroupValues.Add(index.GroupListMapping[(int)index_new.IntValues[i]]);
                                lastp.Ids.Add(index_new.Ids[i]);
                            }
                        }
                        for (int i = 0; i < index.Parts.Count(); i++)
                        {
                            index.Parts[i].Ids = null;
                        }
                    }
                    else //both
                    {
                        int map = 0;
                        if (index.GroupListMapping.Any())
                        {
                            map = index.GroupListMapping.Max(f => f.Value);
                            map++;
                        }
                        for (int i = 0; i < index.Parts.Count; i++)
                        {
                            var p = index.Parts[i];
                            p.Ids = ids_index.Parts[i].IntValues;
                            IndexPart np = null;
                            HashSet<int> removed = null;
                            int icount = p.GroupValues.Count;
                            for (int j = 0; j < icount; j++)
                            {
                                int cid = p.Ids[j];
                                if (has_changed_int /*&& changed_int_bloom[cid % dbloomsize]*/ && index_changed.IntValues.ContainsKey(cid))
                                {
                                    if (np == null)
                                    {
                                        np = new IndexPart()
                                        {
                                            Ids = new List<int>(p.Ids),
                                            GroupValues = new List<int>(p.GroupValues),
                                            IntValues = new List<int>(p.IntValues)
                                        };
                                    }
                                    if (!index.GroupListMapping.ContainsKey(index_changed.IntValues[cid]))
                                    {
                                        index.GroupListMapping[index_changed.IntValues[cid]] = map;
                                        map++;
                                    }
                                    np.GroupValues[j] = index.GroupListMapping[index_changed.IntValues[cid]];
                                    np.IntValues[j] = index_changed.IntValues[cid];
                                }
                                if (has_deleted /*&& deleted_bloom[cid % dbloomsize]*/ && index_deleted.Ids.Contains(cid))
                                {
                                    if (removed == null)
                                    {
                                        removed = new HashSet<int>();
                                    }
                                    removed.Add(cid);
                                }
                            }
                            if (removed != null)
                            {
                                if (np == null)
                                {
                                    np = new IndexPart()
                                    {
                                        Ids = new List<int>(p.Ids),
                                        GroupValues = new List<int>(p.GroupValues),
                                        IntValues = new List<int>(p.IntValues)
                                    };
                                }
                                var npp = new IndexPart()
                                {
                                    Ids = new List<int>(),
                                    GroupValues = new List<int>(),
                                    IntValues = new List<int>()
                                };
                                for (int z = 0; z < np.Ids.Count; z++)
                                {
                                    if (!removed.Contains((int)np.Ids[z]))
                                    {
                                        npp.Ids.Add(np.Ids[z]);
                                        npp.GroupValues.Add(np.GroupValues[z]);
                                        npp.IntValues.Add(np.IntValues[z]);
                                    }
                                }
                                np = npp;
                            }
                            if (np != null)
                            {
                                p = np; //wow, finally!
                            }
                            parts.Add(p);
                        }
                        if (index_new != null)
                        {
                            IndexPart lastp = null;
                            if (parts.Any())
                            {
                                lastp = parts.LastOrDefault();
                            }
                            if (lastp == null)
                            {
                                lastp = new IndexPart()
                                {
                                    Ids = new List<int>(),
                                    GroupValues = new List<int>(),
                                    IntValues = new List<int>()
                                };
                                parts.Add(lastp);
                            }
                            else
                            {
                                lastp = new IndexPart()
                                {
                                    Ids = new List<int>(lastp.Ids),
                                    GroupValues = new List<int>(lastp.GroupValues),
                                    IntValues = new List<int>(lastp.IntValues)
                                };
                                parts[parts.Count() - 1] = lastp;
                            }
                            for (int i = 0; i < index_new.IntValues.Count; i++)
                            {
                                if (lastp.Ids.Count == 1000)
                                {
                                    lastp = new IndexPart()
                                    {
                                        Ids = new List<int>(),
                                        GroupValues = new List<int>(),
                                        IntValues = new List<int>()
                                    };
                                    parts.Add(lastp);
                                }
                                if (!index.GroupListMapping.ContainsKey((int)index_new.IntValues[i]))
                                {
                                    index.GroupListMapping[(int)index_new.IntValues[i]] = map;
                                    map++;
                                }
                                lastp.GroupValues.Add(index.GroupListMapping[(int)index_new.IntValues[i]]);
                                lastp.IntValues.Add(index_new.IntValues[i]);
                                lastp.Ids.Add(index_new.Ids[i]);
                            }
                        }
                        for (int i = 0; i < index.Parts.Count(); i++)
                        {
                            index.Parts[i].Ids = null;
                        }
                    }
                    break;
                case LinqDbTypes.double_:
                case LinqDbTypes.DateTime_:
                    for (int i = 0; i < index.Parts.Count; i++)
                    {
                        var p = index.Parts[i];
                        p.Ids = ids_index.Parts[i].IntValues;
                        IndexPart np = null;
                        HashSet<int> removed = null;
                        int icount = p.DoubleValues.Count;
                        for (int j = 0; j < icount; j++)
                        {
                            int cid = p.Ids[j];
                            if (has_changed_double /*&& changed_double_bloom[cid % dbloomsize]*/ && index_changed.DoubleValues.ContainsKey(cid))
                            {
                                if (np == null)
                                {
                                    np = new IndexPart()
                                    {
                                        Ids = new List<int>(p.Ids),
                                        DoubleValues = new List<double>(p.DoubleValues)
                                    };
                                }
                                np.DoubleValues[j] = index_changed.DoubleValues[cid];
                            }
                            if (has_deleted /*&& deleted_bloom[cid % dbloomsize]*/ && index_deleted.Ids.Contains(cid))
                            {
                                if (removed == null)
                                {
                                    removed = new HashSet<int>();
                                }
                                removed.Add(cid);
                            }
                        }
                        if (removed != null)
                        {
                            if (np == null)
                            {
                                np = new IndexPart()
                                {
                                    Ids = new List<int>(p.Ids),
                                    DoubleValues = new List<double>(p.DoubleValues)
                                };
                            }
                            var npp = new IndexPart()
                            {
                                Ids = new List<int>(),
                                DoubleValues = new List<double>()
                            };
                            for (int z = 0; z < np.Ids.Count; z++)
                            {
                                if (!removed.Contains((int)np.Ids[z]))
                                {
                                    npp.Ids.Add(np.Ids[z]);
                                    npp.DoubleValues.Add(np.DoubleValues[z]);
                                }
                            }
                            np = npp;
                        }
                        if (np != null)
                        {
                            p = np; //wow, finally!
                        }
                        parts.Add(p);
                    }
                    if (index_new != null)
                    {
                        IndexPart lastp = null;
                        if (parts.Any())
                        {
                            lastp = parts.LastOrDefault();
                        }
                        if (lastp == null)
                        {
                            lastp = new IndexPart()
                            {
                                Ids = new List<int>(),
                                DoubleValues = new List<double>()
                            };
                            parts.Add(lastp);
                        }
                        else
                        {
                            lastp = new IndexPart()
                            {
                                Ids = new List<int>(lastp.Ids),
                                DoubleValues = new List<double>(lastp.DoubleValues)
                            };
                            parts[parts.Count() - 1] = lastp;
                        }
                        for (int i = 0; i < index_new.DoubleValues.Count; i++)
                        {
                            if (lastp.Ids.Count == 1000)
                            {
                                lastp = new IndexPart()
                                {
                                    Ids = new List<int>(),
                                    DoubleValues = new List<double>()
                                };
                                parts.Add(lastp);
                            }
                            lastp.DoubleValues.Add(index_new.DoubleValues[i]);
                            lastp.Ids.Add(index_new.Ids[i]);
                        }
                    }
                    for (int i = 0; i < index.Parts.Count(); i++)
                    {
                        index.Parts[i].Ids = null;
                    }
                    break;                
                default:
                    break;
            }

            //free old snapshots
            if ((DateTime.Now - last_cleanup[type + "|" + prop]).TotalMilliseconds > 30000)
            {
                var alive = snapshots_alive[type + "|" + prop];
                var new_alive = new List<Tuple<bool, string>>(); //bool - schedueled for deletion
                foreach (var sn in alive)
                {
                    if (!sn.Item1)
                    {
                        var nsn = new Tuple<bool, string>(true, sn.Item2);
                        new_alive.Add(nsn);

                    }
                    else
                    {
                        IndexGeneric val = null;
                        indexes.TryRemove(sn.Item2, out val);
                    }
                }
                new_alive.Add(new Tuple<bool, string>(false, type + "|" + prop + "|" + snapshot_id));
                snapshots_alive[type + "|" + prop] = new_alive;
                last_cleanup[type + "|" + prop] = DateTime.Now;
            }
            else
            {
                snapshots_alive[type + "|" + prop].Add(new Tuple<bool, string>(false, type + "|" + prop + "|" + snapshot_id));
            }

            key = type + "|" + prop + "|" + snapshot_id;
            indexes[key] = new_index;
            latest_snapshots[latest_key] = snapshot_id;
        }

        void CompactData(string type)
        {
            if (!compaction_count.ContainsKey(type))
            {
                compaction_count[type] = 0;
            }
            if (compaction_count[type] > 50)
            {
                var props = existing_indexes[type];
                foreach (var prop in props)
                {
                    var snap_id = latest_snapshots[type + "|" + prop];
                    var key = type + "|" + prop + "|" + snap_id;
                    var index = indexes[key];
                    switch (index.ColumnType)
                    {
                        case LinqDbTypes.int_:
                            if (index.IndexType == IndexType.PropertyOnly || index.IndexType == IndexType.Both)
                            {
                                var compacted_parts = new List<IndexPart>();
                                foreach (var p in index.Parts)
                                {
                                    if (p.IntValues.Any())
                                    {
                                        compacted_parts.Add(p);
                                    }
                                }
                                index.Parts = compacted_parts;
                            }
                            else if(index.IndexType == IndexType.GroupOnly)
                            {
                                var compacted_parts = new List<IndexPart>();
                                foreach (var p in index.Parts)
                                {
                                    if (p.GroupValues.Any())
                                    {
                                        compacted_parts.Add(p);
                                    }
                                }
                                index.Parts = compacted_parts;
                            }
                            break;
                        case LinqDbTypes.double_:
                        case LinqDbTypes.DateTime_:
                            var d_compacted_parts = new List<IndexPart>();
                            foreach (var p in index.Parts)
                            {
                                if (p.DoubleValues.Any())
                                {
                                    d_compacted_parts.Add(p);
                                }
                            }
                            index.Parts = d_compacted_parts;
                            break;
                        default:
                            break;
                    }
                }
                compaction_count[type] = 0;
            }
            else
            {
                compaction_count[type]++;
            }
        }
    }

    public class IndexGeneric
    {
        public string TypeName { get; set; }
        public string ColumnName { get; set; }
        public LinqDbTypes ColumnType { get; set; }
        public List<IndexPart> Parts { get; set; }
        public ConcurrentDictionary<int, int> GroupListMapping { get; set; }
        public IndexType IndexType { get; set; }
    }

    public enum IndexType : int
    {
        GroupOnly = 0,
        PropertyOnly = 1,
        Both = 2
    }
    public class IndexPart
    {
        public List<int> Ids { get; set; }
        public List<int> IntValues { get; set; }
        public List<double> DoubleValues { get; set; }
        public List<int> GroupValues { get; set; }
    }

    public class IndexDeletedData
    {
        public IndexDeletedData(string ColumnName, LinqDbTypes Type)
        {
            Ids = new HashSet<int>();
            this.ColumnName = ColumnName;
            this.Type = Type;
        }
        public string ColumnName { get; set; }
        public LinqDbTypes Type { get; set; }
        public HashSet<int> Ids { get; set; }
    }
    public class IndexNewData
    {
        public IndexNewData(string ColumnName, LinqDbTypes Type)
        {
            Ids = new List<int>();
            IntValues = new List<int>();
            DoubleValues = new List<double>();
            this.ColumnName = ColumnName;
            this.Type = Type;
        }
        public string ColumnName { get; set; }
        public LinqDbTypes Type { get; set; }
        public List<int> Ids { get; set; }
        public List<int> IntValues { get; set; }
        public List<double> DoubleValues { get; set; }
    }
    public class IndexChangedData
    {
        public IndexChangedData(string ColumnName, LinqDbTypes Type)
        {
            IntValues = new Dictionary<int, int>();
            DoubleValues = new Dictionary<int, double>();
            this.ColumnName = ColumnName;
            this.Type = Type;
        }
        public string ColumnName { get; set; }
        public LinqDbTypes Type { get; set; }
        public Dictionary<int, int> IntValues { get; set; }
        public Dictionary<int, double> DoubleValues { get; set; }
    }
}
