using RocksDbSharp;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public int SaveIncrement<T>(T item, LinqdbTransactionInternal trans)
        {
            SaveBatchIncrement<T>(new List<T>() { item }, trans);
            return Convert.ToInt32(item.GetType().GetProperty("Id").GetValue(item));
        }
        public void SaveBatchIncrement<T>(List<T> items, LinqdbTransactionInternal trans)
        {
            if (!items.Any())
            {
                return;
            }
            List<T> current_values = new List<T>();
            foreach (var v in items)
            {
                current_values.Add(v);
            }
            if (current_values.Any())
            {
                SaveItemsBatch<T>(current_values, trans);
            }
        }
        public int Save<T>(T item, LinqdbTransactionInternal trans)
        {
            SaveBatch<T>(new List<T>() { item }, trans);
            return Convert.ToInt32(item.GetType().GetProperty("Id").GetValue(item));
        }
        Tuple<HashSet<int>, List<object>> GetIds<T>(List<T> items, HashSet<int> exclude, List<object> old_items)
        {
            if (exclude == null)
            {
                var res = new HashSet<int>();
                foreach (var item in items)
                {
                    var prop = item.GetType().GetProperty("Id");
                    if (prop == null)
                    {
                        throw new LinqDbException("Linqdb: type must have integer Id property");
                    }
                    res.Add(Convert.ToInt32(prop.GetValue(item)));
                }
                return new Tuple<HashSet<int>, List<object>>(res, null);
            }
            else
            {
                var res = new HashSet<int>();
                foreach (var item in items)
                {
                    var prop = item.GetType().GetProperty("Id");
                    if (prop == null)
                    {
                        throw new LinqDbException("Linqdb: type must have integer Id property");
                    }
                    var id = Convert.ToInt32(prop.GetValue(item));
                    if (id == 0 || !exclude.Contains(id))
                    {
                        res.Add(id);
                        old_items.Add(item);
                    }
                }
                exclude.UnionWith(res);
                return new Tuple<HashSet<int>, List<object>>(exclude, old_items);
            }
        }
        public void SaveBatch<T>(List<T> items, LinqdbTransactionInternal trans)
        {
            if (trans != null)
            {
                SaveBatchTransaction(items, trans);
                return;
            }

            var table_info = CheckTableInfo<T>();

            bool done = false;
            string error = null;
            var ilock = ModifyBatch.GetTableSaveBatchLock(table_info.Name);
            lock (ilock)
            {
                if (!ModifyBatch._save_batch.ContainsKey(table_info.Name))
                {
                    var ids = GetIds(items, null, null).Item1;
                    ModifyBatch._save_batch[table_info.Name] = new SaveData() { Callbacks = new List<Action<string>>(), Ids = ids, Items = items.Cast<object>().ToList() };
                }
                else
                {
                    var ids = GetIds(items, ModifyBatch._save_batch[table_info.Name].Ids, ModifyBatch._save_batch[table_info.Name].Items);
                    ModifyBatch._save_batch[table_info.Name].Ids = ids.Item1;
                    ModifyBatch._save_batch[table_info.Name].Items = ids.Item2;
                }
                ModifyBatch._save_batch[table_info.Name].Callbacks.Add(f =>
                {
                    done = true;
                    error = f;
                });
            }

            var _write_lock = GetTableWriteLock(typeof(T).Name);

            bool lockAcquired = false;
            int maxWaitMs = 60000;
            SaveData _save_data = null;
            try
            {
                DateTime start = DateTime.Now;
                while (!done)
                {
                    lockAcquired = Monitor.TryEnter(_write_lock, 0);
                    if (lockAcquired)
                    {
                        if (done)
                        {
                            Monitor.Exit(_write_lock);
                            lockAcquired = false;
                            break;
                        }
                        else
                        {
                            break;
                        }
                    }
                    Thread.Sleep(250);
                    //if ((DateTime.Now - start).TotalMilliseconds > maxWaitMs)
                    //{
                    //    throw new LinqDbException("Linqdb: Save waited too long to acquire write lock. Is the load too high?");
                    //}
                }
                if (done)
                {
                    if (!string.IsNullOrEmpty(error))
                    {
                        throw new LinqDbException(error);
                    }
                    else
                    {
                        return;
                    }
                }

                //not done, but have write lock for the table
                lock (ilock)
                {
                    _save_data = ModifyBatch._save_batch[table_info.Name];
                    var oval = new SaveData();
                    ModifyBatch._save_batch.TryRemove(table_info.Name, out oval);
                }
                if (_save_data.Items.Any())
                {
                    List<T> current_values = new List<T>();
                    foreach (var v in _save_data.Items)
                    {
                        current_values.Add((T)v);
                    }
                    if (current_values.Any())
                    {
                        SaveItemsBatch<T>(current_values, trans);
                    }
                }
                foreach (var cb in _save_data.Callbacks)
                {
                    cb(null);
                }
            }
            catch (Exception ex)
            {
                if (_save_data != null)
                {
                    var additionalInfo = ex.Message;
                    if (_save_data.Callbacks.Count() > 1)
                    {
                        additionalInfo += " This error could belong to another entity which happened to be in the same batch.";
                    }
                    foreach (var cb in _save_data.Callbacks)
                    {
                        cb(additionalInfo);
                    }
                }
                throw;
            }
            finally
            {
                if (lockAcquired)
                {
                    Monitor.Exit(_write_lock);
                }
            }
        }
        public void SaveBatchTransaction<T>(List<T> items, LinqdbTransactionInternal trans)
        {
            if (!items.Any())
            {
                return;
            }
            List<T> current_values = new List<T>();
            foreach (var v in items)
            {
                current_values.Add(v);
            }
            if (current_values.Any())
            {
                SaveItemsBatch<T>(current_values, trans);
            }
        }
        public void SaveItemsBatch<T>(List<T> items, LinqdbTransactionInternal trans)
        {
            if (trans == null)
            {
                var table_info = CheckTableInfo<T>();
                using (WriteBatchWithConstraints batch = new WriteBatchWithConstraints())
                {
                    Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> meta_index = BuildMetaOnIndex(table_info);
                    Dictionary<string, KeyValuePair<byte[], HashSet<int>>> string_cache = new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>();
                    var lastPhase = GetLastStep(table_info);
                    SaveItems(table_info, typeof(T).Name, items.Cast<object>().ToList(), batch, null, string_cache, meta_index, false);
                    WriteStringCacheToBatch(batch, string_cache, table_info, lastPhase);
                    var snapshots_dic = InsertIndexChanges(table_info, meta_index);
                    foreach (var snap in snapshots_dic)
                    {
                        var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[snap.Key]);
                        batch.Put(skey, Encoding.UTF8.GetBytes(snap.Value));
                    }
                    leveld_db.Write(batch._writeBatch);
                }
            }
            else
            {
                var type_name = typeof(T).Name;
                var table_info = CheckTableInfo<T>();
                if (!trans.data_to_save.ContainsKey(type_name))
                {
                    trans.data_to_save[type_name] = new KeyValuePair<TableInfo, List<object>>(table_info, new List<object>());
                }
                foreach (var item in items)
                {
                    var id = Convert.ToInt32(item.GetType().GetProperty("Id").GetValue(item));
                    if (id < 0)
                    {
                        throw new LinqDbException("Linqdb: Id must not be negative");
                    }
                    if (id == 0)
                    {
                        id = GetNextId(type_name, 0);
                        item.GetType().GetProperty("Id").SetValue(item, id);
                    }
                    trans.data_to_save[type_name].Value.Add(item);
                }
            }
        }

        public void SaveItems(TableInfo table_info, string type_name, List<object> items, WriteBatchWithConstraints batch, Dictionary<string, int> trans_count_cache, Dictionary<string, KeyValuePair<byte[], HashSet<int>>> string_cache, Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> memory_index_meta, bool is_trans)
        {
            //var string_cache = new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>();
            var ids = new HashSet<int>();
            foreach (var item in items)
            {
                if (item.GetType().GetProperty("Id") == null)
                {
                    throw new LinqDbException("Linqdb: type must have integer Id property");
                }
                bool is_new = false;
                var id = Convert.ToInt32(item.GetType().GetProperty("Id").GetValue(item));
                if (id < 0)
                {
                    throw new LinqDbException("Linqdb: Id must not be negative");
                }
                if (id == 0)
                {
                    id = GetNextId(type_name, 0);
                    item.GetType().GetProperty("Id").SetValue(item, id);
                    is_new = true;
                }
                else
                {
                    GetNextId(type_name, id);
                }
                if (ids.Contains(id))
                {
                    continue;
                }
                else
                {
                    ids.Add(id);
                }
                if (!is_new) //maybe new if id is non 0 (but not existing)
                {
                    var key_info = new IndexKeyInfo()
                    {
                        ColumnNumber = table_info.ColumnNumbers["Id"],
                        TableNumber = table_info.TableNumber,
                        ColumnType = table_info.Columns["Id"],
                        Id = id
                    };
                    var value_key = MakeValueKey(key_info);
                    var v = leveld_db.Get(value_key);
                    if (v == null)
                    {
                        is_new = true;
                        if (id > Int32.MaxValue / 2)
                        {
                            if (!is_trans)
                            {
                                throw new LinqDbException("Linqdb: max Id value of new item is " + (Int32.MaxValue / 2));
                            }
                            else if (GetMaxId(table_info.Name) < id)
                            {
                                throw new LinqDbException("Linqdb: max Id value of new item is " + (Int32.MaxValue / 2));
                            }
                        }
                    }
                }
                foreach (var p in table_info.Columns)
                {
                    var info = item.GetType().GetProperty(p.Key);
                    var value = info.GetValue(item);
                    if (p.Value == LinqDbTypes.string_)
                    {
                        SaveStringData(batch, (string)value, p.Key, table_info, id, string_cache, is_new);
                    }
                    else if (p.Value == LinqDbTypes.binary_)
                    {
                        SaveBinaryColumn(batch, value, p.Key, table_info, id, is_new);
                    }
                    else
                    {
                        IndexDeletedData index_deleted = null;
                        IndexNewData index_new = null;
                        IndexChangedData index_changed = null;
                        if (memory_index_meta.ContainsKey(p.Key))
                        {
                            index_deleted = memory_index_meta[p.Key].Item2;
                            index_new = memory_index_meta[p.Key].Item1;
                            index_changed = memory_index_meta[p.Key].Item3;
                        }
                        SaveDataColumn(batch, value, p.Key, p.Value, table_info, id, is_new, index_new, index_changed);
                    }
                }
            }
            IncrementCount(table_info, ids, batch, trans_count_cache);
            //WriteStringCacheToBatch(batch, string_cache, table_info, trans_phase_cache);
        }

        //class ToBeSavedValue
        //{
        //    public bool IsNull { get; set; }
        //    public LinqDbTypes Type { get; set; }
        //    public double? DoubleVal { get; set; }
        //    public DateTime? DateVal { get; set; }
        //    public int? IntVal { get; set; }
        //}

        public void WriteStringCacheToBatch(WriteBatchWithConstraints batch, Dictionary<string, KeyValuePair<byte[], HashSet<int>>> cache, TableInfo tinfo, int? lastPhase)
        {
            var modified = new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>();
            //int last_phase = GetLastStringPhase(tinfo, trans_phase_cache);
            //int new_last_phase = last_phase;
            using (SHA1CryptoServiceProvider sha1 = new SHA1CryptoServiceProvider())
            {
                foreach (var item in cache)
                {
                    var parts = item.Key.Split(":".ToArray(), StringSplitOptions.None);
                    int phase = Convert.ToInt32(parts[0]);
                    bool isNotThere = lastPhase != null && lastPhase + 1 < phase;
                    string hash = Convert.ToBase64String(sha1.ComputeHash(item.Value.Key));
                    if (parts[1] == "1") //add
                    {
                        byte[] val = null;
                        if (!modified.ContainsKey(hash))
                        {
                            if (isNotThere)
                            {
                                //batch.Put(item.Value.Key, WriteHashsetToBytes(item.Value.Value));
                                modified[hash] = new KeyValuePair<byte[], HashSet<int>>(item.Value.Key, item.Value.Value);
                            }
                            else
                            {
                                val = leveld_db.Get(item.Value.Key);
                                if (val != null)
                                {
                                    var old_index = ReadHashsetFromBytes(val);
                                    old_index.UnionWith(item.Value.Value);
                                    //batch.Put(item.Value.Key, WriteHashsetToBytes(old_index));
                                    modified[hash] = new KeyValuePair<byte[], HashSet<int>>(item.Value.Key, old_index);
                                }
                                else
                                {
                                    //batch.Put(item.Value.Key, WriteHashsetToBytes(item.Value.Value));
                                    modified[hash] = new KeyValuePair<byte[], HashSet<int>>(item.Value.Key, item.Value.Value);
                                }
                            }
                        }
                        else
                        {
                            modified[hash].Value.UnionWith(item.Value.Value);
                            //batch.Put(item.Value.Key, WriteHashsetToBytes(modified[hash]));
                        }
                    }
                    else //remove
                    {
                        if (!modified.ContainsKey(hash))
                        {
                            var val = leveld_db.Get(item.Value.Key);
                            if (val != null)
                            {
                                var old_index = ReadHashsetFromBytes(val);
                                old_index.ExceptWith(item.Value.Value);
                                //batch.Put(item.Value.Key, WriteHashsetToBytes(old_index));
                                modified[hash] = new KeyValuePair<byte[], HashSet<int>>(item.Value.Key, old_index);
                            }
                        }
                        else
                        {
                            modified[hash].Value.ExceptWith(item.Value.Value);
                            //batch.Put(item.Value.Key, WriteHashsetToBytes(modified[hash]));
                        }
                    }
                }
                foreach (var val in modified)
                {
                    var hs = WriteHashsetToBytes(val.Value.Value);
                    if (hs[0] == 0 && hs[1] == 0 && hs[2] == 0 && hs[3] == 0)
                    {
                        batch.Delete(val.Value.Key);
                    }
                    else
                    {
                        batch.Put(val.Value.Key, hs);
                    }
                }
            }
        }
        void SaveDataColumn(WriteBatchWithConstraints batch, object value, string name, LinqDbTypes type, TableInfo table_info, int id, bool is_new, IndexNewData index_new, IndexChangedData index_changed)
        {
            var key_info = new IndexKeyInfo()
            {
                ColumnNumber = table_info.ColumnNumbers[name],
                TableNumber = table_info.TableNumber,
                ColumnType = table_info.Columns[name],
                Id = id
            };
            var value_key = MakeValueKey(key_info);
            byte[] old_val = null;
            if (!is_new)
            {
                old_val = leveld_db.Get(value_key);
            }

            byte[] vres = null;
            byte[] res = null;
            short negative = 1;
            if (value != null)
            {
                switch (type)
                {
                    case LinqDbTypes.DateTime_:
                        res = BitConverter.GetBytes((Convert.ToDateTime(value) - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds).MyReverseNoCopy();
                        vres = res;
                        break;
                    case LinqDbTypes.double_:
                        var dv = Convert.ToDouble(value, CultureInfo.InvariantCulture);
                        vres = BitConverter.GetBytes(dv).MyReverseNoCopy();
                        if (dv < 0)
                        {
                            negative = -1;
                            dv *= -1;
                        }
                        res = BitConverter.GetBytes(dv).MyReverseNoCopy();
                        break;
                    case LinqDbTypes.int_:
                        var iv = Convert.ToInt32(value);
                        vres = BitConverter.GetBytes(iv).MyReverseNoCopy();
                        if (iv < 0)
                        {
                            negative = -1;
                            iv *= -1;
                        }
                        res = BitConverter.GetBytes(iv).MyReverseNoCopy();
                        break;
                    default:
                        break;
                }

            }
            else
            {
                res = NullConstant;
                vres = NullConstant;
            }

            if (old_val != null && ValsEqual(old_val, vres))
            {
                return;
            }

            //keep up the memory index, if there is one
            if (is_new && index_new != null)
            {
                if (value != null)
                {
                    switch (type)
                    {
                        case LinqDbTypes.DateTime_:
                            var datetime = Convert.ToDateTime(value);
                            var date_val = (Convert.ToDateTime(value) - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
                            index_new.DoubleValues.Add(date_val);
                            index_new.Ids.Add(id);
                            break;
                        case LinqDbTypes.double_:
                            var dv = Convert.ToDouble(value, CultureInfo.InvariantCulture);
                            index_new.DoubleValues.Add(dv);
                            index_new.Ids.Add(id);
                            break;
                        case LinqDbTypes.int_:
                            var iv = Convert.ToInt32(value);
                            index_new.IntValues.Add(iv);
                            index_new.Ids.Add(id);
                            break;
                        default:
                            break;
                    }
                }
                else
                {
                    //switch (type)
                    //{
                    //    case LinqDbTypes.DateTime_:
                    //        index_new.DoubleValues.Add(null);
                    //        index_new.Ids.Add(id);
                    //        break;
                    //    case LinqDbTypes.double_:
                    //        index_new.DoubleValues.Add(null);
                    //        index_new.Ids.Add(id);
                    //        break;
                    //    case LinqDbTypes.int_:
                    //        index_new.IntValues.Add(0);
                    //        index_new.Ids.Add(id);
                    //        break;
                    //    default:
                    //        break;
                    //}
                    throw new LinqDbException("Linqdb: in-memory indexes do not support null values. Error while saving column: " + index_new.ColumnName);
                }
            }
            if (!is_new && index_changed != null)
            {
                if (value != null)
                {
                    switch (type)
                    {
                        case LinqDbTypes.DateTime_:
                            var datetime = Convert.ToDateTime(value);
                            var date_val = (Convert.ToDateTime(value) - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
                            index_changed.DoubleValues[id] = date_val;
                            break;
                        case LinqDbTypes.double_:
                            var dv = Convert.ToDouble(value, CultureInfo.InvariantCulture);
                            index_changed.DoubleValues[id] = dv;
                            break;
                        case LinqDbTypes.int_:
                            var iv = Convert.ToInt32(value);
                            index_changed.IntValues[id] = iv;
                            break;
                        default:
                            break;
                    }
                }
                else
                {
                    //switch (type)
                    //{
                    //    case LinqDbTypes.DateTime_:
                    //        index_changed.DoubleValues[id] = null;
                    //        break;
                    //    case LinqDbTypes.double_:
                    //        index_changed.DoubleValues[id] = null;
                    //        break;
                    //    case LinqDbTypes.int_:
                    //        index_changed.IntValues[id] = 0;
                    //        break;
                    //    default:
                    //        break;
                    //}
                    throw new LinqDbException("Linqdb: in-memory indexes do not support null values. Error while saving column: " + index_changed.ColumnName);
                }
            }
            //delete old index
            if (old_val != null)
            {
                bool is_old_negative = false;
                if (ValsEqual(old_val, NullConstant))
                {
                    is_old_negative = false;
                }
                else if (table_info.Columns[name] == LinqDbTypes.double_ && BitConverter.ToDouble(old_val.MyReverseWithCopy(), 0) < 0)
                {
                    is_old_negative = true;
                    old_val = BitConverter.GetBytes((BitConverter.ToDouble(old_val.MyReverseWithCopy(), 0) * -1)).MyReverseNoCopy();
                }
                else if (table_info.Columns[name] == LinqDbTypes.int_ && BitConverter.ToInt32(old_val.MyReverseWithCopy(), 0) < 0)
                {
                    is_old_negative = true;
                    old_val = BitConverter.GetBytes((BitConverter.ToInt32(old_val.MyReverseWithCopy(), 0) * -1)).MyReverseNoCopy();
                }

                byte[] index_key = null;
                if (is_old_negative)
                {
                    index_key = MakeIndexKey(new IndexKeyInfo()
                    {
                        TableNumber = table_info.TableNumber,
                        ColumnNumber = (short)(-1 * table_info.ColumnNumbers[name]),
                        ColumnType = table_info.Columns[name],
                        Val = old_val,
                        Id = id
                    });
                }
                else
                {
                    index_key = MakeIndexKey(new IndexKeyInfo()
                    {
                        TableNumber = table_info.TableNumber,
                        ColumnNumber = table_info.ColumnNumbers[name],
                        ColumnType = table_info.Columns[name],
                        Val = old_val,
                        Id = id
                    });
                }

                batch.Delete(index_key);
            }

            key_info = new IndexKeyInfo()
            {
                TableNumber = table_info.TableNumber,
                ColumnNumber = (short)(negative * table_info.ColumnNumbers[name]),
                Val = res,
                Id = id,
                ColumnType = table_info.Columns[name]
            };
            var vkey_info = new IndexKeyInfo()
            {
                TableNumber = table_info.TableNumber,
                ColumnNumber = table_info.ColumnNumbers[name],
                Val = vres,
                Id = id,
                ColumnType = table_info.Columns[name]
            };
            var new_index_key = MakeIndexKey(key_info);
            var new_value_key = MakeValueKey(vkey_info);

            batch.Put(new_index_key, Filler);
            batch.Put(new_value_key, vres);
        }

        void SaveStringData(WriteBatchWithConstraints batch, string value, string name, TableInfo table_info, int id, Dictionary<string, KeyValuePair<byte[], HashSet<int>>> cache, bool is_new)
        {
            if (value == "")
            {
                value = null;
            }
            if (value != null)
            {
                if (value.Length > 1048576)
                {
                    throw new LinqDbException("Linqdb: string value max size is 1Mb.");
                }
            }
            byte[] old = null;
            if (!is_new)
            {
                var key = MakeStringValueKey(new IndexKeyInfo()
                {
                    ColumnNumber = table_info.ColumnNumbers[name],
                    TableNumber = table_info.TableNumber,
                    Id = id
                });
                old = leveld_db.Get(key);
            }
            string old_val = null;
            if (old != null && old.Length != 1)
            {
                old_val = Encoding.Unicode.GetString(old.Skip(1).ToArray());
            }
            if (old_val != null && old_val == value)
            {
                return;
            }
            if (name.ToLower().EndsWith("search"))
            {
                UpdateIndex(old_val, value, id, batch, table_info.ColumnNumbers[name], table_info.TableNumber, cache, false);
            }
            if (name.ToLower().EndsWith("searchs"))
            {
                UpdateIndex(old_val, value, id, batch, table_info.ColumnNumbers[name], table_info.TableNumber, cache, true);
            }
            Updateue(old_val, value, id, batch, table_info, name);

            //index
            //first delete old value
            //if (old_val?.ToLower(CultureInfo.InvariantCulture) != value?.ToLower(CultureInfo.InvariantCulture))
            //{
                var old_key_info = new IndexKeyInfo()
                {
                    TableNumber = table_info.TableNumber,
                    ColumnNumber = table_info.ColumnNumbers[name],
                    Val = old_val == null ? NullConstant : CalculateMD5Hash(old_val.ToLower(CultureInfo.InvariantCulture)),
                    Id = id,
                    ColumnType = table_info.Columns[name]
                };
                var old_index_key = MakeIndexKey(old_key_info);
                batch.Delete(old_index_key);

                var key_info = new IndexKeyInfo()
                {
                    TableNumber = table_info.TableNumber,
                    ColumnNumber = table_info.ColumnNumbers[name],
                    Val = value == null ? NullConstant : CalculateMD5Hash(value.ToLower(CultureInfo.InvariantCulture)),
                    Id = id,
                    ColumnType = table_info.Columns[name]
                };
                var index_key = MakeIndexKey(key_info);
                batch.Put(index_key, Filler);
            //}
        }
        void SaveBinaryColumn(WriteBatchWithConstraints batch, object value, string name, TableInfo table_info, int id, bool is_new)
        {
            byte[] bres = null;
            byte[] res = null;
            if (value != null && ((byte[])value).Length == 0)
            {
                value = null;
            }
            if (value != null)
            {
                if (((byte[])value).Length > 1048576)
                {
                    throw new LinqDbException("Linqdb: binary value max size is 1Mb.");
                }
                byte[] br = new byte[1 + ((byte[])value).Length];
                br[0] = BinaryOrStringValuePrefix[0];
                for (int i = 0; i < ((byte[])value).Length; i++)
                {
                    br[i + 1] = ((byte[])value)[i];
                }
                bres = br;
                res = NotNullFiller;
            }
            else
            {
                bres = BinaryOrStringValuePrefix;
                res = NullConstant;
            }

            var vkey_info = new IndexKeyInfo()
            {
                TableNumber = table_info.TableNumber,
                ColumnNumber = table_info.ColumnNumbers[name],
                Id = id,
                ColumnType = table_info.Columns[name]
            };
            var value_key = MakeBinaryValueKey(vkey_info);
            byte[] old_value = null;
            if (!is_new)
            {
                old_value = leveld_db.Get(value_key);
            }
            batch.Put(value_key, bres);

            //for == (!=) null to work
            //first delete old index 
            if (old_value != null)
            {
                byte[] old_filler = null;
                if (ValsEqual(old_value, BinaryOrStringValuePrefix))
                {
                    old_filler = NullConstant;
                }
                else
                {
                    old_filler = NotNullFiller;
                }
                var old_key_info = new IndexKeyInfo()
                {
                    TableNumber = table_info.TableNumber,
                    ColumnNumber = table_info.ColumnNumbers[name],
                    Val = old_filler,
                    Id = id,
                    ColumnType = table_info.Columns[name]
                };
                batch.Delete(MakeIndexKey(old_key_info));
            }
            var key_info = new IndexKeyInfo()
            {
                TableNumber = table_info.TableNumber,
                ColumnNumber = table_info.ColumnNumbers[name],
                Val = res,
                Id = id,
                ColumnType = table_info.Columns[name]
            };
            var index_key = MakeIndexKey(key_info);
            batch.Put(index_key, Filler);
        }

        public void WriteTransactionBatch(WriteBatchWithConstraints batch)
        {
            leveld_db.Write(batch._writeBatch);
        }

        public byte[] CalculateMD5Hash(string input)
        {
            if (input == null)
            {
                return null;
            }
            string prefix = null;
            if (input.Length < 10)
            {
                prefix = input.PadRight(10, '!');
            }
            else
            {
                prefix = input.Substring(0, 10);
            }

            var hash = CalculateHash(input);
            var pr_bytes = Encoding.UTF8.GetBytes(prefix).ToList();
            var b = BitConverter.GetBytes(hash);
            pr_bytes.AddRange(b);
            return pr_bytes.ToArray();
        }

        static UInt64 CalculateHash(string read)
        {
            UInt64 hashedValue = 3074457345618258791ul;
            for (int i = 0; i < read.Length; i++)
            {
                hashedValue += read[i];
                hashedValue *= 3074457345618258799ul;
            }
            return hashedValue;
        }

        byte[] Filler = new List<byte>() { (byte)0 }.ToArray();
        byte[] NotNullFiller = new List<byte>() { (byte)46 }.ToArray();
        byte[] NullConstant = new List<byte>() { (byte)45 }.ToArray();
        //string NullConstantString = Convert.ToBase64String(new List<byte>() { (byte)45 }.ToArray());
        byte[] PostEverything = new List<byte>() { (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126, (byte)126 }.ToArray(); //30 in length so that to be after string index which is 26 in length
        byte[] PostNullConstantPreEverythingElse = new List<byte>() { (byte)45, (byte)45, (byte)45 }.ToArray();
        byte[] PreNull = new List<byte>() { (byte)43 }.ToArray();
        byte[] IndexKeyStart = new List<byte>() { (byte)97 }.ToArray();
        byte[] ValueKeyStart = new List<byte>() { (byte)98 }.ToArray();
        byte[] CountKeyStart = new List<byte>() { (byte)99 }.ToArray();
        //byte[] PhaseCountKeyStart = new List<byte>() { (byte)100 }.ToArray();
        byte[] StringSearchIndexKeyStart = new List<byte>() { (byte)105 }.ToArray();
        byte[] StringValueKeyStart = new List<byte>() { (byte)115 }.ToArray();
        byte[] BinaryValueKeyStart = new List<byte>() { (byte)119 }.ToArray();
        byte[] SnapshotIdKeyStart = new List<byte>() { (byte)120 }.ToArray();
        byte[] BinaryOrStringValuePrefix = new List<byte>() { (byte)58 }.ToArray();

        bool ValsEqual(byte[] a, byte[] b)
        {
            if (a.Length != b.Length)
            {
                return false;
            }
            for (int i = 0; i < a.Length; i++)
            {
                if (a[i] != b[i])
                {
                    return false;
                }
            }
            return true;
        }
        bool ValsContains(byte[] a, byte[] b)
        {
            if (a.Length > b.Length)
            {
                return false;
            }
            for (int i = 0; i < a.Length; i++)
            {
                if (a[i] != b[i])
                {
                    return false;
                }
            }
            return true;
        }
        bool ValGreater(byte[] a, byte[] b)
        {
            if (a.Length != b.Length)
            {
                throw new LinqDbException("Linqdb: different val lengths!");
            }
            for (int i = 0; i < a.Length; i++)
            {
                if (a[i] > b[i])
                {
                    return true;
                }
                else if (a[i] < b[i])
                {
                    return false;
                }

            }
            return false;
        }
        public byte[] MakeSnapshotKey(short TableNumber, short ColumnNumber)
        {
            byte[] list = new byte[5];
            list[0] = SnapshotIdKeyStart[0];
            list[1] = (byte)(TableNumber >> 8);
            list[2] = (byte)(TableNumber & 255);
            list[3] = (byte)(ColumnNumber >> 8);
            list[4] = (byte)(ColumnNumber & 255);
            return list;
        }
        byte[] MakeIndexKey(IndexKeyInfo info)
        {
            byte[] list = new byte[10 + info.Val.Length];
            list[0] = IndexKeyStart[0];
            list[1] = (byte)(info.TableNumber >> 8);
            list[2] = (byte)(info.TableNumber & 255);
            list[3] = (byte)(info.ColumnNumber >> 8);
            list[4] = (byte)(info.ColumnNumber & 255);
            list[5] = (byte)(info.Val.Length & 255);
            for (int i = 0; i < info.Val.Length; i++)
            {
                list[6 + i] = info.Val[i];
            }
            list[6 + info.Val.Length] = (byte)(info.Id >> 0x18);
            list[7 + info.Val.Length] = (byte)(info.Id >> 0x10);
            list[8 + info.Val.Length] = (byte)(info.Id >> 8);
            list[9 + info.Val.Length] = (byte)(info.Id);

            return list;
        }
        byte[] MakeIndexSearchKey(IndexKeyInfo info)
        {
            byte[] list = new byte[6 + info.Val.Length];
            list[0] = IndexKeyStart[0];
            list[1] = (byte)(info.TableNumber >> 8);
            list[2] = (byte)(info.TableNumber & 255);
            list[3] = (byte)(info.ColumnNumber >> 8);
            list[4] = (byte)(info.ColumnNumber & 255);
            list[5] = (byte)(info.Val.Length & 255);
            for (int i = 0; i < info.Val.Length; i++)
            {
                list[6 + i] = info.Val[i];
            }
            return list;
        }
        byte[] MakeValueKey(IndexKeyInfo info)
        {
            byte[] list = new byte[9];
            list[0] = ValueKeyStart[0];
            list[1] = (byte)(info.TableNumber >> 8);
            list[2] = (byte)(info.TableNumber & 255);
            list[3] = (byte)(info.ColumnNumber >> 8);
            list[4] = (byte)(info.ColumnNumber & 255);
            list[5] = (byte)(info.Id >> 0x18);
            list[6] = (byte)(info.Id >> 0x10);
            list[7] = (byte)(info.Id >> 8);
            list[8] = (byte)(info.Id);
            return list;
        }
        byte[] MakeStringValueKey(IndexKeyInfo info)
        {
            byte[] list = new byte[9];
            list[0] = StringValueKeyStart[0];
            list[1] = (byte)(info.TableNumber >> 8);
            list[2] = (byte)(info.TableNumber & 255);
            list[3] = (byte)(info.ColumnNumber >> 8);
            list[4] = (byte)(info.ColumnNumber & 255);
            list[5] = (byte)(info.Id >> 0x18);
            list[6] = (byte)(info.Id >> 0x10);
            list[7] = (byte)(info.Id >> 8);
            list[8] = (byte)(info.Id);
            return list;
        }
        byte[] MakeFirstStringValueKey(IndexKeyInfo info)
        {
            byte[] list = new byte[5];
            list[0] = StringValueKeyStart[0];
            list[1] = (byte)(info.TableNumber >> 8);
            list[2] = (byte)(info.TableNumber & 255);
            list[3] = (byte)(info.ColumnNumber >> 8);
            list[4] = (byte)(info.ColumnNumber & 255);
            return list;
        }
        byte[] MakeStringIndexKey(IndexKeyInfo info, string word, int phase)
        {
            var wb = Encoding.UTF8.GetBytes(word);
            byte[] list = new byte[9 + wb.Length];
            list[0] = StringSearchIndexKeyStart[0];
            list[1] = (byte)(info.TableNumber >> 8);
            list[2] = (byte)(info.TableNumber & 255);
            list[3] = (byte)(info.ColumnNumber >> 8);
            list[4] = (byte)(info.ColumnNumber & 255);

            //short count = (short)list.Count();
            //list[5] = (byte)(count >> 8);
            //list[6] = (byte)(count & 255);
            for (int i = 0; i < wb.Length; i++)
            {
                list[5 + i] = wb[i];
            }
            list[5 + wb.Length] = (byte)(phase >> 0x18);
            list[6 + wb.Length] = (byte)(phase >> 0x10);
            list[7 + wb.Length] = (byte)(phase >> 8);
            list[8 + wb.Length] = (byte)(phase);
            return list;
        }
        string GetStringIndexWord(byte[] key)
        {
            byte[] res = new byte[key.Length - 9];
            for (int i = 5; i < key.Length - 4; i++)
            {
                res[i - 5] = key[i];
            }
            return Encoding.UTF8.GetString(res);
        }
        byte[] MakeBinaryValueKey(IndexKeyInfo info)
        {
            byte[] list = new byte[9];
            list[0] = BinaryValueKeyStart[0];
            list[1] = (byte)(info.TableNumber >> 8);
            list[2] = (byte)(info.TableNumber & 255);
            list[3] = (byte)(info.ColumnNumber >> 8);
            list[4] = (byte)(info.ColumnNumber & 255);
            list[5] = (byte)(info.Id >> 0x18);
            list[6] = (byte)(info.Id >> 0x10);
            list[7] = (byte)(info.Id >> 8);
            list[8] = (byte)(info.Id);
            return list;
        }
        byte[] MakeFirstBinaryValueKey(IndexKeyInfo info)
        {
            byte[] list = new byte[5];
            list[0] = BinaryValueKeyStart[0];
            list[1] = (byte)(info.TableNumber >> 8);
            list[2] = (byte)(info.TableNumber & 255);
            list[3] = (byte)(info.ColumnNumber >> 8);
            list[4] = (byte)(info.ColumnNumber & 255);

            return list;
        }
        byte[] MakeFirstValueKey(IndexKeyInfo info)
        {
            byte[] list = new byte[5];
            list[0] = ValueKeyStart[0];
            list[1] = (byte)(info.TableNumber >> 8);
            list[2] = (byte)(info.TableNumber & 255);
            list[3] = (byte)(info.ColumnNumber >> 8);
            list[4] = (byte)(info.ColumnNumber & 255);

            return list;
        }

        IndexKeyInfo GetValueKey(byte[] key)
        {
            if (key.Length < 9)
            {
                return new IndexKeyInfo()
                {
                    Marker = new byte[1] { key[0] }
                };
            }
            var res = new IndexKeyInfo()
            {
                TableNumber = BitConverter.ToInt16(new byte[2] { key[2], key[1] }, 0),
                ColumnNumber = BitConverter.ToInt16(new byte[2] { key[4], key[3] }, 0),
                Id = BitConverter.ToInt32(new byte[4] { key[8], key[7], key[6], key[5] }, 0),
                Marker = new byte[1] { key[0] }
            };

            return res;
        }

        IndexKeyInfo GetIndexKey(byte[] key)
        {
            if (key[0] != IndexKeyStart[0])
            {
                return new IndexKeyInfo()
                {
                    NotKey = true
                };
            }

            var res = new IndexKeyInfo()
            {
                TableNumber = (short)(key[1] << 8 | key[2]),
                ColumnNumber = (short)(key[3] << 8 | key[4]),
            };

            short var_length = (short)key[5];
            var val = new byte[var_length];
            for (int i = 0; i < var_length; i++)
            {
                val[i] = key[6 + i];
            }
            res.Val = val;
            res.Id = (int)(key[6 + var_length] << 24 | key[7 + var_length] << 16 | key[8 + var_length] << 8 | key[9 + var_length]);

            return res;
        }

        void IncrementCount(TableInfo table_info, HashSet<int> ids, WriteBatchWithConstraints batch, Dictionary<string, int> trans_count_cache)
        {
            byte[] c = null;
            var ckey = CountKeyStart.ToList();
            ckey.AddRange(BitConverter.GetBytes(table_info.TableNumber));
            string hash = Convert.ToBase64String(ckey.ToArray());

            if (trans_count_cache != null)
            {
                if (trans_count_cache.ContainsKey(hash))
                {
                    c = BitConverter.GetBytes(trans_count_cache[hash]);
                }
                else
                {
                    c = leveld_db.Get(ckey.ToArray());
                    if (c == null)
                    {
                        trans_count_cache[hash] = 0;
                    }
                    else
                    {
                        trans_count_cache[hash] = BitConverter.ToInt32(c, 0);
                    }
                }
            }
            else
            {
                c = leveld_db.Get(ckey.ToArray());
            }

            foreach (var id in ids)
            {
                var k = MakeIndexKey(new IndexKeyInfo()
                {
                    TableNumber = table_info.TableNumber,
                    ColumnNumber = table_info.ColumnNumbers["Id"],
                    Id = Convert.ToInt32(id),
                    Val = BitConverter.GetBytes(Convert.ToInt32(id)).MyReverseNoCopy(),
                    ColumnType = table_info.Columns["Id"]
                });
                var r = leveld_db.Get(k);
                if (r == null)
                {
                    if (c == null)
                    {
                        if (trans_count_cache != null)
                        {
                            trans_count_cache[hash] = 1;
                            c = BitConverter.GetBytes(1);
                        }
                        else
                        {
                            batch.Put(ckey.ToArray(), BitConverter.GetBytes(1));
                            c = BitConverter.GetBytes(1);
                        }
                    }
                    else
                    {
                        if (trans_count_cache != null)
                        {
                            trans_count_cache[hash]++;
                            c = BitConverter.GetBytes(trans_count_cache[hash]);
                        }
                        else
                        {
                            int v = BitConverter.ToInt32(c, 0) + 1;
                            batch.Put(ckey.ToArray(), BitConverter.GetBytes(v));
                            c = BitConverter.GetBytes(v);
                        }
                    }
                }
            }
        }
        void DecrementCount(TableInfo table_info, HashSet<int> ids, WriteBatchWithConstraints batch, Dictionary<string, int> trans_count_cache)
        {
            if (!ids.Any())
            {
                return;
            }
            var ckey = CountKeyStart.ToList();
            ckey.AddRange(BitConverter.GetBytes(table_info.TableNumber));
            string hash = Convert.ToBase64String(ckey.ToArray());
            if (trans_count_cache != null)
            {
                if (!trans_count_cache.ContainsKey(hash))
                {
                    var counter_val = leveld_db.Get(ckey.ToArray());
                    int counter = BitConverter.ToInt32(counter_val, 0);
                    trans_count_cache[hash] = counter;
                }
                trans_count_cache[hash] -= ids.Count();
            }
            else
            {
                var counter_val = leveld_db.Get(ckey.ToArray());
                int counter = BitConverter.ToInt32(counter_val, 0);
                counter -= ids.Count();
                batch.Put(ckey.ToArray(), BitConverter.GetBytes(counter));
            }
        }
        public void FlushTransCountCache(Dictionary<string, int> trans_count_cache, WriteBatchWithConstraints batch)
        {
            foreach (var val in trans_count_cache)
            {
                var ckey = Convert.FromBase64String(val.Key);
                batch.Put(ckey, BitConverter.GetBytes(val.Value));
            }
        }

        int GetTableRowCount(TableInfo table_info, ReadOptions ro)
        {
            var ckey = CountKeyStart.ToList();
            ckey.AddRange(BitConverter.GetBytes(table_info.TableNumber));
            byte[] counter_val = null;
            if (ro != null)
            {
                counter_val = leveld_db.Get(ckey.ToArray(), null, ro);
            }
            else
            {
                counter_val = leveld_db.Get(ckey.ToArray());
            }
            if (counter_val == null)
            {
                return 0;
            }
            else
            {
                return BitConverter.ToInt32(counter_val, 0);
            }
        }

        //int GetLastStringPhase(TableInfo table_info, Dictionary<string, int> trans_phase_cache)
        //{
        //    var ckey = PhaseCountKeyStart.ToList();
        //    ckey.AddRange(BitConverter.GetBytes(table_info.TableNumber));
        //    var key = ckey.ToArray();

        //    string hash = null;
        //    if (trans_phase_cache != null)
        //    {
        //        hash = Convert.ToBase64String(key);
        //        if (trans_phase_cache.ContainsKey(hash))
        //        {
        //            return trans_phase_cache[hash];
        //        }
        //    }

        //    int res;
        //    var counter_val = leveld_db.Get(key);
        //    if (counter_val == null)
        //    {
        //        res = 0;
        //    }
        //    else
        //    {
        //        res = BitConverter.ToInt32(counter_val, 0);
        //    }

        //    if (hash != null)
        //    {
        //        trans_phase_cache[hash] = res;
        //    }

        //    return res;
        //}

        //void SetLastStringPhase(TableInfo table_info, int phase, WriteBatchWithConstraints batch, Dictionary<string, int> trans_phase_cache)
        //{
        //    var ckey = PhaseCountKeyStart.ToList();
        //    ckey.AddRange(BitConverter.GetBytes(table_info.TableNumber));
        //    var key = ckey.ToArray();
        //    var val = BitConverter.GetBytes(phase);

        //    if (trans_phase_cache != null)
        //    {
        //        var hash = Convert.ToBase64String(key);
        //        trans_phase_cache[hash] = phase;
        //        return;
        //    }

        //    batch.Put(key, val);
        //}
        //public void FlushTransPhaseCache(Dictionary<string, int> trans_phase_cache, WriteBatchWithConstraints batch)
        //{
        //    foreach (var val in trans_phase_cache)
        //    {
        //        batch.Put(Convert.FromBase64String(val.Key), BitConverter.GetBytes(val.Value));
        //    }
        //}
        TableInfo CheckTableInfo<T>()
        {
            var name = typeof(T).Name;
            ConcurrentDictionary<string, LinqDbTypes> columns = new ConcurrentDictionary<string, LinqDbTypes>();
            foreach (var p in typeof(T).GetProperties())
            {
                var t = TypeToLinqType(p);
                if (t == LinqDbTypes.unknown_)
                {
                    continue;
                }
                columns[p.Name] = t;
            }
            var table_info = new TableInfo()
            {
                Name = name,
                Columns = columns
            };
            UpdateTableInfo(name, table_info, true);

            return table_info;
        }

        TableInfo CheckTableInfo(Dictionary<string, string> def, string table_name, bool can_write)
        {
            var name = table_name;
            ConcurrentDictionary<string, LinqDbTypes> columns = new ConcurrentDictionary<string, LinqDbTypes>();
            foreach (var p in def.Keys)
            {
                var t = StringTypeToLinqType(def[p]);
                if (t == LinqDbTypes.unknown_)
                {
                    continue;
                }
                columns[p] = t;
            }
            var table_info = new TableInfo()
            {
                Name = name,
                Columns = columns
            };
            UpdateTableInfo(name, table_info, can_write);

            return table_info;
        }
    }

    public class IndexKeyInfo
    {
        public bool NotKey { get; set; }
        public short TableNumber { get; set; }
        public short ColumnNumber { get; set; }

        public byte[] Val { get; set; }
        public int Id { get; set; }
        public LinqDbTypes ColumnType { get; set; }
        public byte[] Marker { get; set; }
    }
}
