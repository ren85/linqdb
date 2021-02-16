using RocksDbSharp;
using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public void UpdateIncrement<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, int?> values, LinqdbTransactionInternal trans)
        {
            GenericUpdateIncrement<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), trans);
        }
        public void Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, int?> values, LinqdbTransactionInternal trans)
        {
            GenericUpdate<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), trans, LinqDbTypes.int_);
        }
        public void Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, int> values, LinqdbTransactionInternal trans)
        {
            GenericUpdate<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), trans, LinqDbTypes.int_);
        }
        public void Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, double?> values, LinqdbTransactionInternal trans)
        {
            GenericUpdate<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), trans, LinqDbTypes.double_);
        }
        public void Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, double> values, LinqdbTransactionInternal trans)
        {
            GenericUpdate<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), trans, LinqDbTypes.double_);
        }
        public void Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, DateTime?> values, LinqdbTransactionInternal trans)
        {
            GenericUpdate<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), trans, LinqDbTypes.DateTime_);
        }
        public void Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, DateTime> values, LinqdbTransactionInternal trans)
        {
            GenericUpdate<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), trans, LinqDbTypes.DateTime_);
        }
        public void Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, byte[]> values, LinqdbTransactionInternal trans)
        {
            GenericUpdate<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), trans, LinqDbTypes.binary_);
        }
        public void Update<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, string> values, LinqdbTransactionInternal trans)
        {
            GenericUpdate<T, TKey>(keySelector, values.ToDictionary(f => f.Key, f => (object)f.Value), trans, LinqDbTypes.string_);
        }
        public void GenericUpdate<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, object> values, LinqdbTransactionInternal trans, LinqDbTypes type)
        {
            CheckTableInfo<T>();
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            var table_info = GetTableInfo(typeof(T).Name);


            if (table_info.Columns[name] != type)
            {
                throw new LinqDbException("Linqdb: wrong data type for given column.");
            }

            var info = new UpdateInfo()
            {
                TableNumber = table_info.TableNumber,
                ColumnNumber = table_info.ColumnNumbers[name],
                ColumnType = table_info.Columns[name],
                TableInfo = table_info,
                ColumnName = name
            };

            var data = new Dictionary<int, object>();
            foreach (var v in values)
            {
                data[v.Key] = v.Value;
            }
            Update<T>(info, data, table_info, trans);
        }
        public void GenericUpdateIncrement<T, TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, object> values, LinqdbTransactionInternal trans)
        {
            CheckTableInfo<T>();
            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            var table_info = GetTableInfo(typeof(T).Name);
            var info = new UpdateInfo()
            {
                TableNumber = table_info.TableNumber,
                ColumnNumber = table_info.ColumnNumbers[name],
                ColumnType = table_info.Columns[name],
                TableInfo = table_info,
                ColumnName = name
            };

            var data = new Dictionary<int, object>();
            foreach (var v in values)
            {
                data[v.Key] = v.Value;
            }
            UpdateIncrement<T>(info, data, table_info, trans);
        }

        public void UpdateIncrement<T>(UpdateInfo info, Dictionary<int, object> values, TableInfo table_info, LinqdbTransactionInternal trans)
        {
            Dictionary<int, object> current_values = new Dictionary<int, object>();
            foreach (var kv in values)
            {
                current_values[kv.Key] = kv.Value;
            }
            if (current_values.Any())
            {
                UpdateBatchIncrement<T>(info, current_values, table_info, trans);
            }
        }
        public void Update<T>(UpdateInfo info, Dictionary<int, object> values, TableInfo table_info, LinqdbTransactionInternal trans)
        {
            Dictionary<int, object> current_values = new Dictionary<int, object>();
            foreach (var kv in values)
            {
                current_values[kv.Key] = kv.Value;
            }
            if (current_values.Any())
            {
                UpdateBatch<T>(info, current_values, table_info, trans);
            }
        }

        public void UpdateBatchIncrement<T>(UpdateInfo info, Dictionary<int, object> values, TableInfo table_info, LinqdbTransactionInternal trans)
        {
            var _write_lock = GetTableWriteLock(typeof(T).Name);
            if (trans == null)
            {
                lock (_write_lock)
                {
                    using (WriteBatchWithConstraints batch = new WriteBatchWithConstraints())
                    {
                        Dictionary<string, KeyValuePair<byte[], HashSet<int>>> string_cache = new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>();
                        Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> meta_index = BuildMetaOnIndex(table_info);
                        UpdateBatch(info, values, table_info, batch, string_cache, meta_index);
                        WriteStringCacheToBatch(batch, string_cache, table_info, null);
                        var snapshots_dic = InsertIndexChanges(table_info, meta_index);
                        foreach (var snap in snapshots_dic)
                        {
                            var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[snap.Key]);
                            batch.Put(skey, Encoding.UTF8.GetBytes(snap.Value));
                        }
                        leveld_db.Write(batch._writeBatch);
                    }
                }
            }
            else
            {
                var type_name = typeof(T).Name;
                info.TableInfo = table_info;
                if (!trans.data_to_update.ContainsKey(type_name))
                {
                    trans.data_to_update[type_name] = new List<KeyValuePair<UpdateInfo, Dictionary<int, object>>>();
                }
                trans.data_to_update[type_name].Add(new KeyValuePair<UpdateInfo, Dictionary<int, object>>(info, values));
            }
        }

        public void UpdateBatch<T>(UpdateInfo info, Dictionary<int, object> values, TableInfo table_info, LinqdbTransactionInternal trans)
        {
            if (trans == null)
            {
                bool done = false;
                string error = null;
                var ilock = ModifyBatch.GetTableUpdateBatchLock(table_info.Name);
                var key = table_info.Name + "|" + info.ColumnName;
                lock (ilock)
                {
                    if (!ModifyBatch._update_batch.ContainsKey(key))
                    {
                        ModifyBatch._update_batch[key] = new UpdateData() { Callbacks = new List<Action<string>>(), values = values};
                    }
                    else
                    {
                        var vals = ModifyBatch._update_batch[key].values;
                        foreach (var v in values)
                        {
                            if (!vals.ContainsKey(v.Key))
                            {
                                vals[v.Key] = v.Value;
                            }
                        }
                    }
                    ModifyBatch._update_batch[key].Callbacks.Add(f =>
                    {
                        done = true;
                        error = f;
                    });
                }

                var _write_lock = GetTableWriteLock(typeof(T).Name);

                bool lockAcquired = false;
                int maxWaitMs = 60000;
                UpdateData _update_data = null;
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
                        //    throw new LinqDbException("Linqdb: Update waited too long to acquire write lock. Is the load too high?");
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
                        _update_data = ModifyBatch._update_batch[key];
                        var oval = new UpdateData();
                        ModifyBatch._update_batch.TryRemove(key, out oval);
                    }
                    if (_update_data.values.Any())
                    {
                        using (WriteBatchWithConstraints batch = new WriteBatchWithConstraints())
                        {
                            Dictionary<string, KeyValuePair<byte[], HashSet<int>>> string_cache = new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>();
                            Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> meta_index = BuildMetaOnIndex(table_info);
                            UpdateBatch(info, _update_data.values, table_info, batch, string_cache, meta_index);
                            WriteStringCacheToBatch(batch, string_cache, table_info, null);
                            var snapshots_dic = InsertIndexChanges(table_info, meta_index);
                            foreach (var snap in snapshots_dic)
                            {
                                var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[snap.Key]);
                                batch.Put(skey, Encoding.UTF8.GetBytes(snap.Value));
                            }
                            leveld_db.Write(batch._writeBatch);
                        }
                    }
                    foreach (var cb in _update_data.Callbacks)
                    {
                        cb(null);
                    }
                }
                catch (Exception ex)
                {
                    if (_update_data != null)
                    {
                        var additionalInfo = ex.Message;
                        if (_update_data.Callbacks.Count() > 1)
                        {
                            additionalInfo += " This error could belong to another entity which happened to be in the same batch.";
                        }
                        foreach (var cb in _update_data.Callbacks)
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
            else
            {
                var type_name = typeof(T).Name;
                info.TableInfo = table_info;
                if (!trans.data_to_update.ContainsKey(type_name))
                {
                    trans.data_to_update[type_name] = new List<KeyValuePair<UpdateInfo, Dictionary<int, object>>>();
                }
                trans.data_to_update[type_name].Add(new KeyValuePair<UpdateInfo, Dictionary<int, object>>(info, values));
            }
        }

        public void UpdateBatch(UpdateInfo info, Dictionary<int, object> values, TableInfo table_info, WriteBatchWithConstraints batch, Dictionary<string, KeyValuePair<byte[], HashSet<int>>> string_cache, Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> memory_index_meta)
        {
            foreach (var item in values)
            {
                var key = MakeIndexKey(new IndexKeyInfo()
                {
                    TableNumber = info.TableNumber,
                    ColumnNumber = table_info.ColumnNumbers["Id"],
                    Val = BitConverter.GetBytes(item.Key).MyReverseNoCopy(),
                    Id = item.Key
                });
                var index_val = leveld_db.Get(key);
                if (index_val == null)
                {
                    continue;
                }

                object value = item.Value;

                if (info.ColumnType == LinqDbTypes.string_)
                {
                    SaveStringData(batch, (string)item.Value, info.ColumnName, info.TableInfo, item.Key, string_cache, false);
                }
                else if (info.ColumnType == LinqDbTypes.binary_)
                {
                    SaveBinaryColumn(batch, value, info.ColumnName, info.TableInfo, item.Key, false);
                }
                else
                {
                    IndexDeletedData index_deleted = null;
                    IndexNewData index_new = null;
                    IndexChangedData index_changed = null;
                    if (memory_index_meta.ContainsKey(info.ColumnName))
                    {
                        index_deleted = memory_index_meta[info.ColumnName].Item2;
                        index_new = memory_index_meta[info.ColumnName].Item1;
                        index_changed = memory_index_meta[info.ColumnName].Item3;
                    }
                    SaveDataColumn(batch, value, info.ColumnName, info.ColumnType, info.TableInfo, item.Key, false, index_new, index_changed);
                }
            }
            //WriteStringCacheToBatch(batch, string_cache, table_info, null);
        }
    }

    public class UpdateInfo
    {
        public short TableNumber { get; set; }
        public short ColumnNumber { get; set; }
        public LinqDbTypes ColumnType { get; set; }
        public TableInfo TableInfo { get; set; }
        public string ColumnName { get; set; }
    }
}
