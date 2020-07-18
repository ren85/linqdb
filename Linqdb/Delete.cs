using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public void Delete<T>(HashSet<int> ids, LinqdbTransactionInternal trans)
        {
            CheckTableInfo<T>();
            HashSet<int> current_values = new HashSet<int>();
            foreach (var v in ids)
            {
                current_values.Add(v);
            }
            if (current_values.Any())
            {
                DeleteBatch<T>(current_values, trans);
            }
        }
        public void DeleteBatch<T>(HashSet<int> ids, LinqdbTransactionInternal trans)
        {
            var _write_lock = GetTableWriteLock(typeof(T).Name);
            if (trans == null)
            {
                var table_info = GetTableInfo(typeof(T).Name);
                bool done = false;
                string error = null;
                var ilock = ModifyBatch.GetTableDeleteBatchLock(table_info.Name);
                lock (ilock)
                {
                    if (!ModifyBatch._delete_batch.ContainsKey(table_info.Name))
                    {
                        ModifyBatch._delete_batch[table_info.Name] = new DeleteData() { Callbacks = new List<Action<string>>(), ids = ids };
                    }
                    else
                    {
                        ModifyBatch._delete_batch[table_info.Name].ids.UnionWith(ids);
                    }
                    ModifyBatch._delete_batch[table_info.Name].Callbacks.Add(f =>
                    {
                        done = true;
                        error = f;
                    });
                }

                bool lockAcquired = false;
                int maxWaitMs = 60000;
                DeleteData _delete_data = null;
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
                        //    throw new LinqDbException("Linqdb: Delete waited too long to acquire write lock. Is the load too high?");
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
                        _delete_data = ModifyBatch._delete_batch[table_info.Name];
                        var oval = new DeleteData();
                        ModifyBatch._delete_batch.TryRemove(table_info.Name, out oval);
                    }
                    if (_delete_data.ids.Any())
                    {
                        using (WriteBatchWithConstraints batch = new WriteBatchWithConstraints())
                        {
                            Dictionary<string, KeyValuePair<byte[], HashSet<int>>> string_cache = new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>();
                            Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> meta_index = BuildMetaOnIndex(table_info);
                            DeleteBatch(_delete_data.ids, table_info, batch, null, string_cache, meta_index);
                            WriteStringCacheToBatch(batch, string_cache, table_info);
                            var snapshots_dic = InsertIndexChanges(table_info, meta_index);
                            foreach (var snap in snapshots_dic)
                            {
                                var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[snap.Key]);
                                batch.Put(skey, Encoding.UTF8.GetBytes(snap.Value));
                            }
                            leveld_db.Write(batch._writeBatch);
                        }
                    }
                    foreach (var cb in _delete_data.Callbacks)
                    {
                        cb(null);
                    }
                }
                catch (Exception ex)
                {
                    if (_delete_data != null)
                    {
                        var additionalInfo = ex.Message;
                        if (_delete_data.Callbacks.Count() > 1)
                        {
                            additionalInfo += " This error could belong to another entity which happened to be in the same batch.";
                        }
                        foreach (var cb in _delete_data.Callbacks)
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
                var table_info = GetTableInfo(type_name);
                if (!trans.data_to_delete.ContainsKey(type_name))
                {
                    trans.data_to_delete[type_name] = new KeyValuePair<TableInfo, HashSet<int>>(table_info, new HashSet<int>());
                }
                trans.data_to_delete[type_name].Value.UnionWith(ids);
            }
        }

        public void DeleteBatch(HashSet<int> ids, TableInfo table_info, WriteBatchWithConstraints batch, Dictionary<string, int> trans_count_cache, Dictionary<string, KeyValuePair<byte[], HashSet<int>>> string_cache, Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> memory_index_meta)
        {            
            var existing_ids = new HashSet<int>();
            var historic_columns = GetAllColumnsWithHistoric(table_info.TableNumber);
            foreach (var id in ids)
            {
                var key = MakeIndexKey(new IndexKeyInfo()
                {
                    TableNumber = table_info.TableNumber,
                    ColumnNumber = table_info.ColumnNumbers["Id"],
                    Val = BitConverter.GetBytes(id).MyReverseNoCopy(),
                    Id = id
                });
                var index_val = leveld_db.Get(key);
                if (index_val == null)
                {
                    continue;
                }
                existing_ids.Add(id);
                foreach (var column in historic_columns)
                {
                    if (column.Item2 != LinqDbTypes.binary_ && column.Item2 != LinqDbTypes.string_)
                    {
                        var column_name = table_info.ColumnNumbers.Select(f => new { f.Key, f.Value }).Where(f => f.Value == column.Item1).FirstOrDefault();
                        IndexDeletedData index_deleted = null;
                        if (column_name != null && memory_index_meta.ContainsKey(column_name.Key))
                        {
                            index_deleted = memory_index_meta[column_name.Key].Item2;
                        }
                        DeleteDataColumn(batch, table_info.TableNumber, column.Item1, column.Item2, id, index_deleted);
                    }
                    else if (column.Item2 == LinqDbTypes.binary_)
                    {
                        DeleteBinaryColumn(batch, table_info.TableNumber, column.Item1, column.Item2, id);
                    }
                    else
                    {
                        DeleteStringColumn(batch, table_info.TableNumber, column.Item1, table_info, column.Item2, id, string_cache);
                    }
                }
            }
            DecrementCount(table_info, existing_ids, batch, trans_count_cache);
            //WriteStringCacheToBatch(batch, string_cache, table_info, trans_phase_cache);
        }
        void DeleteDataColumn(WriteBatchWithConstraints batch, short TableNumber, short ColumnNumber, LinqDbTypes ColumnType, int id, IndexDeletedData index_deleted)
        {
            var key_info = new IndexKeyInfo()
            {
                ColumnNumber = ColumnNumber,
                TableNumber = TableNumber,
                ColumnType = ColumnType,
                Id = id
            };
            var value_key = MakeValueKey(key_info);
            var old_val = leveld_db.Get(value_key);
            if (old_val == null) //maybe new column added and delete invoked
            {
                return;
            }
            bool is_old_negative = false;
            if (ValsEqual(old_val, NullConstant))
            {
                is_old_negative = false;
            }
            else if (ColumnType == LinqDbTypes.double_ && BitConverter.ToDouble(old_val.MyReverseWithCopy(), 0) < 0)
            {
                is_old_negative = true;
                old_val = BitConverter.GetBytes((BitConverter.ToDouble(old_val.MyReverseWithCopy(), 0) * -1)).MyReverseNoCopy();
            }
            else if (ColumnType == LinqDbTypes.int_ && BitConverter.ToInt32(old_val.MyReverseWithCopy(), 0) < 0)
            {
                is_old_negative = true;
                old_val = BitConverter.GetBytes((BitConverter.ToInt32(old_val.MyReverseWithCopy(), 0) * -1)).MyReverseNoCopy();
            }

            byte[] index_key = null;
            if (is_old_negative)
            {
                index_key = MakeIndexKey(new IndexKeyInfo()
                {
                    TableNumber = TableNumber,
                    ColumnNumber = (short)(-1 * ColumnNumber),
                    ColumnType = ColumnType,
                    Val = old_val,
                    Id = id
                });
            }
            else
            {
                index_key = MakeIndexKey(new IndexKeyInfo()
                {
                    TableNumber = TableNumber,
                    ColumnNumber = ColumnNumber,
                    ColumnType = ColumnType,
                    Val = old_val,
                    Id = id
                });
            }

            batch.Delete(value_key);
            batch.Delete(index_key);

            //keep up the memory index, if there is one
            if (index_deleted != null)
            {
                index_deleted.Ids.Add(id);
            }
        }

        void DeleteBinaryColumn(WriteBatchWithConstraints batch, short TableNumber, short ColumnNumber, LinqDbTypes ColumnType, int id)
        {
            var key_info = new IndexKeyInfo()
            {
                ColumnNumber = ColumnNumber,
                TableNumber = TableNumber,
                ColumnType = ColumnType,
                Id = id
            };
            var value_key = MakeBinaryValueKey(key_info);
            var old_val = leveld_db.Get(value_key);

            var index_key = MakeIndexKey(new IndexKeyInfo()
            {
                TableNumber = TableNumber,
                ColumnNumber = ColumnNumber,
                ColumnType = ColumnType,
                Val = (old_val == null || old_val.Length == 1) ? NullConstant : NotNullFiller,
                Id = id
            });
            batch.Delete(index_key);
            batch.Delete(value_key);
        }
        void DeleteStringColumn(WriteBatchWithConstraints batch, short TableNumber, short ColumnNumber, TableInfo table_info, LinqDbTypes ColumnType, int id, Dictionary<string, KeyValuePair<byte[], HashSet<int>>> cache)
        {
            var key_info = new IndexKeyInfo()
            {
                ColumnNumber = ColumnNumber,
                TableNumber = TableNumber,
                ColumnType = ColumnType,
                Id = id
            };
            var value_key = MakeStringValueKey(key_info);
            var old_val = leveld_db.Get(value_key);
            string old_val_string = null;
            if (old_val != null && old_val.Length != 1)
            {
                old_val_string = Encoding.Unicode.GetString(old_val.Skip(1).ToArray());
            }
            var index_key = MakeIndexKey(new IndexKeyInfo()
            {
                TableNumber = TableNumber,
                ColumnNumber = ColumnNumber,
                ColumnType = ColumnType,
                Val = (old_val == null || old_val.Length == 1) ? NullConstant : CalculateMD5Hash(old_val_string.ToLower(CultureInfo.InvariantCulture)),
                Id = id
            });
            batch.Delete(index_key);
            batch.Delete(value_key);
            var column_name = table_info.ColumnNumbers.Select(f => new { f.Key, f.Value }).Where(f => f.Value == ColumnNumber).FirstOrDefault().Key;
            if (column_name.ToLower().EndsWith("search"))
            {
                UpdateIndex(old_val_string, null, id, batch, ColumnNumber, TableNumber, cache);
            }
        }
    }


}
