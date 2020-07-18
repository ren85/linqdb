using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public class LinqdbTransactionInternal : IDisposable
    {
        public Dictionary<string, KeyValuePair<TableInfo, List<object>>> data_to_save = new Dictionary<string, KeyValuePair<TableInfo, List<object>>>();
        public Dictionary<string, List<KeyValuePair<UpdateInfo, Dictionary<int, object>>>> data_to_update = new Dictionary<string, List<KeyValuePair<UpdateInfo, Dictionary<int, object>>>>();
        public Dictionary<string, KeyValuePair<TableInfo, HashSet<int>>> data_to_delete = new Dictionary<string, KeyValuePair<TableInfo, HashSet<int>>>();

        public Ldb ldb { get; set; }

        public void Commit()
        {

            var ids = new Dictionary<string, HashSet<int>>();
            var update_ids = new Dictionary<string, HashSet<int>>();
            var locks = new List<string>();
            locks.AddRange(data_to_save.Keys);
            locks.AddRange(data_to_update.Keys);
            locks.AddRange(data_to_delete.Keys);
            if (!locks.Any())
            {
                return;
            }
            locks = locks.Distinct().OrderBy(f => f).ToList(); //ordering avoids deadlocks

            var key = locks.Aggregate((a, b) => a + "|" + b);

            //checks for this individual transaction correctness
            foreach (var save_list in data_to_save)
            {
                if (!ids.ContainsKey(save_list.Key))
                {
                    ids[save_list.Key] = new HashSet<int>();
                }
                foreach (var item in save_list.Value.Value)
                {
                    var id = Convert.ToInt32(item.GetType().GetProperty("Id").GetValue(item));
                    if (ids[save_list.Key].Contains(id))
                    {
                        throw new LinqDbException("Linqdb: same entity cannot be modified twice in a transaction. " + save_list.Key + ", id " + id);
                    }
                    else
                    {
                        ids[save_list.Key].Add(id);
                    }
                }
            }
            foreach (var update_list in data_to_update)
            {
                if (!ids.ContainsKey(update_list.Key))
                {
                    ids[update_list.Key] = new HashSet<int>();
                }
                foreach (var update_field in update_list.Value)
                {
                    if (!update_ids.ContainsKey(update_field.Key.TableInfo.Name + "|" + update_field.Key.ColumnName))
                    {
                        update_ids[update_field.Key.TableInfo.Name + "|" + update_field.Key.ColumnName] = new HashSet<int>();
                    }
                    foreach (var id in update_field.Value.Keys)
                    {
                        if (ids[update_field.Key.TableInfo.Name].Contains(id))
                        {
                            throw new LinqDbException("Linqdb: same entity cannot be modified twice in a transaction. " + update_field.Key.TableInfo.Name + ", id " + id);
                        }
                        if (update_ids[update_field.Key.TableInfo.Name + "|" + update_field.Key.ColumnName].Contains(id))
                        {
                            throw new LinqDbException("Linqdb: same entity's field cannot be updated twice in a transaction. " + update_field.Key.TableInfo.Name + ", field " + update_field.Key.ColumnName + ", id " + id);
                        }
                        else
                        {
                            update_ids[update_field.Key.TableInfo.Name + "|" + update_field.Key.ColumnName].Add(id);
                        }
                    }
                }
            }
            foreach (var updated in update_ids)
            {
                var name = updated.Key.Split("|".ToCharArray(), StringSplitOptions.RemoveEmptyEntries)[0].Trim();
                if (!ids.ContainsKey(name))
                {
                    ids[name] = updated.Value;
                }
                else
                {
                    ids[name].UnionWith(updated.Value);
                }
            }
            foreach (var delete_list in data_to_delete)
            {
                if (!ids.ContainsKey(delete_list.Key))
                {
                    ids[delete_list.Key] = new HashSet<int>();
                }
                foreach (var id in delete_list.Value.Value)
                {
                    if (ids[delete_list.Key].Contains(id))
                    {
                        throw new LinqDbException("Linqdb: same entity cannot be modified twice in a transaction. " + delete_list.Key + ", id " + id);
                    }
                    else
                    {
                        ids[delete_list.Key].Add(id);
                    }
                }
            }

            bool no = false; //should we include this transaction in a batch or not (no if we have intersecting ids)
            bool done = false;
            string error = null;
            var ilock = ModifyBatchTransaction.GetTableTransBatchLock(key);
            lock (ilock)
            {
                if (!ModifyBatchTransaction._trans_batch.ContainsKey(key))
                {
                    ModifyBatchTransaction._trans_batch[key] = new TransBatchData() { Callbacks = new List<Action<string>>(), Ids = ids };
                    ModifyBatchTransaction._trans_batch[key].data_to_delete = data_to_delete;
                    ModifyBatchTransaction._trans_batch[key].data_to_save = data_to_save;
                    ModifyBatchTransaction._trans_batch[key].data_to_update = data_to_update;
                }
                else
                {
                    foreach (var tids in ModifyBatchTransaction._trans_batch[key].Ids)
                    {
                        if (tids.Value.Intersect(ids[tids.Key]).Any())
                        {
                            no = true;
                            break;
                        }
                    }
                    if (!no)
                    {
                        foreach (var tids in ModifyBatchTransaction._trans_batch[key].Ids)
                        {
                            tids.Value.UnionWith(ids[tids.Key]);
                        }

                        //merge
                        foreach (var del in ModifyBatchTransaction._trans_batch[key].data_to_delete)
                        {
                            if (data_to_delete.ContainsKey(del.Key))
                            {
                                del.Value.Value.UnionWith(data_to_delete[del.Key].Value);
                            }
                        }
                        foreach (var del in data_to_delete)
                        {
                            if (!ModifyBatchTransaction._trans_batch[key].data_to_delete.ContainsKey(del.Key))
                            {
                                ModifyBatchTransaction._trans_batch[key].data_to_delete[del.Key] = del.Value;
                            }
                        }

                        foreach (var sav in ModifyBatchTransaction._trans_batch[key].data_to_save)
                        {
                            if (data_to_save.ContainsKey(sav.Key))
                            {
                                sav.Value.Value.AddRange(data_to_save[sav.Key].Value);
                            }                            
                        }
                        foreach (var sav in data_to_save)
                        {
                            if (!ModifyBatchTransaction._trans_batch[key].data_to_save.ContainsKey(sav.Key))
                            {
                                ModifyBatchTransaction._trans_batch[key].data_to_save[sav.Key] = sav.Value;
                            }
                        }

                        foreach (var upd in ModifyBatchTransaction._trans_batch[key].data_to_update)
                        {
                            if (data_to_update.ContainsKey(upd.Key))
                            {
                                foreach (var colmn in data_to_update[upd.Key])
                                {
                                    if (upd.Value.Any(f => f.Key.ColumnName == colmn.Key.ColumnName))
                                    {
                                        var dic = upd.Value.First(f => f.Key.ColumnName == colmn.Key.ColumnName).Value;
                                        foreach (var cv in colmn.Value)
                                        {
                                            dic[cv.Key] = cv.Value;
                                        }
                                    }
                                    else
                                    {
                                        upd.Value.Add(colmn);
                                    }
                                }
                            }
                        }
                        foreach (var upd in data_to_update)
                        {
                            if (!ModifyBatchTransaction._trans_batch[key].data_to_update.ContainsKey(upd.Key))
                            {
                                ModifyBatchTransaction._trans_batch[key].data_to_update[upd.Key] = upd.Value;
                            }
                        }

                    }
                }
                if (!no)
                {
                    ModifyBatchTransaction._trans_batch[key].Callbacks.Add(f =>
                    {
                        done = true;
                        error = f;
                    });
                }
            }

            if (!no)
            {
                Dictionary<string, int> trans_count_cache = new Dictionary<string, int>();
                bool lockAcquired = false;
                int maxWaitMs = 60000;
                var _write_locks = new List<object>();
                TransBatchData _trans_data = null;
                try
                {
                    DateTime start = DateTime.Now;
                    while (!done)
                    {
                        var flock = locks.First();
                        var _write_lock = ldb.GetTableWriteLock(flock);
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
                                _write_locks.Add(_write_lock);
                                foreach (var l in locks.Skip(1).ToList())
                                {
                                    var write_lock = ldb.GetTableWriteLock(l);
                                    Monitor.Enter(write_lock);
                                    _write_locks.Add(write_lock);
                                }
                                break;
                            }
                        }
                        Thread.Sleep(250);
                        //if ((DateTime.Now - start).TotalMilliseconds > maxWaitMs)
                        //{
                        //    throw new LinqDbException("Linqdb: Commit waited too long to acquire transaction write lock. Is the load too high?");
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
                        _trans_data = ModifyBatchTransaction._trans_batch[key];
                        var oval = new TransBatchData();
                        ModifyBatchTransaction._trans_batch.TryRemove(key, out oval);
                    }

                    using (WriteBatchWithConstraints batch = new WriteBatchWithConstraints())
                    {
                        //string cache
                        var sc = new Dictionary<string, KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>>();
                        var index_data = new List<Tuple<TableInfo, Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>>>>();
                        foreach (var save_list in _trans_data.data_to_save)
                        {
                            if (!sc.ContainsKey(save_list.Value.Key.Name))
                            {
                                sc[save_list.Value.Key.Name] = new KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>(save_list.Value.Key, new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>());
                            }

                            Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> meta_index = ldb.BuildMetaOnIndex(save_list.Value.Key);
                            ldb.SaveItems(save_list.Value.Key, save_list.Key, save_list.Value.Value, batch, trans_count_cache, sc[save_list.Value.Key.Name].Value, meta_index, true);
                            index_data.Add(new Tuple<TableInfo, Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>>>(save_list.Value.Key, meta_index));
                        }
                        foreach (var update_list in _trans_data.data_to_update)
                        {
                            foreach (var update_field in update_list.Value)
                            {
                                if (!sc.ContainsKey(update_field.Key.TableInfo.Name))
                                {
                                    sc[update_field.Key.TableInfo.Name] = new KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>(update_field.Key.TableInfo, new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>());
                                }
                                Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> meta_index = ldb.BuildMetaOnIndex(update_field.Key.TableInfo);
                                ldb.UpdateBatch(update_field.Key, update_field.Value, update_field.Key.TableInfo, batch, sc[update_field.Key.TableInfo.Name].Value, meta_index);
                                var snapshots_dic = ldb.InsertIndexChanges(update_field.Key.TableInfo, meta_index);
                                index_data.Add(new Tuple<TableInfo, Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>>>(update_field.Key.TableInfo, meta_index));
                            }

                        }
                        foreach (var delete_list in _trans_data.data_to_delete)
                        {
                            if (!sc.ContainsKey(delete_list.Value.Key.Name))
                            {
                                sc[delete_list.Value.Key.Name] = new KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>(delete_list.Value.Key, new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>());
                            }
                            Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> meta_index = ldb.BuildMetaOnIndex(delete_list.Value.Key);
                            ldb.DeleteBatch(delete_list.Value.Value, delete_list.Value.Key, batch, trans_count_cache, sc[delete_list.Value.Key.Name].Value, meta_index);
                            index_data.Add(new Tuple<TableInfo, Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>>>(delete_list.Value.Key, meta_index));
                        }
                        if (trans_count_cache != null && trans_count_cache.Any())
                        {
                            ldb.FlushTransCountCache(trans_count_cache, batch);
                        }
                        foreach (var scache in sc)
                        {
                            ldb.WriteStringCacheToBatch(batch, scache.Value.Value, scache.Value.Key);
                        }
                        foreach (var idata in index_data)
                        {
                            var snapshots_dic = ldb.InsertIndexChanges(idata.Item1, idata.Item2);
                            foreach (var snap in snapshots_dic)
                            {
                                var skey = ldb.MakeSnapshotKey(idata.Item1.TableNumber, idata.Item1.ColumnNumbers[snap.Key]);
                                batch.Put(skey, Encoding.UTF8.GetBytes(snap.Value));
                            }
                        }
                        ldb.leveld_db.Write(batch._writeBatch);
                    }


                    foreach (var cb in _trans_data.Callbacks)
                    {
                        cb(null);
                    }
                }
                catch (Exception ex)
                {
                    if (_trans_data != null)
                    {
                        foreach (var cb in _trans_data.Callbacks)
                        {
                            cb(ex.Message);
                        }
                    }
                    throw;
                }
                finally
                {
                    if (lockAcquired)
                    {
                        foreach (var l in _write_locks)
                        {
                            Monitor.Exit(l);
                        }
                    }
                }
            }
            else
            {
                //the old way
                Dictionary<string, int> trans_count_cache = new Dictionary<string, int>();
                var _write_locks = new List<object>();
                try
                {
                    foreach (var l in locks)
                    {
                        var write_lock = ldb.GetTableWriteLock(l);
                        Monitor.Enter(write_lock);
                        _write_locks.Add(write_lock);
                    }

                    using (WriteBatchWithConstraints batch = new WriteBatchWithConstraints())
                    {
                        //string cache
                        var sc = new Dictionary<string, KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>>();
                        var index_data = new List<Tuple<TableInfo, Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>>>>();
                        foreach (var save_list in data_to_save)
                        {
                            if (!sc.ContainsKey(save_list.Value.Key.Name))
                            {
                                sc[save_list.Value.Key.Name] = new KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>(save_list.Value.Key, new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>());
                            }                           
                            Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> meta_index = ldb.BuildMetaOnIndex(save_list.Value.Key);
                            ldb.SaveItems(save_list.Value.Key, save_list.Key, save_list.Value.Value, batch, trans_count_cache, sc[save_list.Value.Key.Name].Value, meta_index, true);
                            index_data.Add(new Tuple<TableInfo, Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>>>(save_list.Value.Key, meta_index));
                        }
                        foreach (var update_list in data_to_update)
                        {
                            
                            foreach (var update_field in update_list.Value)
                            {
                                if (!sc.ContainsKey(update_field.Key.TableInfo.Name))
                                {
                                    sc[update_field.Key.TableInfo.Name] = new KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>(update_field.Key.TableInfo, new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>());
                                }                                
                                Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> meta_index = ldb.BuildMetaOnIndex(update_field.Key.TableInfo);
                                ldb.UpdateBatch(update_field.Key, update_field.Value, update_field.Key.TableInfo, batch, sc[update_field.Key.TableInfo.Name].Value, meta_index);
                                var snapshots_dic = ldb.InsertIndexChanges(update_field.Key.TableInfo, meta_index);
                                index_data.Add(new Tuple<TableInfo, Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>>>(update_field.Key.TableInfo, meta_index));
                            }

                        }                        
                        foreach (var delete_list in data_to_delete)
                        {
                            if (!sc.ContainsKey(delete_list.Value.Key.Name))
                            {
                                sc[delete_list.Value.Key.Name] = new KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>(delete_list.Value.Key, new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>());
                            }
                            Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> meta_index = ldb.BuildMetaOnIndex(delete_list.Value.Key);
                            ldb.DeleteBatch(delete_list.Value.Value, delete_list.Value.Key, batch, trans_count_cache, sc[delete_list.Value.Key.Name].Value, meta_index);
                            index_data.Add(new Tuple<TableInfo, Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>>>(delete_list.Value.Key, meta_index));
                        }
                        if (trans_count_cache != null && trans_count_cache.Any())
                        {
                            ldb.FlushTransCountCache(trans_count_cache, batch);
                        }
                        foreach (var scache in sc)
                        {
                            ldb.WriteStringCacheToBatch(batch, scache.Value.Value, scache.Value.Key);
                        }
                        foreach (var idata in index_data)
                        {
                            var snapshots_dic = ldb.InsertIndexChanges(idata.Item1, idata.Item2);
                            foreach (var snap in snapshots_dic)
                            {
                                var skey = ldb.MakeSnapshotKey(idata.Item1.TableNumber, idata.Item1.ColumnNumbers[snap.Key]);
                                batch.Put(skey, Encoding.UTF8.GetBytes(snap.Value));
                            }
                        }
                        ldb.leveld_db.Write(batch._writeBatch);
                    }
                }
                finally
                {
                    foreach (var l in _write_locks)
                    {
                        Monitor.Exit(l);
                    }
                }
            }

        }
        public void Dispose()
        {
            data_to_save = null;
            data_to_update = null;
            data_to_delete = null;
        }
    }
}
