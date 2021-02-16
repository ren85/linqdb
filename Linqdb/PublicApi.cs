using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using LinqDbInternal;
using System.Linq.Expressions;
using ServerSharedData;
using System.Threading;
using RocksDbSharp;

namespace LinqDb
{
    public static class LinqdbExtensionMethods
    {
        public static int CountDistinct<TKey, TValue, TProp>(this IGrouping<TKey, TValue> grouping, Expression<Func<TValue, TProp>> keySelector)
        {
            return 0;
        }
    }
    public class LinqdbTransaction : IDisposable
    {
        public LinqdbTransactionInternal _internal { get; set; }

        public LinqdbTransaction()
        {
            _internal = new LinqdbTransactionInternal();
        }
        public void Dispose()
        {
            _internal.Dispose();
        }
        /// <summary>
        ///  Writes changes to disk.
        /// </summary>
        public void Commit()
        {
            _internal.Commit();
        }
    }
    public class Db
    {
        public void _InternalBuildIndexesOnStart()
        {
            _db.ServerBuildIndexesOnStart();
        }
        public ServerResult _Internal_server_execute(byte[] input)
        {
            if (input[0] != (byte)CommandType.Transaction)
            {
                var command = CommandHelper.GetCommands(input);
                return _db.Execute(command, null, null, null);
            }
            else
            {
                var commands = new List<Command>();
                int c_count = BitConverter.ToInt32(new byte[4] { input[input.Length - 4], input[input.Length - 3], input[input.Length - 2], input[input.Length - 1] }, 0);
                int current = 1;
                for (int i = 0; i < c_count; i++)
                {
                    int c_length = BitConverter.ToInt32(new byte[4] { input[current], input[current+1], input[current+2], input[current+3] }, 0);
                    current += 4;
                    byte[] c_data = new byte[c_length];
                    for (int j = 0; j < c_length; j++, current++)
                    {
                        c_data[j] = input[current];
                    }
                    var command = CommandHelper.GetCommands(c_data);
                    commands.Add(command);
                }
                if (!CommandHelper.CanWrite(commands.First().User, commands.First().Pass))
                {
                    throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                }

                var locks = new List<string>();
                locks = commands.Select(f => f.TableName).Distinct().OrderBy(f => f).ToList(); //ordering avoids deadlocks


                var key = locks.Aggregate((a, b) => a + "|" + b);
                var ids = new Dictionary<string, HashSet<int>>();

                foreach (var comm in commands)
                {
                    if (!ids.ContainsKey(comm.TableName))
                    {
                        ids[comm.TableName] = new HashSet<int>();
                    }
                    foreach (var inst in comm.Commands)
                    {
                        if (inst.Type == "save")
                        {
                            foreach (var item in inst.Data)
                            {
                                var id = BitConverter.ToInt32(item.Bytes[comm.TableDefOrder["Id"]], 0);
                                ids[comm.TableName].Add(id);
                            }
                        }
                        if (inst.Type == "update")
                        {
                            //if (inst.UpdateData.Keys == null)
                            //{
                            //    Console.WriteLine("bad 1 " + comm.TableName + " " + key);
                            //}
                            ids[comm.TableName].UnionWith(inst.UpdateData.Keys);
                        }
                        if (inst.Type == "delete")
                        {
                            //if (inst.DeleteIds == null)
                            //{
                            //    Console.WriteLine("bad 2 " + comm.TableName + " " + key);
                            //}
                            ids[comm.TableName].UnionWith(inst.DeleteIds);
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
                        ModifyBatchTransaction._trans_batch[key].commands = new List<Command>(commands);
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
                                //if (ids[tids.Key] == null)
                                //{
                                //    Console.WriteLine("bad 3 " + tids.Key + " " + key);
                                //}
                                tids.Value.UnionWith(ids[tids.Key]);
                            }
                            foreach (var cm in commands)
                            {
                                var fcm = cm.Commands.First();
                                var cms = ModifyBatchTransaction._trans_batch[key].commands;
                                if (fcm.Type == "save")
                                {
                                    var exist = cms.FirstOrDefault(f => f.TableName == cm.TableName && f.Commands.First().Type == "save");
                                    if (exist == null)
                                    {
                                        cms.Add(cm);
                                    }
                                    else
                                    {
                                        exist.Commands.First().Data.AddRange(fcm.Data);
                                    }
                                }
                                if (fcm.Type == "update")
                                {
                                    var exist = cms.FirstOrDefault(f => f.TableName == cm.TableName && f.Commands.First().Type == "update");
                                    if (exist == null)
                                    {
                                        cms.Add(cm);
                                    }
                                    else
                                    {
                                        foreach (var upd in fcm.UpdateData)
                                        {
                                            exist.Commands.First().UpdateData[upd.Key] = upd.Value;
                                        }
                                    }
                                }
                                if (fcm.Type == "delete")
                                {
                                    var exist = cms.FirstOrDefault(f => f.TableName == cm.TableName && f.Commands.First().Type == "delete");
                                    if (exist == null)
                                    {
                                        cms.Add(cm);
                                    }
                                    else
                                    {
                                        //if (fcm.DeleteIds == null)
                                        //{
                                        //    Console.WriteLine("bad 4");
                                        //}
                                        exist.Commands.First().DeleteIds.UnionWith(fcm.DeleteIds);
                                    }
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
                            var _write_lock = _db.GetTableWriteLock(flock);
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
                                        var write_lock = _db.GetTableWriteLock(l);
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
                                return new ServerResult();
                            }
                        }

                        //not done, but have write lock for the table
                        lock (ilock)
                        {
                            _trans_data = ModifyBatchTransaction._trans_batch[key];
                            var oval = new TransBatchData();
                            ModifyBatchTransaction._trans_batch.TryRemove(key, out oval);
                        }

                        var sc = new Dictionary<string, KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>>();
                        using (WriteBatchWithConstraints batch = new WriteBatchWithConstraints())
                        {
                            foreach (var cm in _trans_data.commands)
                            {
                                _db.Execute(cm, batch, trans_count_cache, sc);
                            }
                            if (trans_count_cache != null && trans_count_cache.Any())
                            {
                                _db.FlushTransCountCache(trans_count_cache, batch);
                            }
                            foreach (var scache in sc)
                            {
                                _db.WriteStringCacheToBatch(batch, scache.Value.Value, scache.Value.Key, null);
                            }
                            _db.leveld_db.Write(batch._writeBatch);
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
                    return new ServerResult();
                }
                else
                {
                    //the old way
                    var _write_locks = new List<object>();
                    try
                    {
                        foreach (var l in locks)
                        {
                            var write_lock = _db.GetTableWriteLock(l);
                            Monitor.Enter(write_lock);
                            _write_locks.Add(write_lock);
                        }
                        Dictionary<string, int> trans_count_cache = new Dictionary<string, int>();
                        var sc = new Dictionary<string, KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>>();
                        using (WriteBatchWithConstraints batch = new WriteBatchWithConstraints())
                        {
                            foreach (var cm in commands)
                            {
                                _db.Execute(cm, batch, trans_count_cache, sc);
                            }
                            if (trans_count_cache != null && trans_count_cache.Any())
                            {
                                _db.FlushTransCountCache(trans_count_cache, batch);
                            }
                            foreach (var scache in sc)
                            {
                                _db.WriteStringCacheToBatch(batch, scache.Value.Value, scache.Value.Key, null);
                            }
                            _db.leveld_db.Write(batch._writeBatch);
                        }
                    }
                    finally
                    {
                        foreach (var l in _write_locks)
                        {
                            Monitor.Exit(l);
                        }
                    }
                    return new ServerResult();
                }
            }
        }
        Ldb _db { get; set; }

        public RocksDb _internal_data_db
        {
            get
            {
                return _db.leveld_db;
            }

        }
        private Db() { }
        /// <summary>
        ///  Initialize database. Required once per application lifetime.
        /// </summary>
        public Db(string path, bool delete_indexes = true)
        {
            _db = new Ldb(path, delete_indexes);
        }
        /// <summary>
        ///  Get list of tables.
        /// </summary>
        public List<string> GetTables()
        {
            return _db.GetTables();
        }
        /// <summary>
        ///  Gets definition of a table.
        /// </summary>
        public string GetTableDefinition(string name)
        {
            return _db.GetTableDefinition(name);
        }
        /// <summary>
        ///  Get existing indexes.
        /// </summary>
        public List<string> GetExistingIndexes()
        {
            var res = _db.GetExistingIndexes() + "";
            return res.Split("|".ToCharArray(), StringSplitOptions.RemoveEmptyEntries).ToList();
        }
        /// <summary>
        ///  Indicates which table operation is to be performed on.
        /// </summary>
        public ILinqDbQueryable<T> Table<T>() where T : new()
        {
            var _inter =_db.Table<T>();
            return new ILinqDbQueryable<T>() { _internal = _inter };
        }
        /// <summary>
        ///  Indicates which table operation is to be performed on. Modification operations are performed in a given transaction.
        /// </summary>
        public ILinqDbQueryable<T> Table<T>(LinqdbTransaction transaction) where T : new()
        {
            transaction._internal.ldb = _db;
            var _inter = _db.Table<T>(transaction._internal);
            return new ILinqDbQueryable<T>() { _internal = _inter };
        }
        /// <summary>
        ///  Live replication of the database. If 'path' exists it will be deleted. Works best when database is least used.
        /// </summary>
        public void Replicate(string path)
        {
            _db.Replicate(path);
        }
        /// <summary>
        ///  Dispose database at the end of application lifetime.
        /// </summary>
        public void Dispose()
        {
            _db.Dispose();
        }
    }
    public class SelectedIds
    {
        public List<int> Ids { get; set; }
        public bool AllIds { get; set; }
    }

    public class ILinqDbQueryable<T> where T : new()
    {
        public IDbQueryable<T> _internal { get; set; }
        /// <summary>
        ///  Saves new item if Id = 0 and assigns new Id. Updates existing item if Id is existing's item's Id.
        /// </summary>
        public int Save(T item)
        {
            return _internal._db.Save(item, _internal.LDBTransaction);
        }
        /// <summary>
        ///  Saves any amount of items non-atomically, i.e. if it fails in the middle some items will be saved and some won't be.
        /// </summary>
        public void SaveNonAtomically(List<T> items, int batchSize = 5000)
        {
            if (items == null || !items.Any())
            {
                return;
            }
            if (_internal.LDBTransaction != null)
            {
                throw new LinqDbException("Non-atomic save can't be used in a transaction");
            }
            if (batchSize <= 0)
            {
                throw new LinqDbException("batchSize must be positive number");
            }
            for (int i = 0; ; i++)
            {
                var res = items.Skip(i * batchSize).Take(batchSize).ToList();
                if (!res.Any())
                {
                    return;
                }
                _internal._db.SaveBatch(res, _internal.LDBTransaction);
            }            
        }
        /// <summary>
        ///  Same as Save but more efficient.
        /// </summary>
        public void SaveBatch(List<T> items)
        {
            if (items == null || !items.Any())
            {
                return;
            }
            _internal._db.SaveBatch(items, _internal.LDBTransaction);
        }
        /// <summary>
        ///  Record count.
        /// </summary>
        public int Count()
        {
            return _internal._db.Count<T>(_internal.LDBTree);
        }
        /// <summary>
        ///  Get selected ids without having to select them (more efficiently).
        /// </summary>
        public SelectedIds GetIds()
        {
            var res = _internal._db.GetIds<T>(_internal.LDBTree);
            return new SelectedIds()
            {
                Ids = res.Item1,
                AllIds = res.Item2
            };
        }
        /// <summary>
        ///  Last search step. To be used in step-search.
        /// </summary>
        public int LastStep()
        {
            return _internal._db.GetLastStep<T>(_internal.LDBTree);
        }
        /// <summary>
        ///  Deletes item with given Id.
        /// </summary>
        public void Delete(int id)
        {
            _internal._db.Delete<T>(new HashSet<int>() { id }, _internal.LDBTransaction);
        }
        /// <summary>
        ///  Deletes items with given Ids.
        /// </summary>
        public void Delete(HashSet<int> ids)
        {
            if (ids == null || !ids.Any())
            {
                return;
            }
            _internal._db.Delete<T>(ids, _internal.LDBTransaction);
        }
        /// <summary>
        ///  Deletes any amount of items non-atomically, i.e. if it fails in the middle some items will be deleted and some won't be.
        /// </summary>
        public void DeleteNonAtomically(HashSet<int> ids, int batchSize = 5000)
        {
            if (ids == null || !ids.Any())
            {
                return;
            }
            if (_internal.LDBTransaction != null)
            {
                throw new LinqDbException("Non-atomic delete can't be used in a transaction");
            }
            if (batchSize <= 0)
            {
                throw new LinqDbException("batchSize must be positive number");
            }
            var list = ids.ToList();
            for (int i = 0; ; i++)
            {
                var res = list.Skip(i * batchSize).Take(batchSize).ToList();
                if (!res.Any())
                {
                    return;
                }
                _internal._db.Delete<T>(new HashSet<int>(res), _internal.LDBTransaction);
            }
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, int id, int? value)
        {
            _internal._db.Update(keySelector, new Dictionary<int, int?>() { { id, value } }, _internal.LDBTransaction);
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, int id, int value)
        {
            _internal._db.Update(keySelector, new Dictionary<int, int?>() { { id, value } }, _internal.LDBTransaction);
        }
        /// <summary>
        ///  Updates item's fields with supplied values. Item are identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, int?> values)
        {
            if (values == null || !values.Any())
            {
                return;
            }
            _internal._db.Update(keySelector, values, _internal.LDBTransaction);
        }

        /// <summary>
        ///  Updates any amount of items non-atomically, i.e. if it fails in the middle some items will be updated and some won't be.
        /// </summary>
        public void UpdateNonAtomically<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, int?> values, int batchSize = 5000)
        {
            if (values == null || !values.Any())
            {
                return;
            }
            if (_internal.LDBTransaction != null)
            {
                throw new LinqDbException("Non-atomic update can't be used in a transaction");
            }
            if (batchSize <= 0)
            {
                throw new LinqDbException("batchSize must be positive number");
            }
            for (int i = 0; ; i++)
            {
                var res = values.Skip(i * batchSize).Take(batchSize).ToDictionary(f => f.Key, f => f.Value);
                if (!res.Any())
                {
                    return;
                }
                _internal._db.Update(keySelector, res, _internal.LDBTransaction);
            }
        }
        /// <summary>
        ///  Updates item's fields with supplied values. Item are identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, int> values)
        {
            if (values == null || !values.Any())
            {
                return;
            }
            _internal._db.Update(keySelector, values, _internal.LDBTransaction);
        }
        /// <summary>
        ///  Updates any amount of items non-atomically, i.e. if it fails in the middle some items will be updated and some won't be.
        /// </summary>
        public void UpdateNonAtomically<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, int> values, int batchSize = 5000)
        {
            if (values == null || !values.Any())
            {
                return;
            }
            if (_internal.LDBTransaction != null)
            {
                throw new LinqDbException("Non-atomic update can't be used in a transaction");
            }
            if (batchSize <= 0)
            {
                throw new LinqDbException("batchSize must be positive number");
            }
            for (int i = 0; ; i++)
            {
                var res = values.Skip(i * batchSize).Take(batchSize).ToDictionary(f => f.Key, f => f.Value);
                if (!res.Any())
                {
                    return;
                }
                _internal._db.Update(keySelector, res, _internal.LDBTransaction);
            }
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, int id, double? value)
        {
            _internal._db.Update(keySelector, new Dictionary<int, double?>() { { id, value } }, _internal.LDBTransaction);
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, int id, double value)
        {
            _internal._db.Update(keySelector, new Dictionary<int, double?>() { { id, value } }, _internal.LDBTransaction);
        }
        /// <summary>
        ///  Updates item's fields with supplied values. Item are identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, double?> values)
        {
            if (values == null || !values.Any())
            {
                return;
            }
            _internal._db.Update(keySelector, values, _internal.LDBTransaction);
        }
        /// <summary>
        ///  Updates any amount of items non-atomically, i.e. if it fails in the middle some items will be updated and some won't be.
        /// </summary>
        public void UpdateNonAtomically<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, double?> values, int batchSize = 5000)
        {
            if (values == null || !values.Any())
            {
                return;
            }
            if (_internal.LDBTransaction != null)
            {
                throw new LinqDbException("Non-atomic update can't be used in a transaction");
            }
            if (batchSize <= 0)
            {
                throw new LinqDbException("batchSize must be positive number");
            }
            for (int i = 0; ; i++)
            {
                var res = values.Skip(i * batchSize).Take(batchSize).ToDictionary(f => f.Key, f => f.Value);
                if (!res.Any())
                {
                    return;
                }
                _internal._db.Update(keySelector, res, _internal.LDBTransaction);
            }
        }
        /// <summary>
        ///  Updates item's fields with supplied values. Item are identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, double> values)
        {
            if (values == null || !values.Any())
            {
                return;
            }
            _internal._db.Update(keySelector, values, _internal.LDBTransaction);
        }
        /// <summary>
        ///  Updates any amount of items non-atomically, i.e. if it fails in the middle some items will be updated and some won't be.
        /// </summary>
        public void UpdateNonAtomically<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, double> values, int batchSize = 5000)
        {
            if (values == null || !values.Any())
            {
                return;
            }
            if (_internal.LDBTransaction != null)
            {
                throw new LinqDbException("Non-atomic update can't be used in a transaction");
            }
            if (batchSize <= 0)
            {
                throw new LinqDbException("batchSize must be positive number");
            }
            for (int i = 0; ; i++)
            {
                var res = values.Skip(i * batchSize).Take(batchSize).ToDictionary(f => f.Key, f => f.Value);
                if (!res.Any())
                {
                    return;
                }
                _internal._db.Update(keySelector, res, _internal.LDBTransaction);
            }
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, int id, DateTime? value)
        {
            _internal._db.Update(keySelector, new Dictionary<int, DateTime?>() { { id, value } }, _internal.LDBTransaction);
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, int id, DateTime value)
        {
            _internal._db.Update(keySelector, new Dictionary<int, DateTime?>() { { id, value } }, _internal.LDBTransaction);
        }
        /// <summary>
        ///  Updates item's fields with supplied values. Item are identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, DateTime?> values)
        {
            if (values == null || !values.Any())
            {
                return;
            }
            _internal._db.Update(keySelector, values, _internal.LDBTransaction);
        }
        /// <summary>
        ///  Updates any amount of items non-atomically, i.e. if it fails in the middle some items will be updated and some won't be.
        /// </summary>
        public void UpdateNonAtomically<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, DateTime?> values, int batchSize = 5000)
        {
            if (values == null || !values.Any())
            {
                return;
            }
            if (_internal.LDBTransaction != null)
            {
                throw new LinqDbException("Non-atomic update can't be used in a transaction");
            }
            if (batchSize <= 0)
            {
                throw new LinqDbException("batchSize must be positive number");
            }
            for (int i = 0; ; i++)
            {
                var res = values.Skip(i * batchSize).Take(batchSize).ToDictionary(f => f.Key, f => f.Value);
                if (!res.Any())
                {
                    return;
                }
                _internal._db.Update(keySelector, res, _internal.LDBTransaction);
            }
        }
        /// <summary>
        ///  Updates item's fields with supplied values. Item are identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, DateTime> values)
        {
            if (values == null || !values.Any())
            {
                return;
            }
            _internal._db.Update(keySelector, values, _internal.LDBTransaction);
        }
        /// <summary>
        ///  Updates any amount of items non-atomically, i.e. if it fails in the middle some items will be updated and some won't be.
        /// </summary>
        public void UpdateNonAtomically<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, DateTime> values, int batchSize = 5000)
        {
            if (values == null || !values.Any())
            {
                return;
            }
            if (_internal.LDBTransaction != null)
            {
                throw new LinqDbException("Non-atomic update can't be used in a transaction");
            }
            if (batchSize <= 0)
            {
                throw new LinqDbException("batchSize must be positive number");
            }
            for (int i = 0; ; i++)
            {
                var res = values.Skip(i * batchSize).Take(batchSize).ToDictionary(f => f.Key, f => f.Value);
                if (!res.Any())
                {
                    return;
                }
                _internal._db.Update(keySelector, res, _internal.LDBTransaction);
            }
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, int id, byte[] value)
        {
            _internal._db.Update(keySelector, new Dictionary<int, byte[]>() { { id, value } }, _internal.LDBTransaction);
        }
        /// <summary>
        ///  Updates item's fields with supplied values. Item are identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, byte[]> values)
        {
            if (values == null || !values.Any())
            {
                return;
            }
            _internal._db.Update(keySelector, values, _internal.LDBTransaction);
        }
        /// <summary>
        ///  Updates any amount of items non-atomically, i.e. if it fails in the middle some items will be updated and some won't be.
        /// </summary>
        public void UpdateNonAtomically<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, byte[]> values, int batchSize = 5000)
        {
            if (values == null || !values.Any())
            {
                return;
            }
            if (_internal.LDBTransaction != null)
            {
                throw new LinqDbException("Non-atomic update can't be used in a transaction");
            }
            if (batchSize <= 0)
            {
                throw new LinqDbException("batchSize must be positive number");
            }
            for (int i = 0; ; i++)
            {
                var res = values.Skip(i * batchSize).Take(batchSize).ToDictionary(f => f.Key, f => f.Value);
                if (!res.Any())
                {
                    return;
                }
                _internal._db.Update(keySelector, res, _internal.LDBTransaction);
            }
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, int id, string value)
        {
            _internal._db.Update(keySelector, new Dictionary<int, string>() { { id, value } }, _internal.LDBTransaction);
        }
        /// <summary>
        ///  Updates item's fields with supplied values. Item are identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, string> values)
        {
            if (values == null || !values.Any())
            {
                return;
            }
            _internal._db.Update(keySelector, values, _internal.LDBTransaction);
        }
        /// <summary>
        ///  Updates any amount of items non-atomically, i.e. if it fails in the middle some items will be updated and some won't be.
        /// </summary>
        public void UpdateNonAtomically<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<int, string> values, int batchSize = 5000)
        {
            if (values == null || !values.Any())
            {
                return;
            }
            if (_internal.LDBTransaction != null)
            {
                throw new LinqDbException("Non-atomic update can't be used in a transaction");
            }
            if (batchSize <= 0)
            {
                throw new LinqDbException("batchSize must be positive number");
            }
            for (int i = 0; ; i++)
            {
                var res = values.Skip(i * batchSize).Take(batchSize).ToDictionary(f => f.Key, f => f.Value);
                if (!res.Any())
                {
                    return;
                }
                _internal._db.Update(keySelector, res, _internal.LDBTransaction);
            }
        }
        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient.
        /// </summary>
        public List<R> Select<R>(Expression<Func<T, R>> predicate)
        {
            var statistics = new LinqdbSelectStatisticsInternal();
            return _internal._db.Select(_internal.LDBTree, predicate, statistics);
        }

        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient. Statistics is used as out parameter.
        /// </summary>
        public List<R> Select<R>(Expression<Func<T, R>> predicate, LinqdbSelectStatistics statistics)
        {
            var internalStatistics = new LinqdbSelectStatisticsInternal();
            var res = _internal._db.Select(_internal.LDBTree, predicate, internalStatistics);
            statistics.Total = internalStatistics.Total;
            statistics.SearchedPercentile = internalStatistics.SearchedPercentile;
            return res;
        }

        /// <summary>
        ///  Selects anonymous type using result entities. Selects in iterations to avoid select limit. Drawback is that it may return data not matching conditions if data changed after select began.
        /// </summary>
        public List<R> SelectNonAtomically<R>(Expression<Func<T, R>> predicate, int batchSize = 3000)
        {
            var results = new List<R>();
            var ids = _internal._db.GetIds<T>(_internal.LDBTree).Item1;
            for (int i = 0; ; i++)
            {
                var cids = ids.Skip(i * batchSize).Take(batchSize).Select(f => (int?)f).ToList();
                if (!cids.Any())
                {
                    return results;   
                }
                _internal.LDBTree = new QueryTree();
                _internal._db.Intersect<T, int>(_internal, null, new HashSet<int?>(cids));
                var statistics = new LinqdbSelectStatisticsInternal();
                var res = _internal._db.Select(_internal.LDBTree, predicate, statistics);
                results.AddRange(res);
            }            
        }

        /// <summary>
        ///  Selects entire entities using result set.
        /// </summary>
        public List<T> SelectEntity()
        {
            var statistics = new LinqdbSelectStatisticsInternal();
            return _internal._db.SelectEntity<T>(_internal.LDBTree, statistics);
        }
        /// <summary>
        ///  Selects entire entities using result set. Statistics is used as out parameter.
        /// </summary>
        public List<T> SelectEntity(LinqdbSelectStatistics statistics)
        {
            var stats = new LinqdbSelectStatisticsInternal();
            var res = _internal._db.SelectEntity<T>(_internal.LDBTree, stats);
            statistics.Total = stats.Total;
            statistics.SearchedPercentile = stats.SearchedPercentile;
            return res;
        }

        /// <summary>
        ///  Selects entire entities using result set. Selects in iterations to avoid select limit. Drawback is that it may return data not matching conditions if data changed after select began.
        /// </summary>
        public List<T> SelectEntityNonAtomically(int batchSize = 3000)
        {
            var results = new List<T>();
            var ids = _internal._db.GetIds<T>(_internal.LDBTree).Item1;
            for (int i = 0; ; i++)
            {
                var cids = ids.Skip(i * batchSize).Take(batchSize).Select(f => (int?)f).ToList();
                if (!cids.Any())
                {
                    return results;
                }
                _internal.LDBTree = new QueryTree();
                _internal._db.Intersect<T, int>(_internal, null, new HashSet<int?>(cids));
                var statistics = new LinqdbSelectStatisticsInternal();
                var res = _internal._db.SelectEntity<T>(_internal.LDBTree, statistics);
                results.AddRange(res);
            }
        }
        /// <summary>
        ///  Atomically increment (decrement) value (https://en.wikipedia.org/wiki/Linearizability). Must be used after (and only) .Where, which identifies single record to update.
        /// </summary>
        //public void AtomicIncrement<TKey>(Expression<Func<T, TKey>> keySelector, int value, T new_item_if_doesnt_exist)
        //{
        //    if (_internal.LDBTransaction != null)
        //    {
        //        throw new LinqDbException("Linqdb: transactions are not supported with AtomicIncrement.");
        //    }
        //    _internal._db.AtomicIncrement(_internal, keySelector, value, new_item_if_doesnt_exist);
        //}
        /// <summary>
        ///  Atomically increment (decrement) value (https://en.wikipedia.org/wiki/Linearizability). Returns old value or null if item was created. Must be used after (and only) .Where, which identifies single record to update.
        /// </summary>
        public int? AtomicIncrement<TKey>(Expression<Func<T, TKey>> keySelector, int value, T new_item_if_doesnt_exist, int? only_if_old_equal)
        {
            if (_internal.LDBTransaction != null)
            {
                throw new LinqDbException("Linqdb: transactions are not supported with AtomicIncrement.");
            }
            var val = _internal._db.AtomicIncrement(_internal, keySelector, value, new_item_if_doesnt_exist, only_if_old_equal);
            return val;
        }
        /// <summary>
        ///  Atomically increment (decrement) value (https://en.wikipedia.org/wiki/Linearizability). Must be used after (and only) .Where, which identifies single record to update.
        /// </summary>
        public void AtomicIncrement2Props<TKey1, TKey2>(Expression<Func<T, TKey1>> keySelector1, Expression<Func<T, TKey2>> keySelector2, int value1, int value2, T new_item_if_doesnt_exist)
        {
            if (_internal.LDBTransaction != null)
            {
                throw new LinqDbException("Linqdb: transactions are not supported with AtomicIncrement.");
            }
            _internal._db.AtomicIncremen2Props(_internal, keySelector1, keySelector2, value1, value2, new_item_if_doesnt_exist);
        }
        /// <summary>
        ///  Applies between condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Between<TKey>(Expression<Func<T, TKey>> keySelector, int from, int to, BetweenBoundaries boundaries = BetweenBoundaries.BothInclusive)
        {
            _internal._db.Between(_internal, keySelector, (double)from, (double)to, (BetweenBoundariesInternal)(int)boundaries);
            return this;
        }
        /// <summary>
        ///  Applies between condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Between<TKey>(Expression<Func<T, TKey>> keySelector, double from, double to, BetweenBoundaries boundaries = BetweenBoundaries.BothInclusive)
        {
            _internal._db.Between(_internal, keySelector, from, to, (BetweenBoundariesInternal)(int)boundaries);
            return this;
        }
        /// <summary>
        ///  Applies between condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Between<TKey>(Expression<Func<T, TKey>> keySelector, DateTime from, DateTime to, BetweenBoundaries boundaries = BetweenBoundaries.BothInclusive)
        {
            _internal._db.Between(_internal, keySelector, (from - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds, (to - new DateTime(0001, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds, (BetweenBoundariesInternal)(int)boundaries);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<int?> set)
        {
            _internal._db.Intersect(_internal, keySelector, set);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<int> set)
        {
            _internal._db.Intersect(_internal, keySelector, set);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<double?> set)
        {
            _internal._db.Intersect(_internal, keySelector, set);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<double> set)
        {
            _internal._db.Intersect(_internal, keySelector, set);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<DateTime?> set)
        {
            _internal._db.Intersect(_internal, keySelector, set);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<DateTime> set)
        {
            _internal._db.Intersect(_internal, keySelector, set);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set..
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<string> set)
        {
            _internal._db.Intersect(_internal, keySelector, set);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<int> set)
        {
            _internal._db.Intersect(_internal, keySelector, new HashSet<int?>(set.Select(f => (int?)f)));
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<double> set)
        {
            _internal._db.Intersect(_internal, keySelector, new HashSet<double?>(set.Select(f => (double?)f)));
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<DateTime> set)
        {
            _internal._db.Intersect(_internal, keySelector, new HashSet<DateTime?>(set.Select(f => (DateTime?)f)));
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<string> set)
        {
            _internal._db.Intersect(_internal, keySelector, new HashSet<string>(set.Select(f => f)));
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<int?> set)
        {
            _internal._db.Intersect(_internal, keySelector, new HashSet<int?>(set));
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<double?> set)
        {
            _internal._db.Intersect(_internal, keySelector, new HashSet<double?>(set));
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<DateTime?> set)
        {
            _internal._db.Intersect(_internal, keySelector, new HashSet<DateTime?>(set));
            return this;
        }
        /// <summary>
        ///  Full text search on a column. 
        /// </summary>
        public ILinqDbQueryable<T> Search<TKey>(Expression<Func<T, TKey>> keySelector, string search_query, int? start_step = null, int? steps = null)
        {
            _internal._db.Search(_internal, keySelector, search_query, false, false, 0, start_step, steps);
            return this;
        }
        /// <summary>
        ///  Full text search on a column matching beginning of a word.
        /// </summary>
        public ILinqDbQueryable<T> SearchPartial<TKey>(Expression<Func<T, TKey>> keySelector, string search_query)
        {
            _internal._db.Search(_internal, keySelector, search_query, true, false, 0, null, null);
            return this;
        }
        /// <summary>
        ///  Full text search on a column, limited by time period.
        /// </summary>
        public ILinqDbQueryable<T> SearchTimeLimited<TKey>(Expression<Func<T, TKey>> keySelector, string search_query, int maxSearchTimeInMs)
        {
            _internal._db.Search(_internal, keySelector, search_query, false, true, maxSearchTimeInMs, null, null);
            return this;
        }
        /// <summary>
        ///  Applies where condition to the result set. 
        /// </summary>
        public ILinqDbQueryable<T> Where(Expression<Func<T, bool>> predicate)
        {
            _internal._db.Where(_internal, predicate);
            return this;
        }
        /// <summary>
        ///  Applies or condition to the neighbouring statements.
        /// </summary>
        public ILinqDbQueryable<T> Or()
        {
            Ldb_ext.Or(_internal);
            return this;
        }
        /// <summary>
        ///  Orders data by ascending values.
        /// </summary>
        public ILinqDbOrderedQueryable<T> OrderBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            var _inter = _internal._db.OrderBy<T, TKey>(_internal, keySelector);
            return new ILinqDbOrderedQueryable<T>() { _internal = _inter };
        }
        /// <summary>
        ///  Orders data by descending values.
        /// </summary>
        public ILinqDbOrderedQueryable<T> OrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            var _inter = _internal._db.OrderByDescending<T, TKey>(_internal, keySelector);
            return new ILinqDbOrderedQueryable<T>() { _internal = _inter };
        }

        /// <summary>
        ///  Groups data by key. Group index must be created in advance.
        /// </summary>
        public ILinqDbGroupedQueryable<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            var _inter = _internal._db.GroupBy<T, TKey>(_internal, keySelector);
            return new ILinqDbGroupedQueryable<T, TKey>() { _internal = _inter };
        }

        /// <summary>
        ///  Creates in-memory group-by index, parameter is property to be aggregated.
        /// </summary>
        public void CreateGroupByMemoryIndex<TKey1, TKey2>(Expression<Func<T, TKey1>> groupPropertySelector, Expression<Func<T, TKey2>> valuePropertySelector)
        {
            _internal._db.CreateGroupByIndex<T, TKey1, TKey2>(groupPropertySelector, valuePropertySelector);
        }
        /// <summary>
        ///  Creates in-memory property index.
        /// </summary>
        public void CreatePropertyMemoryIndex<TKey>(Expression<Func<T, TKey>> valuePropertySelector)
        {
            _internal._db.CreateIndex<T, TKey>(valuePropertySelector);
        }

        /// <summary>
        ///  Removes index from startup creation.
        /// </summary>
        public void RemovePropertyMemoryIndex<TKey>(Expression<Func<T, TKey>> valuePropertySelector)
        {
            _internal._db.RemoveIndex<T, TKey>(valuePropertySelector);
        }

        /// <summary>
        ///  Removes group index from startup creation.
        /// </summary>
        public void RemoveGroupByMemoryIndex<TKey1, TKey2>(Expression<Func<T, TKey1>> groupPropertySelector, Expression<Func<T, TKey2>> valuePropertySelector)
        {
            _internal._db.RemoveGroupByIndex<T, TKey1, TKey2>(groupPropertySelector, valuePropertySelector);
        }

    }

    public class ILinqDbGroupedQueryable<T, TKey> where T : new()
    {
        public IDbGroupedQueryable<T, TKey> _internal { get; set; }

        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient.
        /// </summary>
        public List<R> Select<R>(Expression<Func<IGrouping<TKey, T> , R>> predicate)
        {
            int total;
            return _internal._db.SelectGrouped<TKey, T, R>(_internal.LDBTree, predicate, out total);
        }        
    }

    public class ILinqDbOrderedQueryable<T> where T : new()
    {
        public IDbOrderedQueryable<T> _internal { get; set; }

        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient.
        /// </summary>
        public List<R> Select<R>(Expression<Func<T, R>> predicate)
        {
            var statistics = new LinqdbSelectStatisticsInternal();
            return _internal._db.Select(_internal.LDBTree, predicate, statistics);
        }
        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient. Statistics is used as out parameter.
        /// </summary>
        public List<R> Select<R>(Expression<Func<T, R>> predicate, LinqdbSelectStatistics statistics)
        {
            var internalStatistics = new LinqdbSelectStatisticsInternal();
            var res = _internal._db.Select(_internal.LDBTree, predicate, internalStatistics);
            statistics.Total = internalStatistics.Total;
            statistics.SearchedPercentile = internalStatistics.SearchedPercentile;
            return res;
        }
        /// <summary>
        ///  Selects entire entities using result set.
        /// </summary>
        public List<T> SelectEntity()
        {
            var statistics = new LinqdbSelectStatisticsInternal();
            return _internal._db.SelectEntity<T>(_internal.LDBTree, statistics);
        }
        /// <summary>
        ///  Selects entire entities using result set. Statistics is used as out parameter.
        /// </summary>
        public List<T> SelectEntity(LinqdbSelectStatistics statistics)
        {
            var stats = new LinqdbSelectStatisticsInternal();
            var res = _internal._db.SelectEntity<T>(_internal.LDBTree, stats);
            statistics.Total = stats.Total;
            statistics.SearchedPercentile = stats.SearchedPercentile;
            return res;
        }
        /// <summary>
        ///  Skips some ordered values.
        /// </summary>
        public ILinqDbOrderedQueryable<T> Skip(int count)
        {
            _internal._db.Skip<T>(_internal, count);
            return this;
        }
        /// <summary>
        ///  Takes some ordered values.
        /// </summary>
        public ILinqDbOrderedQueryable<T> Take(int count)
        {
            _internal._db.Take<T>(_internal, count);
            return this;
        }
    }

    public enum BetweenBoundaries : int
    { 
        BothInclusive,
        FromInclusiveToExclusive,
        FromExclusiveToInclusive,
        BothExclusive
    }
}
