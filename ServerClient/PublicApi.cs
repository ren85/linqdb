using LinqDbClientInternal;
using ServerSharedData;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace LinqdbClient
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
        public void Commit()
        {
            _internal.Commit();
        }
    }
    public class Db
    {
        public Ldb _db_internal
        {
            get
            {
                return _db;
            }
        }
        Ldb _db { get; set; }
        string Hostname { get; set; }
        int Port { get; set; }
        private Db() { }
        /// <summary>
        ///  Initialize database. Required once per application lifetime. If user or pass is not supplied, admin-admin is used.
        /// </summary>
        public Db(string db_url, string user = null, string password = null)
        {
            _db = new Ldb();
            if (user == null && password != null)
            {
                password = null;
            }
            if (user != null && password == null)
            {
                user = null;
            }
            if (user == null && password == null)
            {
                user = "admin";
                password = "admin";
            }
            if (!string.IsNullOrEmpty(user) && user.Count() > 120)
            {
                throw new LinqDbException("Linqdb: user length must be less than 120 characters");
            }
            _db.User = user;
            _db.Pass = password;
            if (db_url.Contains(":"))
            {
                var parts = db_url.Split(":".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
                Hostname = parts[0].Trim();
                Port = Convert.ToInt32(parts[1].Trim());
                _db_internal.Sock = new ClientSockets();
                _db_internal.CallServer = ((byte[] f) =>
                {
                    string error = null;
                    var res = _db_internal.Sock.CallServer(f, Hostname, Port, out error);
                    if (res.Count() == 4 && BitConverter.ToInt32(res, 0) == -1)
                    {
                        throw new LinqDbException("Linqdb: socket error: " + error);
                    }
                    return res;
                });
            }
        }
        /// <summary>
        ///  Get list of tables.
        /// </summary>
        public List<string> GetTables()
        {
            Command cm = new Command();
            cm.Type = (int)CommandType.GetAllTables;
            cm.TableName = "";
            cm.User = _db.User;
            cm.Pass = _db.Pass;
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);
            return res_obj.TableInfo.Split("|".ToCharArray(), StringSplitOptions.RemoveEmptyEntries).ToList();
        }
        /// <summary>
        ///  Server's name from config.txt. This is cached field that is set after GetServerStatus().
        /// </summary>
        public string ServerName { get; set; }
        /// <summary>
        ///  Gets server's name and status. Returns after at most 1 second by default. If server doesn't reply in this period it is considered down.
        /// </summary>
        public LinqdbServerStatus GetServerStatus(int timeToWaitInMs = 1000)
        {
            try
            {
                Command cm = new Command();
                cm.Type = (int)CommandType.GetServerName;
                cm.TableName = "";
                cm.User = _db.User;
                cm.Pass = _db.Pass;
                var bcm = CommandHelper.GetBytes(cm);
                byte[] bres = null;
                Task.Run(() =>
                {
                    bres = _db.CallServer(bcm);
                })
                .Wait(timeToWaitInMs);

                if (bres != null)
                {
                    var res_obj = ServerResultHelper.GetServerResult(bres);
                    ServerName = res_obj.TableInfo;
                    return new LinqdbServerStatus()
                    {
                        ServerName = res_obj.TableInfo,
                        IsUp = true
                    };
                }
                else
                {
                    return new LinqdbServerStatus()
                    {
                        IsUp = false,
                        ServerName = ServerName
                    };
                }
            }
            catch (Exception)
            {
                return new LinqdbServerStatus()
                {
                    IsUp = false,
                    ServerName = ServerName
                };
            }
        }
        /// <summary>
        ///  Get existing indexes.
        /// </summary>
        public List<string> GetExistingIndexes()
        {
            Command cm = new Command();
            cm.Type = (int)CommandType.GetAllIndexes;
            cm.TableName = "";
            cm.User = _db.User;
            cm.Pass = _db.Pass;
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);
            if (string.IsNullOrEmpty(res_obj.TableInfo))
            {
                return new List<string>();
            }
            return res_obj.TableInfo.Split("|".ToCharArray(), StringSplitOptions.RemoveEmptyEntries).ToList();
        }
        /// <summary>
        ///  Gets definition of a table.
        /// </summary>
        public string GetTableDefinition(string name)
        {
            Command cm = new Command();
            cm.Type = (int)CommandType.GetGivenTable;
            cm.TableName = name;
            cm.User = _db.User;
            cm.Pass = _db.Pass;
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);
            return res_obj.TableInfo;
        }
        /// <summary>
        ///  Indicates which queue operation is to be performed on.
        /// </summary>
        public ILinqDbQueueQueryable<T> Queue<T>() where T : new()
        {
            var _inter = new IDbQueueQueryable<T>() { Result = new List<ClientResult>(), _db = this._db };
            return new ILinqDbQueueQueryable<T>() { _internal = _inter };
        }
        /// <summary>
        ///  Indicates which table operation is to be performed on.
        /// </summary>
        public ILinqDbQueryable<T> Table<T>() where T : new()
        {
            var _inter = new IDbQueryable<T>() { Result = new List<ClientResult>(), _db = this._db };
            return new ILinqDbQueryable<T>() { _internal = _inter };
        }
        /// <summary>
        ///  Indicates which table operation is to be performed on. Modification operations are performed in a given transaction.
        /// </summary>
        public ILinqDbQueryable<T> Table<T>(LinqdbTransaction transaction) where T : new()
        {
            if (transaction._internal._db == null)
            {
                transaction._internal._db = this._db;
            }
            var _inter = new IDbQueryable<T>() { Result = new List<ClientResult>(), _db = this._db, LDBTransaction = transaction._internal };
            return new ILinqDbQueryable<T>() { _internal = _inter };
        }
        /// <summary>
        ///  Live replication of the database. If 'path' exists it will be deleted. Works best when database is least used.
        /// </summary>
        public void Replicate(string path)
        {
            var result = _db.Replicate(path);
            List<ClientResult> Result = new List<ClientResult>() { result };

            var cm = CommandHelper.GetCommand(Result, new Dictionary<string, string>(), new Dictionary<string, short>(), typeof(object).Name, _db.User, _db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);
        }
        /// <summary>
        /// No operation
        /// </summary>
        public void Dispose()
        {
        }
    }
    public class SelectedIds
    {
        public List<int> Ids { get; set; }
        public bool AllIds { get; set; }
    }

    public class ILinqDbQueueQueryable<T> where T : new()
    {
        public IDbQueueQueryable<T> _internal { get; set; }

        /// <summary>
        ///  Puts item to memory-queue.
        /// </summary>
        public void PutToQueue(T item)
        {
            var result = _internal._db.PutToQueue(item);
            _internal.Result.Add(result);
            Command cm = CommandHelper.GetQueueCommand(_internal.Result, typeof(T).Name, _internal._db.User, _internal._db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _internal._db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);
            if (!string.IsNullOrEmpty(res_obj.ServerError))
            {
                throw new LinqDbException(res_obj.ServerError);
            }
        }
        /// <summary>
        ///  Gets all items from memory-queue
        /// </summary>
        public List<T> GetAllFromQueue()
        {
            var result = _internal._db.GetFromQueue<T>();
            _internal.Result.Add(result);

            var cm = CommandHelper.GetQueueCommand(_internal.Result, typeof(T).Name, _internal._db.User, _internal._db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _internal._db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);
            var res = new List<T>();
            if (res_obj.QueueData == null || !res_obj.QueueData.Any())
            {
                return res;
            }
            int current = 0;
            while (current < res_obj.QueueData.Count())
            {
                var l = BitConverter.ToInt32(new byte[4] { res_obj.QueueData[current], res_obj.QueueData[current + 1], res_obj.QueueData[current + 2], res_obj.QueueData[current + 3] }, 0);
                current += 4;
                var length = Convert.ToInt32(l);
                var item_bytes = new byte[length];
                for (int i = 0; i < length; i++)
                {
                    item_bytes[i] = res_obj.QueueData[current];
                    current++;
                }
                var item = SharedUtils.DeserializeFromBytes<T>(SharedUtils.Decompress(item_bytes));
                res.Add(item);
            }
            return res;
        }
    }
    public class ILinqDbQueryable<T> where T : new()
    {
        public IDbQueryable<T> _internal { get; set; }
        /// <summary>
        ///  Saves new item if Id = 0 and assigns new Id. Updates existing item if Id is existing's item's Id.
        /// </summary>
        public int Save(T item)
        {
            if (_internal.LDBTransaction == null)
            {
                var def = _internal._db.GetTableDefinition<T>();
                var result = _internal._db.Save(item, def.Item1, def.Item2);
                _internal.Result.Add(result);
                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
                int id = res_obj.Ids.First();
                PropertyInfo prop = item.GetType().GetProperty("Id");
                if (prop != null && prop.CanWrite)
                {
                    prop.SetValue(item, id);
                }
                return id;
            }
            else
            {
                var def = _internal._db.GetTableDefinition<T>();
                PropertyInfo prop = item.GetType().GetProperty("Id");
                var old_id = prop.GetValue(item);
                if (old_id == null || (int)old_id == 0)
                {
                    Command ids_cm = new Command();
                    ids_cm.User = _internal._db.User;
                    ids_cm.Pass = _internal._db.Pass;
                    ids_cm.Type = (int)CommandType.GetNewIds;
                    ids_cm.HowManyNewIds = 1;
                    ids_cm.TableName = typeof(T).Name;
                    ids_cm.TableDef = def.Item1;
                    var ids_bcm = CommandHelper.GetBytes(ids_cm);
                    var ids_bres = _internal._db.CallServer(ids_bcm);
                    var ids_res_obj = ServerResultHelper.GetServerResult(ids_bres);
                    old_id = ids_res_obj.Ids[0];
                    prop.SetValue(item, old_id);
                }

                string name = typeof(T).Name;
                if (!_internal.LDBTransaction.defs.ContainsKey(name))
                {
                    _internal.LDBTransaction.defs[name] = def;
                }
                if (_internal.LDBTransaction.data_to_save.ContainsKey(name))
                {
                    _internal.LDBTransaction.data_to_save[name].Add(item);
                }
                else
                {
                    _internal.LDBTransaction.data_to_save[name] = new List<object>() { item };
                }
                return (int)old_id;
            }
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

            var def = _internal._db.GetTableDefinition<T>();
            for (int j = 0; ; j++)
            {
                var res = items.Skip(j * batchSize).Take(batchSize).ToList();
                if (!res.Any())
                {
                    return;
                }

                var result = _internal._db.SaveBatch(res, def.Item1, def.Item2);
                _internal.Result = new List<ClientResult>() { result };
                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);

                for (int i = 0; i < res.Count; i++)
                {
                    PropertyInfo prop = res[i].GetType().GetProperty("Id");
                    if ((int)prop.GetValue(res[i]) == 0)
                    {
                        prop.SetValue(res[i], res_obj.Ids[i]);
                    }
                }

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
            if (_internal.LDBTransaction == null)
            {
                var def = _internal._db.GetTableDefinition<T>();
                var result = _internal._db.SaveBatch(items, def.Item1, def.Item2);
                _internal.Result.Add(result);
                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);

                for (int i = 0; i < items.Count; i++)
                {
                    PropertyInfo prop = items[i].GetType().GetProperty("Id");
                    if ((int)prop.GetValue(items[i]) == 0)
                    {
                        prop.SetValue(items[i], res_obj.Ids[i]);
                    }
                }
            }
            else
            {
                int how_many = 0;
                var zeros = new List<T>();
                var def = _internal._db.GetTableDefinition<T>();
                foreach (var item in items)
                {
                    PropertyInfo prop = item.GetType().GetProperty("Id");
                    int old_id = (int)prop.GetValue(item);
                    if (old_id == 0)
                    {
                        how_many++;
                        zeros.Add(item);
                    }
                }
                if (how_many > 0)
                {
                    Command ids_cm = new Command();
                    ids_cm.User = _internal._db.User;
                    ids_cm.Pass = _internal._db.Pass;
                    ids_cm.Type = (int)CommandType.GetNewIds;
                    ids_cm.HowManyNewIds = how_many;
                    ids_cm.TableName = typeof(T).Name;
                    ids_cm.TableDef = def.Item1;
                    var ids_bcm = CommandHelper.GetBytes(ids_cm);
                    var ids_bres = _internal._db.CallServer(ids_bcm);
                    var ids_res_obj = ServerResultHelper.GetServerResult(ids_bres);
                    for (int i = 0; i < how_many; i++)
                    {
                        PropertyInfo prop = zeros[i].GetType().GetProperty("Id");
                        var new_id = ids_res_obj.Ids[i];
                        prop.SetValue(zeros[i], new_id);
                    }
                }

                string name = typeof(T).Name;
                if (!_internal.LDBTransaction.defs.ContainsKey(name))
                {
                    _internal.LDBTransaction.defs[name] = def;
                }
                if (!_internal.LDBTransaction.data_to_save.ContainsKey(name))
                {
                    _internal.LDBTransaction.data_to_save[name] = new List<object>();
                }
                _internal.LDBTransaction.data_to_save[name].AddRange(items.Cast<object>());
            }
        }
        /// <summary>
        ///  Record count.
        /// </summary>
        public int Count()
        {
            var result = _internal._db.Count<T>();
            _internal.Result.Add(result);
            var def = _internal._db.GetTableDefinition<T>();

            var cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _internal._db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);

            return res_obj.Count;
        }
        /// <summary>
        ///  Get selected ids without having to select them (more efficiently).
        /// </summary>
        public SelectedIds GetIds()
        {
            var result = _internal._db.GetIds<T>();
            _internal.Result.Add(result);
            var def = _internal._db.GetTableDefinition<T>();

            var cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _internal._db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);

            return new SelectedIds() { AllIds = res_obj.AllIds, Ids = res_obj.Ids };
        }
        /// <summary>
        ///  Last search step. To be used in step-search.
        /// </summary>
        public int LastStep()
        {
            var result = _internal._db.GetLastStep<T>();
            _internal.Result.Add(result);
            var def = _internal._db.GetTableDefinition<T>();

            var cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _internal._db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);

            return res_obj.LastStep;
        }
        /// <summary>
        ///  Deletes item with given Id.
        /// </summary>
        public void Delete(int id)
        {
            if (_internal.LDBTransaction == null)
            {
                var result = _internal._db.DeleteBatch(new HashSet<int>() { id });
                _internal.Result.Add(result);

                var def = _internal._db.GetTableDefinition<T>();
                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
            else
            {
                var result = _internal._db.DeleteBatch(new HashSet<int>() { id });
                string name = typeof(T).Name;
                if (!_internal.LDBTransaction.defs.ContainsKey(name))
                {
                    var def = _internal._db.GetTableDefinition<T>();
                    _internal.LDBTransaction.defs[name] = def;
                }
                if (_internal.LDBTransaction.data_to_delete.ContainsKey(name))
                {
                    _internal.LDBTransaction.data_to_delete[name].UnionWith(result.DeleteIds);
                }
                else
                {
                    _internal.LDBTransaction.data_to_delete[name] = result.DeleteIds;
                }
            }
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
            if (_internal.LDBTransaction == null)
            {
                var result = _internal._db.DeleteBatch(ids);
                _internal.Result.Add(result);

                var def = _internal._db.GetTableDefinition<T>();

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
            else
            {
                var result = _internal._db.DeleteBatch(ids);
                string name = typeof(T).Name;
                if (!_internal.LDBTransaction.defs.ContainsKey(name))
                {
                    var def = _internal._db.GetTableDefinition<T>();
                    _internal.LDBTransaction.defs[name] = def;
                }
                if (_internal.LDBTransaction.data_to_delete.ContainsKey(name))
                {
                    _internal.LDBTransaction.data_to_delete[name].UnionWith(result.DeleteIds);
                }
                else
                {
                    _internal.LDBTransaction.data_to_delete[name] = result.DeleteIds;
                }
            }
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
            var def = _internal._db.GetTableDefinition<T>();
            for (int i = 0; ; i++)
            {
                var res = list.Skip(i * batchSize).Take(batchSize).ToList();
                if (!res.Any())
                {
                    return;
                }

                var result = _internal._db.DeleteBatch(new HashSet<int>(res));
                _internal.Result = new List<ClientResult>() { result };

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, int id, int? value)
        {
            var def = _internal._db.GetTableDefinition<T>();
            if (_internal.LDBTransaction == null)
            {
                var result = _internal._db.Update(keySelector, new Dictionary<int, int?>() { { id, value } });
                _internal.Result.Add(result);
                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.int_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }
                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
            else
            {
                var result = _internal._db.Update(keySelector, new Dictionary<int, int?>() { { id, value } });
                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.int_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }
                string name = typeof(T).Name;
                if (!_internal.LDBTransaction.defs.ContainsKey(name))
                {
                    _internal.LDBTransaction.defs[name] = def;
                }
                if (_internal.LDBTransaction.data_to_update.ContainsKey(name))
                {
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
                else
                {
                    _internal.LDBTransaction.data_to_update[name] = new List<KeyValuePair<string, Dictionary<int, byte[]>>>();
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
            }
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, int id, int value)
        {
            var def = _internal._db.GetTableDefinition<T>();

            if (_internal.LDBTransaction == null)
            {
                var result = _internal._db.Update(keySelector, new Dictionary<int, int>() { { id, value } });
                _internal.Result.Add(result);

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.int_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
            else
            {
                var result = _internal._db.Update(keySelector, new Dictionary<int, int>() { { id, value } });

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.int_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                string name = typeof(T).Name;
                if (!_internal.LDBTransaction.defs.ContainsKey(name))
                {
                    _internal.LDBTransaction.defs[name] = def;
                }
                if (_internal.LDBTransaction.data_to_update.ContainsKey(name))
                {
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
                else
                {
                    _internal.LDBTransaction.data_to_update[name] = new List<KeyValuePair<string, Dictionary<int, byte[]>>>();
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
            }
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
            var def = _internal._db.GetTableDefinition<T>();

            if (_internal.LDBTransaction == null)
            {
                var result = _internal._db.Update(keySelector, values);
                _internal.Result.Add(result);

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.int_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
            else
            {
                var result = _internal._db.Update(keySelector, values);
                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.int_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }
                string name = typeof(T).Name;
                if (!_internal.LDBTransaction.defs.ContainsKey(name))
                {
                    _internal.LDBTransaction.defs[name] = def;
                }
                if (_internal.LDBTransaction.data_to_update.ContainsKey(name))
                {
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
                else
                {
                    _internal.LDBTransaction.data_to_update[name] = new List<KeyValuePair<string, Dictionary<int, byte[]>>>();
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
            }
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
            var def = _internal._db.GetTableDefinition<T>();
            for (int i = 0; ; i++)
            {
                var res = values.Skip(i * batchSize).Take(batchSize).ToDictionary(f => f.Key, f => f.Value);
                if (!res.Any())
                {
                    return;
                }

                var result = _internal._db.Update(keySelector, res);
                _internal.Result = new List<ClientResult>() { result };

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.int_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
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
            var def = _internal._db.GetTableDefinition<T>();

            if (_internal.LDBTransaction == null)
            {
                var result = _internal._db.Update(keySelector, values);
                _internal.Result.Add(result);

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.int_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
            else
            {
                var result = _internal._db.Update(keySelector, values);
                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.int_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }
                string name = typeof(T).Name;
                if (!_internal.LDBTransaction.defs.ContainsKey(name))
                {
                    _internal.LDBTransaction.defs[name] = def;
                }
                if (_internal.LDBTransaction.data_to_update.ContainsKey(name))
                {
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
                else
                {
                    _internal.LDBTransaction.data_to_update[name] = new List<KeyValuePair<string, Dictionary<int, byte[]>>>();
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
            }
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
            var def = _internal._db.GetTableDefinition<T>();
            for (int i = 0; ; i++)
            {
                var res = values.Skip(i * batchSize).Take(batchSize).ToDictionary(f => f.Key, f => f.Value);
                if (!res.Any())
                {
                    return;
                }

                var result = _internal._db.Update(keySelector, res);
                _internal.Result = new List<ClientResult>() { result };

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.int_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, int id, double? value)
        {
            var def = _internal._db.GetTableDefinition<T>();

            if (_internal.LDBTransaction == null)
            {
                var result = _internal._db.Update(keySelector, new Dictionary<int, double?>() { { id, value } });
                _internal.Result.Add(result);
                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.double_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }
                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
            else
            {
                var result = _internal._db.Update(keySelector, new Dictionary<int, double?>() { { id, value } });
                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.double_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }
                string name = typeof(T).Name;
                if (!_internal.LDBTransaction.defs.ContainsKey(name))
                {
                    _internal.LDBTransaction.defs[name] = def;
                }
                if (_internal.LDBTransaction.data_to_update.ContainsKey(name))
                {
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
                else
                {
                    _internal.LDBTransaction.data_to_update[name] = new List<KeyValuePair<string, Dictionary<int, byte[]>>>();
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
            }
        }

        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, int id, double value)
        {
            var def = _internal._db.GetTableDefinition<T>();
            if (_internal.LDBTransaction == null)
            {
                var result = _internal._db.Update(keySelector, new Dictionary<int, double?>() { { id, value } });
                _internal.Result.Add(result);

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.double_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
            else
            {
                var result = _internal._db.Update(keySelector, new Dictionary<int, double?>() { { id, value } });
                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.double_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }
                string name = typeof(T).Name;
                if (!_internal.LDBTransaction.defs.ContainsKey(name))
                {
                    _internal.LDBTransaction.defs[name] = def;
                }
                if (_internal.LDBTransaction.data_to_update.ContainsKey(name))
                {
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
                else
                {
                    _internal.LDBTransaction.data_to_update[name] = new List<KeyValuePair<string, Dictionary<int, byte[]>>>();
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
            }
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
            var def = _internal._db.GetTableDefinition<T>();
            if (_internal.LDBTransaction == null)
            {
                var result = _internal._db.Update(keySelector, values);
                _internal.Result.Add(result);

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.double_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
            else
            {
                var result = _internal._db.Update(keySelector, values);
                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.double_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }
                string name = typeof(T).Name;
                if (!_internal.LDBTransaction.defs.ContainsKey(name))
                {
                    _internal.LDBTransaction.defs[name] = def;
                }
                if (_internal.LDBTransaction.data_to_update.ContainsKey(name))
                {
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
                else
                {
                    _internal.LDBTransaction.data_to_update[name] = new List<KeyValuePair<string, Dictionary<int, byte[]>>>();
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
            }
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
            var def = _internal._db.GetTableDefinition<T>();
            for (int i = 0; ; i++)
            {
                var res = values.Skip(i * batchSize).Take(batchSize).ToDictionary(f => f.Key, f => f.Value);
                if (!res.Any())
                {
                    return;
                }

                var result = _internal._db.Update(keySelector, res);
                _internal.Result = new List<ClientResult>() { result };

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.double_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
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

            var def = _internal._db.GetTableDefinition<T>();
            if (_internal.LDBTransaction == null)
            {
                var result = _internal._db.Update(keySelector, values);
                _internal.Result.Add(result);

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.double_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
            else
            {
                var result = _internal._db.Update(keySelector, values);
                string name = typeof(T).Name;

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.double_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                if (!_internal.LDBTransaction.defs.ContainsKey(name))
                {
                    _internal.LDBTransaction.defs[name] = def;
                }
                if (_internal.LDBTransaction.data_to_update.ContainsKey(name))
                {
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
                else
                {
                    _internal.LDBTransaction.data_to_update[name] = new List<KeyValuePair<string, Dictionary<int, byte[]>>>();
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
            }
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

            var def = _internal._db.GetTableDefinition<T>();
            for (int i = 0; ; i++)
            {
                var res = values.Skip(i * batchSize).Take(batchSize).ToDictionary(f => f.Key, f => f.Value);
                if (!res.Any())
                {
                    return;
                }

                var result = _internal._db.Update(keySelector, res);
                _internal.Result = new List<ClientResult>() { result };

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.double_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, int id, DateTime? value)
        {
            var def = _internal._db.GetTableDefinition<T>();

            if (_internal.LDBTransaction == null)
            {
                var result = _internal._db.Update(keySelector, new Dictionary<int, DateTime?>() { { id, value } });
                _internal.Result.Add(result);

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.DateTime_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }
                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
            else
            {
                var result = _internal._db.Update(keySelector, new Dictionary<int, DateTime?>() { { id, value } });
                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.DateTime_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }
                string name = typeof(T).Name;
                if (!_internal.LDBTransaction.defs.ContainsKey(name))
                {
                    _internal.LDBTransaction.defs[name] = def;
                }
                if (_internal.LDBTransaction.data_to_update.ContainsKey(name))
                {
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
                else
                {
                    _internal.LDBTransaction.data_to_update[name] = new List<KeyValuePair<string, Dictionary<int, byte[]>>>();
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
            }
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, int id, DateTime value)
        {
            var def = _internal._db.GetTableDefinition<T>();
            if (_internal.LDBTransaction == null)
            {
                var result = _internal._db.Update(keySelector, new Dictionary<int, DateTime?>() { { id, value } });
                _internal.Result.Add(result);

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.DateTime_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
            else
            {
                var result = _internal._db.Update(keySelector, new Dictionary<int, DateTime?>() { { id, value } });
                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.DateTime_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }
                string name = typeof(T).Name;
                if (!_internal.LDBTransaction.defs.ContainsKey(name))
                {
                    _internal.LDBTransaction.defs[name] = def;
                }
                if (_internal.LDBTransaction.data_to_update.ContainsKey(name))
                {
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
                else
                {
                    _internal.LDBTransaction.data_to_update[name] = new List<KeyValuePair<string, Dictionary<int, byte[]>>>();
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
            }
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
            var def = _internal._db.GetTableDefinition<T>();
            if (_internal.LDBTransaction == null)
            {
                var result = _internal._db.Update(keySelector, values);
                _internal.Result.Add(result);

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.DateTime_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
            else
            {
                var result = _internal._db.Update(keySelector, values);
                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.DateTime_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }
                string name = typeof(T).Name;
                if (!_internal.LDBTransaction.defs.ContainsKey(name))
                {
                    _internal.LDBTransaction.defs[name] = def;
                }
                if (_internal.LDBTransaction.data_to_update.ContainsKey(name))
                {
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
                else
                {
                    _internal.LDBTransaction.data_to_update[name] = new List<KeyValuePair<string, Dictionary<int, byte[]>>>();
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
            }
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
            var def = _internal._db.GetTableDefinition<T>();
            for (int i = 0; ; i++)
            {
                var res = values.Skip(i * batchSize).Take(batchSize).ToDictionary(f => f.Key, f => f.Value);
                if (!res.Any())
                {
                    return;
                }

                var result = _internal._db.Update(keySelector, res);
                _internal.Result = new List<ClientResult>() { result };

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.DateTime_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
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
            var def = _internal._db.GetTableDefinition<T>();
            if (_internal.LDBTransaction == null)
            {
                var result = _internal._db.Update(keySelector, values);
                _internal.Result.Add(result);

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.DateTime_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
            else
            {
                var result = _internal._db.Update(keySelector, values);
                string name = typeof(T).Name;

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.DateTime_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                if (!_internal.LDBTransaction.defs.ContainsKey(name))
                {
                    _internal.LDBTransaction.defs[name] = def;
                }
                if (_internal.LDBTransaction.data_to_update.ContainsKey(name))
                {
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
                else
                {
                    _internal.LDBTransaction.data_to_update[name] = new List<KeyValuePair<string, Dictionary<int, byte[]>>>();
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
            }
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
            var def = _internal._db.GetTableDefinition<T>();
            for (int i = 0; ; i++)
            {
                var res = values.Skip(i * batchSize).Take(batchSize).ToDictionary(f => f.Key, f => f.Value);
                if (!res.Any())
                {
                    return;
                }

                var result = _internal._db.Update(keySelector, res);
                _internal.Result = new List<ClientResult>() { result };

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.DateTime_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, int id, byte[] value)
        {
            var def = _internal._db.GetTableDefinition<T>();
            if (_internal.LDBTransaction == null)
            {
                var result = _internal._db.Update(keySelector, new Dictionary<int, byte[]>() { { id, value } });
                _internal.Result.Add(result);

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.binary_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
            else
            {
                var result = _internal._db.Update(keySelector, new Dictionary<int, byte[]>() { { id, value } });
                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.binary_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }
                string name = typeof(T).Name;
                if (!_internal.LDBTransaction.defs.ContainsKey(name))
                {
                    _internal.LDBTransaction.defs[name] = def;
                }
                if (_internal.LDBTransaction.data_to_update.ContainsKey(name))
                {
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
                else
                {
                    _internal.LDBTransaction.data_to_update[name] = new List<KeyValuePair<string, Dictionary<int, byte[]>>>();
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
            }
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
            var def = _internal._db.GetTableDefinition<T>();
            if (_internal.LDBTransaction == null)
            {
                var result = _internal._db.Update(keySelector, values);
                _internal.Result.Add(result);

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.binary_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
            else
            {
                var result = _internal._db.Update(keySelector, values);
                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.binary_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }
                string name = typeof(T).Name;
                if (!_internal.LDBTransaction.defs.ContainsKey(name))
                {
                    _internal.LDBTransaction.defs[name] = def;
                }
                if (_internal.LDBTransaction.data_to_update.ContainsKey(name))
                {
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
                else
                {
                    _internal.LDBTransaction.data_to_update[name] = new List<KeyValuePair<string, Dictionary<int, byte[]>>>();
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
            }
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
            var def = _internal._db.GetTableDefinition<T>();
            for (int i = 0; ; i++)
            {
                var res = values.Skip(i * batchSize).Take(batchSize).ToDictionary(f => f.Key, f => f.Value);
                if (!res.Any())
                {
                    return;
                }

                var result = _internal._db.Update(keySelector, res);
                _internal.Result = new List<ClientResult>() { result };

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.binary_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by Id.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, int id, string value)
        {
            var def = _internal._db.GetTableDefinition<T>();
            if (_internal.LDBTransaction == null)
            {
                var result = _internal._db.Update(keySelector, new Dictionary<int, string>() { { id, value } });
                _internal.Result.Add(result);

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.string_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
            else
            {
                var result = _internal._db.Update(keySelector, new Dictionary<int, string>() { { id, value } });
                string name = typeof(T).Name;

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.string_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                if (!_internal.LDBTransaction.defs.ContainsKey(name))
                {
                    _internal.LDBTransaction.defs[name] = def;
                }
                if (_internal.LDBTransaction.data_to_update.ContainsKey(name))
                {
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
                else
                {
                    _internal.LDBTransaction.data_to_update[name] = new List<KeyValuePair<string, Dictionary<int, byte[]>>>();
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
            }
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
            var def = _internal._db.GetTableDefinition<T>();
            if (_internal.LDBTransaction == null)
            {
                var result = _internal._db.Update(keySelector, values);
                _internal.Result.Add(result);

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.string_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
            else
            {
                var result = _internal._db.Update(keySelector, values);
                string name = typeof(T).Name;

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.string_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                if (!_internal.LDBTransaction.defs.ContainsKey(name))
                {
                    _internal.LDBTransaction.defs[name] = def;
                }
                if (_internal.LDBTransaction.data_to_update.ContainsKey(name))
                {
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
                else
                {
                    _internal.LDBTransaction.data_to_update[name] = new List<KeyValuePair<string, Dictionary<int, byte[]>>>();
                    _internal.LDBTransaction.data_to_update[name].Add(new KeyValuePair<string, Dictionary<int, byte[]>>(result.Selector, result.UpdateData));
                }
            }
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
            var def = _internal._db.GetTableDefinition<T>();
            for (int i = 0; ; i++)
            {
                var res = values.Skip(i * batchSize).Take(batchSize).ToDictionary(f => f.Key, f => f.Value);
                if (!res.Any())
                {
                    return;
                }

                var result = _internal._db.Update(keySelector, res);
                _internal.Result = new List<ClientResult>() { result };

                if (_internal._db.StringTypeToLinqType(def.Item1[result.Selector]) != LinqDbTypes.string_)
                {
                    throw new LinqDbException("Linqdb: wrong data type for given column.");
                }

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
        }
        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient.
        /// </summary>
        public List<R> Select<R>(Expression<Func<T, R>> predicate)
        {
            var res = _internal._db.Select(predicate);
            _internal.Result.Add(res);

            var def = _internal._db.GetTableDefinition<T>();
            var cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _internal._db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);

            return Ldb.CreateAnonymousType<R>(res_obj, def, _internal._db, res.AnonSelect);
        }
        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient. Statistics is used as out parameter.
        /// </summary>
        public List<R> Select<R>(Expression<Func<T, R>> predicate, LinqdbSelectStatistics statistics)
        {
            var result = _internal._db.Select(predicate);
            _internal.Result.Add(result);

            var def = _internal._db.GetTableDefinition<T>();
            var cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _internal._db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);
            statistics.Total = res_obj.Total;
            statistics.SearchedPercentile = res_obj.LastStep / (double)1000;

            return Ldb.CreateAnonymousType<R>(res_obj, def, _internal._db, result.AnonSelect);
        }

        /// <summary>
        ///  Selects anonymous type using result entities. Selects in iterations to avoid select limit. Drawback is that it may return data not matching conditions if data changed after select began.
        /// </summary>
        public List<R> SelectNonAtomically<R>(Expression<Func<T, R>> predicate, int batchSize = 3000)
        {
            if (batchSize <= 0)
            {
                throw new LinqDbException("batchSize must be positive number");
            }

            //get ids
            var result = _internal._db.GetIds<T>();
            _internal.Result.Add(result);
            var def = _internal._db.GetTableDefinition<T>();

            var cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _internal._db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);

            var ids = res_obj.Ids;
            var results = new List<R>();
            for (int i = 0; ; i++)
            {
                var currentIds = ids.Skip(i * batchSize).Take(batchSize).Select(f => (int?)f).ToList();
                if (!currentIds.Any())
                {
                    return results;
                }
                _internal.Result = new List<ClientResult>();
                var idSelector = GetExpression<IHaveId, int>(f => f.Id);
                var intersectRes = _internal._db.Intersect(idSelector, new HashSet<int?>(currentIds));
                _internal.Result.Add(intersectRes);

                var selectRes = _internal._db.Select(predicate);
                _internal.Result.Add(selectRes);
                cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                bcm = CommandHelper.GetBytes(cm);
                bres = _internal._db.CallServer(bcm);
                res_obj = ServerResultHelper.GetServerResult(bres);

                var iterRes = Ldb.CreateAnonymousType<R>(res_obj, def, _internal._db, selectRes.AnonSelect);
                results.AddRange(iterRes);
            }
        }

        class IHaveId
        {
            public int Id { get; set; }
        }
        public Expression<Func<T, TKey>> GetExpression<T, TKey>(Expression<Func<T, TKey>> par)
        {
            return par;
        }
        /// <summary>
        ///  Selects entire entities using result set.
        /// </summary>
        public List<T> SelectEntity()
        {
            var result = _internal._db.SelectEntity<T>();
            _internal.Result.Add(result);

            var def = _internal._db.GetTableDefinition<T>();
            var cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _internal._db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);

            return Ldb.CreateType<T>(res_obj, def, _internal._db);
        }
        /// <summary>
        ///  Selects entire entities using result set. Statistics is used as out parameter.
        /// </summary>
        public List<T> SelectEntity(LinqdbSelectStatistics statistics)
        {
            var result = _internal._db.SelectEntity<T>();
            _internal.Result.Add(result);

            var def = _internal._db.GetTableDefinition<T>();
            var cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _internal._db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);
            statistics.Total = res_obj.Total;
            statistics.SearchedPercentile = res_obj.LastStep / (double)1000;

            return Ldb.CreateType<T>(res_obj, def, _internal._db);
        }

        /// <summary>
        ///  Selects entire entities using result set. Selects in iterations to avoid select limit. Drawback is that it may return data not matching conditions if data changed after select began.
        /// </summary>
        public List<T> SelectEntityNonAtomically(int batchSize = 3000)
        {
            if (batchSize <= 0)
            {
                throw new LinqDbException("batchSize must be positive number");
            }

            //get ids
            var result = _internal._db.GetIds<T>();
            _internal.Result.Add(result);
            var def = _internal._db.GetTableDefinition<T>();

            var cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _internal._db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);

            var ids = res_obj.Ids;
            var results = new List<T>();
            for (int i = 0; ; i++)
            {
                var currentIds = ids.Skip(i * batchSize).Take(batchSize).Select(f => (int?)f).ToList();
                if (!currentIds.Any())
                {
                    return results;
                }
                _internal.Result = new List<ClientResult>();
                var idSelector = GetExpression<IHaveId, int>(f => f.Id);
                var intersectRes = _internal._db.Intersect(idSelector, new HashSet<int?>(currentIds));
                _internal.Result.Add(intersectRes);

                var selectRes = _internal._db.SelectEntity<T>();
                _internal.Result.Add(selectRes);

                cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                bcm = CommandHelper.GetBytes(cm);
                bres = _internal._db.CallServer(bcm);
                res_obj = ServerResultHelper.GetServerResult(bres);

                var iterRes = Ldb.CreateType<T>(res_obj, def, _internal._db);
                results.AddRange(iterRes);
            }
        }

        /// <summary>
        ///  Atomically increment (decrement) value (https://en.wikipedia.org/wiki/Linearizability). Must be used after (and only) .Where, which identifies single record to update.
        /// </summary>
        //public void AtomicIncrement<TKey>(Expression<Func<T, TKey>> keySelector, int value, T new_item_if_doesnt_exist)
        //{
        //    if (_internal.LDBTransaction == null)
        //    {
        //        var def = _internal._db.GetTableDefinition<T>();
        //        var result = _internal._db.AtomicIncrement(keySelector, value, new_item_if_doesnt_exist, def.Item1, def.Item2);
        //        _internal.Result.Add(result);

        //        Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
        //        var bcm = CommandHelper.GetBytes(cm);
        //        var bres = _internal._db.CallServer(bcm);
        //        var res_obj = ServerResultHelper.GetServerResult(bres);
        //    }
        //    else
        //    {
        //        throw new LinqDbException("Linqdb: transactions are not supported with AtomicIncrement.");
        //    }
        //}
        /// <summary>
        ///  Atomically increment (decrement) value (https://en.wikipedia.org/wiki/Linearizability). Returns old value or null if item was created. Must be used after (and only) .Where, which identifies single record to update.
        /// </summary>
        public int? AtomicIncrement<TKey>(Expression<Func<T, TKey>> keySelector, int value, T new_item_if_doesnt_exist, int? only_if_old_equal)
        {
            if (_internal.LDBTransaction == null)
            {
                var def = _internal._db.GetTableDefinition<T>();
                var result = _internal._db.AtomicIncrement(keySelector, value, new_item_if_doesnt_exist, def.Item1, def.Item2, only_if_old_equal);
                _internal.Result.Add(result);

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
                int? old_val = res_obj.Old_value;
                return old_val;
            }
            else
            {
                throw new LinqDbException("Linqdb: transactions are not supported with AtomicIncrement.");
            }
        }
        /// <summary>
        ///  Atomically increment (decrement) value (https://en.wikipedia.org/wiki/Linearizability). Must be used after (and only) .Where, which identifies single record to update.
        /// </summary>
        public void AtomicIncrement2Props<TKey1, TKey2>(Expression<Func<T, TKey1>> keySelector1, Expression<Func<T, TKey2>> keySelector2, int value1, int value2, T new_item_if_doesnt_exist)
        {
            if (_internal.LDBTransaction == null)
            {
                var def = _internal._db.GetTableDefinition<T>();
                var result = _internal._db.AtomicIncrement2Props(keySelector1, keySelector2, value1, value2, new_item_if_doesnt_exist, def.Item1, def.Item2);
                _internal.Result.Add(result);

                Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
                var bcm = CommandHelper.GetBytes(cm);
                var bres = _internal._db.CallServer(bcm);
                var res_obj = ServerResultHelper.GetServerResult(bres);
            }
            else
            {
                throw new LinqDbException("Linqdb: transactions are not supported with AtomicIncrement.");
            }
        }
        /// <summary>
        ///  Applies between condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Between<TKey>(Expression<Func<T, TKey>> keySelector, int from, int to, BetweenBoundaries boundaries = BetweenBoundaries.BothInclusive)
        {
            var result = _internal._db.Between(keySelector, (double)from, (double)to, (BetweenBoundariesInternal)(int)boundaries);
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Applies between condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Between<TKey>(Expression<Func<T, TKey>> keySelector, double from, double to, BetweenBoundaries boundaries = BetweenBoundaries.BothInclusive)
        {
            var result = _internal._db.Between(keySelector, from, to, (BetweenBoundariesInternal)(int)boundaries);
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Applies between condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Between<TKey>(Expression<Func<T, TKey>> keySelector, DateTime from, DateTime to, BetweenBoundaries boundaries = BetweenBoundaries.BothInclusive)
        {
            var result = _internal._db.Between(keySelector, (from - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds, (to - new DateTime(0001, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds, (BetweenBoundariesInternal)(int)boundaries);
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<int?> set)
        {
            var result = _internal._db.Intersect(keySelector, set);
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<int> set)
        {
            var result = _internal._db.Intersect(keySelector, set);
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<double?> set)
        {
            var result = _internal._db.Intersect(keySelector, set);
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<double> set)
        {
            var result = _internal._db.Intersect(keySelector, set);
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<DateTime?> set)
        {
            var result = _internal._db.Intersect(keySelector, set);
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<DateTime> set)
        {
            var result = _internal._db.Intersect(keySelector, set);
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<string> set)
        {
            var result = _internal._db.Intersect(keySelector, new HashSet<string>(set.Select(f => f)));
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<int> set)
        {
            var result = _internal._db.Intersect(keySelector, new HashSet<int>(set.Select(f => f)));
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<int?> set)
        {
            var result = _internal._db.Intersect(keySelector, new HashSet<int?>(set.Select(f => f)));
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<double> set)
        {
            var result = _internal._db.Intersect(keySelector, new HashSet<double>(set.Select(f => f)));
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<double?> set)
        {
            var result = _internal._db.Intersect(keySelector, new HashSet<double?>(set.Select(f => f)));
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<DateTime> set)
        {
            var result = _internal._db.Intersect(keySelector, new HashSet<DateTime>(set.Select(f => f)));
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<DateTime?> set)
        {
            var result = _internal._db.Intersect(keySelector, new HashSet<DateTime?>(set.Select(f => f)));
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<string> set)
        {
            var result = _internal._db.Intersect(keySelector, new HashSet<string>(set.Select(f => f)));
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Full text search on a column.
        /// </summary>
        public ILinqDbQueryable<T> Search<TKey>(Expression<Func<T, TKey>> keySelector, string search_query, int? start_step = null, int? steps = null)
        {
            var result = _internal._db.Search(keySelector, search_query, false, false, 0, start_step, steps);
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Full text search on a column matching beginning of a word.
        /// </summary>
        public ILinqDbQueryable<T> SearchPartial<TKey>(Expression<Func<T, TKey>> keySelector, string search_query)
        {
            var result = _internal._db.Search(keySelector, search_query, true, false, 0, null, null);
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Full text search on a column, limited by time period.
        /// </summary>
        public ILinqDbQueryable<T> SearchTimeLimited<TKey>(Expression<Func<T, TKey>> keySelector, string search_query, int maxSearchTimeInMs)
        {
            var result = _internal._db.Search(keySelector, search_query, false, true, maxSearchTimeInMs, null, null);
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Applies where condition to the result set.
        /// </summary>
        public ILinqDbQueryable<T> Where(Expression<Func<T, bool>> predicate)
        {
            var result = _internal._db.Where(predicate);
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Creates in-memory property index.
        /// </summary>
        public void CreatePropertyMemoryIndex<TKey>(Expression<Func<T, TKey>> valuePropertySelector)
        {
            var result = _internal._db.CreatePropertyIndex(valuePropertySelector);
            _internal.Result.Add(result);

            var def = _internal._db.GetTableDefinition<T>();
            Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _internal._db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);
        }
        /// <summary>
        ///  Creates in-memory group-by index, parameter is property to be aggregated.
        /// </summary>
        public void CreateGroupByMemoryIndex<TKey1, TKey2>(Expression<Func<T, TKey1>> groupPropertySelector, Expression<Func<T, TKey2>> valuePropertySelector)
        {
            var result = _internal._db.CreateGroupByMemoryIndex<T, TKey1, TKey2>(groupPropertySelector, valuePropertySelector);
            _internal.Result.Add(result);

            var def = _internal._db.GetTableDefinition<T>();
            Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _internal._db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);
        }

        /// <summary>
        ///  Removes index from startup creation.
        /// </summary>
        public void RemovePropertyMemoryIndex<TKey>(Expression<Func<T, TKey>> valuePropertySelector)
        {
            var result = _internal._db.RemovePropertyIndex(valuePropertySelector);
            _internal.Result.Add(result);

            var def = _internal._db.GetTableDefinition<T>();
            Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _internal._db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);
        }
        /// <summary>
        ///  Removes group index from startup creation.
        /// </summary>
        public void RemoveGroupByMemoryIndex<TKey1, TKey2>(Expression<Func<T, TKey1>> groupPropertySelector, Expression<Func<T, TKey2>> valuePropertySelector)
        {
            var result = _internal._db.RemoveGroupByMemoryIndex<T, TKey1, TKey2>(groupPropertySelector, valuePropertySelector);
            _internal.Result.Add(result);

            var def = _internal._db.GetTableDefinition<T>();
            Command cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _internal._db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);
        }

        /// <summary>
        ///  Groups data by key. Group index must be created in advance.
        /// </summary>
        public ILinqDbGroupedQueryable<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            var result = _internal._db.GroupBy<T, TKey>(keySelector);
            _internal.Result.Add(result);
            var _inter = new IDbGroupedQueryable<T>() { Result = _internal.Result, _db = _internal._db };
            return new ILinqDbGroupedQueryable<T, TKey>() { _internal = _inter };
        }

        /// <summary>
        ///  Applies or condition to the neighbouring statements.
        /// </summary>
        public ILinqDbQueryable<T> Or()
        {
            var result = Ldb_ext.Or<T>();
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Orders data by ascending values.
        /// </summary>
        public ILinqDbOrderedQueryable<T> OrderBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            var result = _internal._db.OrderBy<T, TKey>(keySelector);
            _internal.Result.Add(result);
            var _inter = new IDbOrderedQueryable<T>() { Result = _internal.Result, _db = _internal._db };
            return new ILinqDbOrderedQueryable<T>() { _internal = _inter };
        }
        /// <summary>
        ///  Orders data by descending values.
        /// </summary>
        public ILinqDbOrderedQueryable<T> OrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            var result = _internal._db.OrderByDescending<T, TKey>(keySelector);
            _internal.Result.Add(result);
            var _inter = new IDbOrderedQueryable<T>() { Result = _internal.Result, _db = _internal._db };
            return new ILinqDbOrderedQueryable<T>() { _internal = _inter };
        }
    }
    public class ILinqDbGroupedQueryable<T, TKey> where T : new()
    {
        public IDbGroupedQueryable<T> _internal { get; set; }

        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient.
        /// </summary>
        public List<R> Select<R>(Expression<Func<IGrouping<TKey, T>, R>> predicate)
        {
            var result = _internal._db.SelectGrouped(predicate, null);
            _internal.Result.Add(result);

            var def = _internal._db.GetTableDefinition<T>();
            var cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _internal._db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);

            return Ldb.CreateGrouppedAnonymousType<R>(res_obj, def, _internal._db, result.AnonSelect);
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
            var result = _internal._db.Select(predicate);
            _internal.Result.Add(result);

            var def = _internal._db.GetTableDefinition<T>();
            var cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _internal._db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);

            return Ldb.CreateAnonymousType<R>(res_obj, def, _internal._db, result.AnonSelect);
        }
        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient. Statistics is used as out parameter.
        /// </summary>
        public List<R> Select<R>(Expression<Func<T, R>> predicate, LinqdbSelectStatistics statistics)
        {
            var result = _internal._db.Select(predicate);
            _internal.Result.Add(result);

            var def = _internal._db.GetTableDefinition<T>();
            var cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _internal._db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);
            statistics.Total = res_obj.Total;
            statistics.SearchedPercentile = res_obj.LastStep / (double)1000;

            return Ldb.CreateAnonymousType<R>(res_obj, def, _internal._db, result.AnonSelect);
        }

        /// <summary>
        ///  Selects entire entities using result set.
        /// </summary>
        public List<T> SelectEntity()
        {
            var result = _internal._db.SelectEntity<T>();
            _internal.Result.Add(result);

            var def = _internal._db.GetTableDefinition<T>();
            var cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _internal._db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);

            return Ldb.CreateType<T>(res_obj, def, _internal._db);
        }
        /// <summary>
        ///  Selects entire entities using result set. Statistics is used as out parameter.
        /// </summary>
        public List<T> SelectEntity(LinqdbSelectStatistics statistics)
        {
            var result = _internal._db.SelectEntity<T>();
            _internal.Result.Add(result);

            var def = _internal._db.GetTableDefinition<T>();
            var cm = CommandHelper.GetCommand(_internal.Result, def.Item1, def.Item2, typeof(T).Name, _internal._db.User, _internal._db.Pass);
            var bcm = CommandHelper.GetBytes(cm);
            var bres = _internal._db.CallServer(bcm);
            var res_obj = ServerResultHelper.GetServerResult(bres);
            statistics.Total = res_obj.Total;
            statistics.SearchedPercentile = res_obj.LastStep / (double)1000;

            return Ldb.CreateType<T>(res_obj, def, _internal._db);
        }
        /// <summary>
        ///  Skips some ordered values.
        /// </summary>
        public ILinqDbOrderedQueryable<T> Skip(int count)
        {
            var result = _internal._db.Skip<T>(count);
            _internal.Result.Add(result);
            return this;
        }
        /// <summary>
        ///  Takes some ordered values.
        /// </summary>
        public ILinqDbOrderedQueryable<T> Take(int count)
        {
            var result = _internal._db.Take<T>(count);
            _internal.Result.Add(result);
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
