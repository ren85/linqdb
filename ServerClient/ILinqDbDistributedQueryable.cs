using LinqDbClientInternal;
using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace LinqdbClient
{
    public struct DistributedId
    {
        public DistributedId(int serverId, int id)
        { 
            ServerId = serverId;
            Id = id;
        }
        public int ServerId { get; set; }
        public int Id { get; set; }
    }
    public class ServerDb
    {
        public Db db { get; set; }
        public int ServerId { get; set; }
    }
    public class ILinqDbDistributedQueryable<T> where T : new()
    {
        Dictionary<int, Db> _dbs_value { get; set; }
        public Dictionary<int, Db> _dbs
        {
            get
            {
                return _dbs_value;
            }
            set
            {
                _list_dbs = value.Select(f => new ServerDb() { ServerId = f.Key, db = f.Value }).ToList();
                _dbs_value = value;

                _list_queryable = new List<Tuple<ServerDb, ILinqDbQueryable<T>>>();
                foreach (var db in _list_dbs)
                {
                    var _inter = new IDbQueryable<T>() { Result = new List<ClientResult>(), _db = db.db._db_internal };
                    var queryable = new ILinqDbQueryable<T>() { _internal = _inter };
                    _list_queryable.Add(new Tuple<ServerDb, ILinqDbQueryable<T>>(db, queryable));
                }
            }
        }
        List<ServerDb> _list_dbs { get; set; }
        List<Tuple<ServerDb, ILinqDbQueryable<T>>> _list_queryable { get; set; }

        ServerDb GetRandomDb()
        {
            var rg = new Random();
            var which = rg.Next(0, _dbs.Count());
            return _list_dbs[which];
        }
        ServerDb GetDb(int ServerId)
        {
            return new ServerDb()
            {
                ServerId = ServerId,
                db = _dbs[ServerId]
            };
        }

        /// <summary>
        ///  Saves new item if Id is 0 and assigns new (Sid,Id). Updates existing item if (Sid,Id) is existing's item's (Sid,Id).
        /// </summary>
        public DistributedId Save(T item)
        {
            var prop = item.GetType().GetProperty("Id");
            if (prop == null)
            {
                throw new LinqDbException("Linqdb: type must have integer Id property");
            }
            int id = (int)prop.GetValue(item);
            var prop2 = item.GetType().GetProperty("Sid");
            if (prop2 == null)
            {
                throw new LinqDbException("Linqdb: type must have integer Sid property");
            }
            int sid = (int)prop2.GetValue(item);

            if (id == 0 && sid == 0)
            {
                var db = GetRandomDb();
                prop2.SetValue(item, db.ServerId);
                var local_id = db.db.Table<T>().Save(item);
                prop.SetValue(item, local_id);
                return new DistributedId()
                {
                    ServerId = sid,
                    Id = local_id
                };
            }
            else if (sid != 0 && id != 0)
            {
                var db = GetDb(sid);
                db.db.Table<T>().Save(item);
                return new DistributedId()
                {
                    Id = id,
                    ServerId = sid
                };
            }
            else
            {
                throw new LinqDbException("Linqdb: Id and Sid must both be equal to zero (for new items) or both be non-zero to indicate specific item.");
            }
        }
        /// <summary>
        ///  Saves any amount of items non-atomically, i.e. if it fails in the middle some items will be saved and some won't be.
        /// </summary>
        public void SaveBatch(List<T> items, int batchSize = 5000)
        {
            if (items == null || !items.Any())
            {
                return;
            }
            var grouped = new Dictionary<int, List<T>>();
            bool isNew = false;
            for (int i = 0; i < items.Count; i++)
            {
                var prop = items[i].GetType().GetProperty("Id");
                if (prop == null)
                {
                    throw new LinqDbException("Linqdb: type must have integer Id property");
                }
                int id = (int)prop.GetValue(items[i]);
                prop = items[i].GetType().GetProperty("Sid");
                if (prop == null)
                {
                    throw new LinqDbException("Linqdb: type must have integer Sid property");
                }
                int sid = (int)prop.GetValue(items[i]);
                if (id != 0 && sid == 0 || id == 0 && sid != 0)
                {
                    throw new LinqDbException("Linqdb: Id and Sid must both be equal to zero (for new items) or both be non-zero to indicate specific item.");
                }
                if (sid != 0)
                {
                    if (grouped.ContainsKey(sid))
                    {
                        grouped[sid] = new List<T>();
                    }
                    grouped[sid].Add(items[i]);
                }
                if (i == 0)
                {
                    if (id == 0)
                    {
                        isNew = true;
                    }
                }
                else
                {
                    if (isNew && id != 0 || !isNew && id == 0)
                    {
                        throw new LinqDbException("Linqdb: Batch cannot contain both new (Id is 0) and non-new items.");
                    }
                }
            }

            var data = new List<Tuple<Db, List<T>>>();
            if (isNew)
            {
                var dataDbs = new List<Tuple<ServerDb, List<T>>>();
                var total = _list_dbs.Count;
                var size = items.Count / total + 1;
                for (int i = 0; i < total; i++)
                {
                    dataDbs.Add(new Tuple<ServerDb, List<T>>(_list_dbs[i], new List<T>(size)));
                }
                for (int i = 0; i < items.Count; i++)
                {
                    int dbId = i % total;
                    var prop = items[i].GetType().GetProperty("Sid");
                    prop.SetValue(items[i], dataDbs[dbId].Item1.ServerId);
                    dataDbs[dbId].Item2.Add(items[i]);
                    data = dataDbs.Select(f => new Tuple<Db, List<T>>(f.Item1.db, f.Item2)).ToList();
                }               
            }
            else
            {
                data = grouped.Select(f => new Tuple<Db, List<T>>(GetDb(f.Key).db, f.Value.ToList())).ToList();
            }

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1.Table<T>().SaveNonAtomically(d.Item2, batchSize);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: SaveNonAtomically error", errors);
            }
        }

        /// <summary>
        ///  Record count.
        /// </summary>
        public int Count()
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            int result = 0;
            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    db_name = d.Item1.db.ServerName;
                    var count = d.Item2.Count();
                    lock (_lock)
                    {
                        result += count;
                    }
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Count error", errors);
            }
            return result;
        }

        /// <summary>
        ///  Get selected ids without having to select them (more efficiently).
        /// </summary>
        public SelectedDistributedIds GetIds()
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            var result = new List<SelectedDistributedIds>();
            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    db_name = d.Item1.db.ServerName;
                    var serverId = d.Item1.ServerId;
                    var ids = d.Item2.GetIds();
                    lock (_lock)
                    {
                        result.Add(new SelectedDistributedIds()
                        { 
                            AllIds = ids.AllIds,
                            Ids = ids.Ids != null && ids.Ids.Any() ? ids.Ids.Select(f => new DistributedId(serverId, f)).ToList() : new List<DistributedId>()
                        });
                    }
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: GetIds error", errors);
            }

            var res = new SelectedDistributedIds()
            {
                AllIds = result.All(f => f.AllIds),
                Ids = new List<DistributedId>()
            };

            foreach (var r in result)
            {
                res.Ids.AddRange(r.Ids);
            }

            return res;
        }
        /// <summary>
        ///  Last search step. To be used in step-search.
        /// </summary>
        public int LastStep()
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            var result = new List<int>();
            Parallel.ForEach(_list_dbs, d =>
            {
                string db_name = d.db.GetIpAndPort();
                try
                {
                    var lastStep = d.db.Table<T>().LastStep();
                    lock (_lock)
                    {
                        result.Add(lastStep);
                    }
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: LastStep error", errors);
            }

            return result.Max();
        }
        /// <summary>
        ///  Deletes item with given Id.
        /// </summary>
        public void Delete(DistributedId id)
        {
            var db = GetDb(id.ServerId);
            db.db.Table<T>().Delete(id.Id);
        }
        /// <summary>
        ///  Deletes any amount of items non-atomically, i.e. if it fails in the middle some items will be deleted and some won't be.
        /// </summary>
        public void Delete(List<DistributedId> ids, int batchSize = 5000)
        {
            if (ids == null || !ids.Any())
            {
                return;
            }

            var data = ids.GroupBy(f => f.ServerId).Select(f => new Tuple<Db, HashSet<int>>(GetDb(f.Key).db, new HashSet<int>(f.Select(z => z.Id).ToList()))).ToList();

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1.Table<T>().DeleteNonAtomically(d.Item2, batchSize);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: DeleteNonAtomically error", errors);
            }
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by DistributedId.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, DistributedId id, byte[] value)
        {
            var db = GetDb(id.ServerId);
            db.db.Table<T>().Update(keySelector, id.Id, value);
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by DistributedId.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, DistributedId id, DateTime value)
        {
            var db = GetDb(id.ServerId);
            db.db.Table<T>().Update(keySelector, id.Id, value);
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by DistributedId.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, DistributedId id, double value)
        {
            var db = GetDb(id.ServerId);
            db.db.Table<T>().Update(keySelector, id.Id, value);
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by DistributedId.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, DistributedId id, int value)
        {
            var db = GetDb(id.ServerId);
            db.db.Table<T>().Update(keySelector, id.Id, value);
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by DistributedId.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, DistributedId id, string value)
        {
            var db = GetDb(id.ServerId);
            db.db.Table<T>().Update(keySelector, id.Id, value);
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by DistributedId.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, DistributedId id, DateTime? value)
        {
            var db = GetDb(id.ServerId);
            db.db.Table<T>().Update(keySelector, id.Id, value);
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by DistributedId.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, DistributedId id, double? value)
        {
            var db = GetDb(id.ServerId);
            db.db.Table<T>().Update(keySelector, id.Id, value);
        }
        /// <summary>
        ///  Updates item's field with supplied value. Item is identified by DistributedId.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, DistributedId id, int? value)
        {
            var db = GetDb(id.ServerId);
            db.db.Table<T>().Update(keySelector, id.Id, value);
        }
        /// <summary>
        ///  Updates items' fields with supplied values. Item is identified by DistributedId.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<DistributedId, byte[]> values, int batchSize = 5000)
        {
            if (values == null || !values.Any())
            {
                return;
            }

            var data = values.GroupBy(f => f.Key.ServerId).Select(f => new Tuple<Db, Dictionary<int, byte[]>>(GetDb(f.Key).db, f.ToDictionary(z => z.Key.Id, z => z.Value))).ToList();

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1.Table<T>().UpdateNonAtomically(keySelector, d.Item2, batchSize);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Update error", errors);
            }
        }
        /// <summary>
        ///  Updates items' fields with supplied values. Item is identified by DistributedId.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<DistributedId, int> values, int batchSize = 5000)
        {
            if (values == null || !values.Any())
            {
                return;
            }

            var data = values.GroupBy(f => f.Key.ServerId).Select(f => new Tuple<Db, Dictionary<int, int>>(GetDb(f.Key).db, f.ToDictionary(z => z.Key.Id, z => z.Value))).ToList();

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1.Table<T>().UpdateNonAtomically(keySelector, d.Item2, batchSize);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Update error", errors);
            }
        }
        /// <summary>
        ///  Updates items' fields with supplied values. Item is identified by DistributedId.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<DistributedId, DateTime> values, int batchSize = 5000)
        {
            if (values == null || !values.Any())
            {
                return;
            }

            var data = values.GroupBy(f => f.Key.ServerId).Select(f => new Tuple<Db, Dictionary<int, DateTime>>(GetDb(f.Key).db, f.ToDictionary(z => z.Key.Id, z => z.Value))).ToList();

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1.Table<T>().UpdateNonAtomically(keySelector, d.Item2, batchSize);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Update error", errors);
            }
        }
        /// <summary>
        ///  Updates items' fields with supplied values. Item is identified by DistributedId.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<DistributedId, double> values, int batchSize = 5000)
        {
            if (values == null || !values.Any())
            {
                return;
            }

            var data = values.GroupBy(f => f.Key.ServerId).Select(f => new Tuple<Db, Dictionary<int, double>>(GetDb(f.Key).db, f.ToDictionary(z => z.Key.Id, z => z.Value))).ToList();

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1.Table<T>().UpdateNonAtomically(keySelector, d.Item2, batchSize);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Update error", errors);
            }
        }
        /// <summary>
        ///  Updates items' fields with supplied values. Item is identified by DistributedId.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<DistributedId, int?> values, int batchSize = 5000)
        {
            if (values == null || !values.Any())
            {
                return;
            }

            var data = values.GroupBy(f => f.Key.ServerId).Select(f => new Tuple<Db, Dictionary<int, int?>>(GetDb(f.Key).db, f.ToDictionary(z => z.Key.Id, z => z.Value))).ToList();

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1.Table<T>().UpdateNonAtomically(keySelector, d.Item2, batchSize);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Update error", errors);
            }
        }
        /// <summary>
        ///  Updates items' fields with supplied values. Item is identified by DistributedId.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<DistributedId, DateTime?> values, int batchSize = 5000)
        {
            if (values == null || !values.Any())
            {
                return;
            }

            var data = values.GroupBy(f => f.Key.ServerId).Select(f => new Tuple<Db, Dictionary<int, DateTime?>>(GetDb(f.Key).db, f.ToDictionary(z => z.Key.Id, z => z.Value))).ToList();

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1.Table<T>().UpdateNonAtomically(keySelector, d.Item2, batchSize);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Update error", errors);
            }
        }
        /// <summary>
        ///  Updates items' fields with supplied values. Item is identified by DistributedId.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<DistributedId, double?> values, int batchSize = 5000)
        {
            if (values == null || !values.Any())
            {
                return;
            }

            var data = values.GroupBy(f => f.Key.ServerId).Select(f => new Tuple<Db, Dictionary<int, double?>>(GetDb(f.Key).db, f.ToDictionary(z => z.Key.Id, z => z.Value))).ToList();

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1.Table<T>().UpdateNonAtomically(keySelector, d.Item2, batchSize);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Update error", errors);
            }
        }
        /// <summary>
        ///  Updates items' fields with supplied values. Item is identified by DistributedId.
        /// </summary>
        public void Update<TKey>(Expression<Func<T, TKey>> keySelector, Dictionary<DistributedId, string> values, int batchSize = 5000)
        {
            if (values == null || !values.Any())
            {
                return;
            }

            var data = values.GroupBy(f => f.Key.ServerId).Select(f => new Tuple<Db, Dictionary<int, string>>(GetDb(f.Key).db, f.ToDictionary(z => z.Key.Id, z => z.Value))).ToList();

            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            Parallel.ForEach(data, d =>
            {
                string db_name = d.Item1.GetIpAndPort();
                try
                {
                    d.Item1.Table<T>().UpdateNonAtomically(keySelector, d.Item2, batchSize);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Update error", errors);
            }
        }

        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient.
        /// </summary>
        public List<R> Select<R>(Expression<Func<T, R>> predicate, int batchSize = 3000)
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            var result = new List<R>();
            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var values = d.Item2.SelectNonAtomically(predicate, batchSize);
                    lock (_lock)
                    {
                        result.AddRange(values);
                    }
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });

            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Select error", errors);
            }

            return result;
        }

        /// <summary>
        ///  Selects entire entities using result set.
        /// </summary>
        public List<T> SelectEntity(int batchSize = 3000)
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            var result = new List<T>();
            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var values = d.Item2.SelectEntityNonAtomically(batchSize);
                    lock (_lock)
                    {
                        result.AddRange(values);
                    }
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: SelectEntity error", errors);
            }

            return result;
        }

        /// <summary>
        ///  Applies between condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Between<TKey>(Expression<Func<T, TKey>> keySelector, int from, int to, BetweenBoundaries boundaries = BetweenBoundaries.BothInclusive)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Between(keySelector, (double)from, (double)to, (BetweenBoundariesInternal)(int)boundaries);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Applies between condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Between<TKey>(Expression<Func<T, TKey>> keySelector, double from, double to, BetweenBoundaries boundaries = BetweenBoundaries.BothInclusive)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Between(keySelector, (double)from, (double)to, (BetweenBoundariesInternal)(int)boundaries);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Applies between condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Between<TKey>(Expression<Func<T, TKey>> keySelector, DateTime from, DateTime to, BetweenBoundaries boundaries = BetweenBoundaries.BothInclusive)
        {

            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Between(keySelector, (from - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds, (to - new DateTime(0001, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds, (BetweenBoundariesInternal)(int)boundaries);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<int?> set)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Intersect(keySelector, set);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<int> set)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Intersect(keySelector, set);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<double?> set)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Intersect(keySelector, set);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<double> set)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Intersect(keySelector, set);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<DateTime?> set)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Intersect(keySelector, set);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<DateTime> set)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Intersect(keySelector, set);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, HashSet<string> set)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Intersect(keySelector, set);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<int> set)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Intersect(keySelector, new HashSet<int>(set));
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<int?> set)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Intersect(keySelector, new HashSet<int?>(set));
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<double> set)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Intersect(keySelector, new HashSet<double>(set));
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<double?> set)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Intersect(keySelector, new HashSet<double?>(set));
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<DateTime> set)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Intersect(keySelector, new HashSet<DateTime>(set));
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<DateTime?> set)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Intersect(keySelector, new HashSet<DateTime?>(set));
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Applies intersect condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Intersect<TKey>(Expression<Func<T, TKey>> keySelector, List<string> set)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Intersect(keySelector, new HashSet<string>(set));
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Full text search on a column.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Search<TKey>(Expression<Func<T, TKey>> keySelector, string search_query, int? start_step = null, int? steps = null)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Search(keySelector, search_query, false, false, 0, start_step, steps);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Full text search on a column matching beginning of a word.
        /// </summary>
        public ILinqDbDistributedQueryable<T> SearchPartial<TKey>(Expression<Func<T, TKey>> keySelector, string search_query)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Search(keySelector, search_query, true, false, 0, null, null);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Full text search on a column, limited by time period.
        /// </summary>
        public ILinqDbDistributedQueryable<T> SearchTimeLimited<TKey>(Expression<Func<T, TKey>> keySelector, string search_query, int maxSearchTimeInMs)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Search(keySelector, search_query, false, true, maxSearchTimeInMs, null, null);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Applies where condition to the result set.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Where(Expression<Func<T, bool>> predicate)
        {
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.Where(predicate);
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Creates in-memory property index.
        /// </summary>
        public void CreatePropertyMemoryIndex<TKey>(Expression<Func<T, TKey>> valuePropertySelector)
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            var result = new List<T>();
            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    d.Item2.CreatePropertyMemoryIndex(valuePropertySelector);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: CreatePropertyMemoryIndex error", errors);
            }
        }
        /// <summary>
        ///  Creates in-memory group-by index, parameter is property to be aggregated.
        /// </summary>
        public void CreateGroupByMemoryIndex<TKey1, TKey2>(Expression<Func<T, TKey1>> groupPropertySelector, Expression<Func<T, TKey2>> valuePropertySelector)
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            var result = new List<T>();
            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    d.Item2.CreateGroupByMemoryIndex(groupPropertySelector, valuePropertySelector);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: CreateGroupByMemoryIndex error", errors);
            }
        }

        /// <summary>
        ///  Removes index from startup creation.
        /// </summary>
        public void RemovePropertyMemoryIndex<TKey>(Expression<Func<T, TKey>> valuePropertySelector)
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            var result = new List<T>();
            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    d.Item2.RemovePropertyMemoryIndex(valuePropertySelector);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: RemovePropertyMemoryIndex error", errors);
            }
        }
        /// <summary>
        ///  Removes group index from startup creation.
        /// </summary>
        public void RemoveGroupByMemoryIndex<TKey1, TKey2>(Expression<Func<T, TKey1>> groupPropertySelector, Expression<Func<T, TKey2>> valuePropertySelector)
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            var result = new List<T>();
            Parallel.ForEach(_list_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    d.Item2.RemoveGroupByMemoryIndex(groupPropertySelector, valuePropertySelector);
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: RemoveGroupByMemoryIndex error", errors);
            }
        }

        /// <summary>
        ///  Groups data by key. Group index must be created in advance.
        /// </summary>
        public ILinqDbGroupedDistributedQueryable<T, TKey> GroupBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            var list_grouped_queryable = new List<Tuple<ServerDb, ILinqDbGroupedQueryable<T, TKey>>>();
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.GroupBy(keySelector);
                db.Item2._internal.Result.Add(result);

                var _inter = new IDbGroupedQueryable<T>() { Result = db.Item2._internal.Result, _db = db.Item2._internal._db };
                list_grouped_queryable.Add(new Tuple<ServerDb, ILinqDbGroupedQueryable<T, TKey>>(db.Item1, new ILinqDbGroupedQueryable<T, TKey>() { _internal = _inter }));
            }

            return new ILinqDbGroupedDistributedQueryable<T, TKey>() { _list_grouped_queryable = list_grouped_queryable };
        }

        /// <summary>
        ///  Applies or condition to the neighbouring statements.
        /// </summary>
        public ILinqDbDistributedQueryable<T> Or()
        {
            foreach (var db in _list_queryable)
            {
                var result = Ldb_ext.Or<T>();
                db.Item2._internal.Result.Add(result);
            }

            return this;
        }
        /// <summary>
        ///  Orders data by ascending values.
        /// </summary>
        public ILinqDbOrderedDistributedQueryable<T, TKey> OrderBy<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            var list_ordered_queryable = new List<Tuple<ServerDb, ILinqDbOrderedQueryable<T>>>();
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.OrderBy(keySelector);
                db.Item2._internal.Result.Add(result);

                var _inter = new IDbOrderedQueryable<T>() { Result = db.Item2._internal.Result, _db = db.Item2._internal._db };
                list_ordered_queryable.Add(new Tuple<ServerDb, ILinqDbOrderedQueryable<T>>(db.Item1, new ILinqDbOrderedQueryable<T>() {  _internal = _inter }));
            }

            return new ILinqDbOrderedDistributedQueryable<T, TKey>() { _list_ordered_queryable = list_ordered_queryable, keySelector = keySelector, Asc = true };
        }
        /// <summary>
        ///  Orders data by descending values.
        /// </summary>
        public ILinqDbOrderedDistributedQueryable<T, TKey> OrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector)
        {
            var list_ordered_queryable = new List<Tuple<ServerDb, ILinqDbOrderedQueryable<T>>>();
            foreach (var db in _list_queryable)
            {
                var result = db.Item2._internal._db.OrderByDescending(keySelector);
                db.Item2._internal.Result.Add(result);

                var _inter = new IDbOrderedQueryable<T>() { Result = db.Item2._internal.Result, _db = db.Item2._internal._db };
                list_ordered_queryable.Add(new Tuple<ServerDb, ILinqDbOrderedQueryable<T>>(db.Item1, new ILinqDbOrderedQueryable<T>() { _internal = _inter }));
            }

            return new ILinqDbOrderedDistributedQueryable<T, TKey>() { _list_ordered_queryable = list_ordered_queryable, keySelector = keySelector, Asc = false };
        }

    }

    public class SelectedDistributedIds
    {
        public List<DistributedId> Ids { get; set; }
        public bool AllIds { get; set; }
    }

    public class ILinqDbGroupedDistributedQueryable<T, TKey> where T : new()
    {
        public List<Tuple<ServerDb, ILinqDbGroupedQueryable<T, TKey>>> _list_grouped_queryable { get; set; }

        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient.
        /// </summary>
        public List<R> Select<R>(Expression<Func<IGrouping<TKey, T>, R>> predicate)
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            var result = new List<R>();
            Parallel.ForEach(_list_grouped_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var values = d.Item2.Select(predicate);
                    lock (_lock)
                    {
                        result.AddRange(values);
                    }
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Select (groupby) error", errors);
            }

            return result;
        }
    }

    public class ILinqDbOrderedDistributedQueryable<T, TKey> where T : new()
    {
        public List<Tuple<ServerDb, ILinqDbOrderedQueryable<T>>> _list_ordered_queryable { get; set; }
        public Expression<Func<T, TKey>> keySelector { get; set; }
        public int TakeCount { get; set; }
        public bool Asc { get; set; }

        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient.
        /// </summary>
        public List<R> Select<R>(Expression<Func<T, R>> predicate)
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            var result = new List<R>();
            Parallel.ForEach(_list_ordered_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var values = d.Item2.Select(predicate);
                    lock (_lock)
                    {
                        result.AddRange(values);
                    }
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Select error (orderby)", errors);
            }

            var expName = keySelector.Body.ToString();
            var propName = expName.Substring(expName.IndexOf('.') + 1);
            var res = OrderListByProperty(result, propName, Asc);

            if (this.TakeCount > 0)
            {
                res = res.Take(TakeCount).ToList();
            }

            return res;
        }

        List<R> OrderListByProperty<R>(List<R> list, string propertyName, bool ascending)
        {
            PropertyInfo property = typeof(R).GetProperty(propertyName);

            if (property == null)
            {
                throw new LinqDbException($"Linqdb: Property '{propertyName}' must be present in anonymous type.");
            }

            if (!property.PropertyType.IsValueType && property.PropertyType != typeof(string))
            {
                throw new ArgumentException($"Linqdb: Property '{propertyName}' must be a value type or string");
            }

            return ascending ? list.OrderBy(f => property.GetValue(f)).ToList() : list.OrderByDescending(f => property.GetValue(f)).ToList();
        }

        /// <summary>
        ///  Selects anonymous type using result entities. Select only what's needed as it is more efficient. Statistics is used as out parameter.
        /// </summary>
        public List<R> Select<R>(Expression<Func<T, R>> predicate, LinqdbSelectStatistics statistics)
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            var result = new List<R>();
            var statsList = new List<LinqdbSelectStatistics>();
            Parallel.ForEach(_list_ordered_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    LinqdbSelectStatistics stat = new LinqdbSelectStatistics();
                    var values = d.Item2.Select(predicate, stat);
                    lock (_lock)
                    {
                        result.AddRange(values);
                        statsList.Add(stat);
                    }
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: Select error (orderby, statistics)", errors);
            }

            statistics.Total = statsList.Sum(f => f.Total);

            var expName = keySelector.Body.ToString();
            var propName = expName.Substring(expName.IndexOf('.') + 1);
            var res = OrderListByProperty(result, propName, Asc);

            if (this.TakeCount > 0)
            {
                res = res.Take(TakeCount).ToList();
            }

            return res;
        }

        /// <summary>
        ///  Selects entire entities using result set.
        /// </summary>
        public List<T> SelectEntity()
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            var result = new List<T>();
            Parallel.ForEach(_list_ordered_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    var values = d.Item2.SelectEntity();
                    lock (_lock)
                    {
                        result.AddRange(values);
                    }
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: SelectEntity error (orderby)", errors);
            }

            var expName = keySelector.Body.ToString();
            var propName = expName.Substring(expName.IndexOf('.') + 1);
            var res = OrderListByProperty(result, propName, Asc);

            if (this.TakeCount > 0)
            {
                res = res.Take(TakeCount).ToList();
            }

            return res;
        }

        /// <summary>
        ///  Selects entire entities using result set. Statistics is used as out parameter.
        /// </summary>
        public List<T> SelectEntity(LinqdbSelectStatistics statistics)
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            var result = new List<T>();
            var statsList = new List<LinqdbSelectStatistics>();
            Parallel.ForEach(_list_ordered_queryable, d =>
            {
                string db_name = d.Item1.db.GetIpAndPort();
                try
                {
                    LinqdbSelectStatistics stat = new LinqdbSelectStatistics();
                    var values = d.Item2.SelectEntity(stat);
                    lock (_lock)
                    {
                        result.AddRange(values);
                        statsList.Add(stat);
                    }
                }
                catch (Exception ex)
                {
                    lock (_lock)
                    {
                        if (!errors.ContainsKey(db_name))
                        {
                            errors[db_name] = new List<Exception>();
                        }
                        errors[db_name].Add(ex);
                    }
                }
            });
            if (errors.Any())
            {
                throw new LinqDbException("Linqdb: SelectEntity error (orderby)", errors);
            }

            statistics.Total = statsList.Sum(f => f.Total);
            var expName = keySelector.Body.ToString();
            var propName = expName.Substring(expName.IndexOf('.') + 1);
            var res = OrderListByProperty(result, propName, Asc);

            if (this.TakeCount > 0)
            {
                res = res.Take(TakeCount).ToList();
            }

            return res;
        }


        /// <summary>
        ///  Takes some ordered values.
        /// </summary>
        public ILinqDbOrderedDistributedQueryable<T, TKey> Take(int count)
        {
            foreach (var db in _list_ordered_queryable)
            {
                var result = db.Item2._internal._db.Take<T>(count);
                db.Item2._internal.Result.Add(result);
            }
            TakeCount = count;
            return this;
        }

    }

    public class IDbGroupedDistributedQueryable<T>
    {
        public Ldb _db { get; set; }
        public List<ClientResult> Result = new List<ClientResult>();
    }
}
