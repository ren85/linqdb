using LinqDbClientInternal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqdbClient
{
    public class DistributedDb
    {
        Dictionary<int, Db> _distributed_dbs { get; set; }
        public DistributedDb(Dictionary<int, Db> distributed_dbs)
        {
            _distributed_dbs = distributed_dbs;
        }

        /// <summary>
        ///  Indicates which distributed table operation is to be performed on.
        /// </summary>
        public ILinqDbDistributedQueryable<T> DistributedTable<T>() where T : new()
        {
            return new ILinqDbDistributedQueryable<T>() { _dbs = _distributed_dbs };
        }

        /// <summary>
        ///  Get table names.
        /// </summary>
        public List<string> GetTables()
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            List<string> result = new List<string>();
            Parallel.ForEach(_distributed_dbs, d =>
            {
                string db_name = d.Value.GetIpAndPort();
                try
                {
                    db_name = d.Value.ServerName;
                    var tables = d.Value.GetTables();
                    lock (_lock)
                    {
                        result.AddRange(tables);
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
                throw new LinqDbException("Linqdb: GetTables", errors);
            }
            return result.Distinct().ToList();
        }
        /// <summary>
        ///  Get table definition.
        /// </summary>
        public string GetTableDefinition(string table_name)
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            List<string> result = new List<string>();
            Parallel.ForEach(_distributed_dbs, d =>
            {
                string db_name = d.Value.GetIpAndPort();
                try
                {
                    db_name = d.Value.ServerName;
                    var def = d.Value.GetTableDefinition(table_name);
                    lock (_lock)
                    {
                        result.Add(def);
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
                throw new LinqDbException("Linqdb: GetTableDefinition", errors);
            }
            return result.FirstOrDefault(f => !string.IsNullOrEmpty(f));
        }

        /// <summary>
        ///  Get existing indexes
        /// </summary>
        public List<string> GetExistingIndexes()
        {
            Dictionary<string, List<Exception>> errors = new Dictionary<string, List<Exception>>();
            object _lock = new object();
            List<string> result = new List<string>();
            Parallel.ForEach(_distributed_dbs, d =>
            {
                string db_name = d.Value.GetIpAndPort();
                try
                {
                    db_name = d.Value.ServerName;
                    var tables = d.Value.GetExistingIndexes();
                    lock (_lock)
                    {
                        result.AddRange(tables);
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
                throw new LinqDbException("Linqdb: GetExistingIndexes", errors);
            }
            return result.Distinct().ToList();
        }


        public void Dispose()
        { }
    }
}
