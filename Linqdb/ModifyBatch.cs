using ServerSharedData;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    //the purpose of ModifyBatch is to accumulate modification operations while waiting for write lock
    //the more data we pack in a batch the more efficient save is going to be
    //especially in presence of string or in-memory index
    public class IncrementData
    {
        //value to increment
        public int Value { get; set; }
        //value to increment if new
        public int ValueIfNew { get; set; }
        //if not exists
        public object NewObject { get; set; }
        //callbacks with error if any
        public List<Action<string>> Callbacks { get; set; }
    }
    public class SaveData
    {
        public HashSet<int> Ids { get; set; }
        public List<object> Items { get; set; }
        public List<Action<string>> Callbacks { get; set; }

        public List<Tuple<int, Action<string, List<int>>>> CallbacksServer { get; set; }
        public List<BinData> ItemsServer { get; set; }
    }
    public class UpdateData
    {
        public Dictionary<int, object> values { get; set; }
        public Dictionary<int, byte[]> valuesServer { get; set; }
        //callbacks with error if any
        public List<Action<string>> Callbacks { get; set; }
    }
    public class DeleteData
    {
        public HashSet<int> ids { get; set; }
        //callbacks with error if any
        public List<Action<string>> Callbacks { get; set; }
    }
    public class ModifyBatch
    {
        #region atomic increment
        static object _increment_batch_lock = new object();
        static Dictionary<string, object> _increment_batch_locks = new Dictionary<string, object>();
        public static object GetTableIncrementBatchLock(string name)
        {
            lock (_increment_batch_lock)
            {
                if (!_increment_batch_locks.ContainsKey(name))
                {
                    _increment_batch_locks[name] = new object();
                }
                return _increment_batch_locks[name];
            }
        }
        //key: dic key: table name + where condition hash
        public static ConcurrentDictionary<ulong, IncrementData> _increment_batch = new ConcurrentDictionary<ulong, IncrementData>();
        #endregion

        #region save
        static object _save_batch_lock = new object();
        static Dictionary<string, object> _save_batch_locks = new Dictionary<string, object>();
        public static object GetTableSaveBatchLock(string name)
        {
            lock (_save_batch_lock)
            {
                if (!_save_batch_locks.ContainsKey(name))
                {
                    _save_batch_locks[name] = new object();
                }
                return _save_batch_locks[name];
            }
        }
        //key: dic key: table name
        public static ConcurrentDictionary<string, SaveData> _save_batch = new ConcurrentDictionary<string, SaveData>();
        #endregion


        #region update
        static object _update_batch_lock = new object();
        static Dictionary<string, object> _update_batch_locks = new Dictionary<string, object>();
        public static object GetTableUpdateBatchLock(string name)
        {
            lock (_update_batch_lock)
            {
                if (!_update_batch_locks.ContainsKey(name))
                {
                    _update_batch_locks[name] = new object();
                }
                return _update_batch_locks[name];
            }
        }
        //key: dic key: table_name|prop_name
        public static ConcurrentDictionary<string, UpdateData> _update_batch = new ConcurrentDictionary<string, UpdateData>();
        #endregion

        #region delete
        static object _delete_batch_lock = new object();
        static Dictionary<string, object> _delete_batch_locks = new Dictionary<string, object>();
        public static object GetTableDeleteBatchLock(string name)
        {
            lock (_delete_batch_lock)
            {
                if (!_delete_batch_locks.ContainsKey(name))
                {
                    _delete_batch_locks[name] = new object();
                }
                return _delete_batch_locks[name];
            }
        }
        //key: dic key: table_name
        public static ConcurrentDictionary<string, DeleteData> _delete_batch = new ConcurrentDictionary<string, DeleteData>();
        #endregion
    }
}
