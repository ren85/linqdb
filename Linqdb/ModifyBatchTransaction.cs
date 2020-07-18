using ServerSharedData;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public class TransBatchData
    {
        public Dictionary<string, HashSet<int>> Ids { get; set; }
        //embedded
        public Dictionary<string, KeyValuePair<TableInfo, List<object>>> data_to_save = new Dictionary<string, KeyValuePair<TableInfo, List<object>>>();
        public Dictionary<string, List<KeyValuePair<UpdateInfo, Dictionary<int, object>>>> data_to_update = new Dictionary<string, List<KeyValuePair<UpdateInfo, Dictionary<int, object>>>>();
        public Dictionary<string, KeyValuePair<TableInfo, HashSet<int>>> data_to_delete = new Dictionary<string, KeyValuePair<TableInfo, HashSet<int>>>();

        //server
        public List<Command> commands { get; set; }

        public List<Action<string>> Callbacks { get; set; }
    }
    public class ModifyBatchTransaction
    {
        static object _trans_batch_lock = new object();
        static Dictionary<string, object> _trans_batch_locks = new Dictionary<string, object>();
        public static object GetTableTransBatchLock(string name)
        {
            lock (_trans_batch_lock)
            {
                if (!_trans_batch_locks.ContainsKey(name))
                {
                    _trans_batch_locks[name] = new object();
                }
                return _trans_batch_locks[name];
            }
        }
        //key: dic key: table_name
        public static ConcurrentDictionary<string, TransBatchData> _trans_batch = new ConcurrentDictionary<string, TransBatchData>();
    }
}
