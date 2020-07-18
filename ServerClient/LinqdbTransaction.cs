using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LinqDbClientInternal
{
    public class LinqdbTransactionInternal : IDisposable
    {
        public Ldb _db { get; set; }
        public int CommandCount { get; set; }

        public Dictionary<string, Tuple<Dictionary<string, string>, Dictionary<string, short>>> defs = new Dictionary<string, Tuple<Dictionary<string, string>, Dictionary<string, short>>>();
        public Dictionary<string, List<object>> data_to_save = new Dictionary<string, List<object>>();
        public Dictionary<string, List<KeyValuePair<string, Dictionary<int, byte[]>>>> data_to_update = new Dictionary<string, List<KeyValuePair<string, Dictionary<int, byte[]>>>>();
        public Dictionary<string, HashSet<int>> data_to_delete = new Dictionary<string, HashSet<int>>();

        public List<byte> Data { get; set; }
        public HashSet<string> _incremented { get; set; }
        public LinqdbTransactionInternal()
        {
            Data = new List<byte>() { 0, 0, 0, 0, (byte)CommandType.Transaction };
            _incremented = new HashSet<string>();
        }
        public void Commit()
        {
            var ids = new Dictionary<string, HashSet<int>>();
            var update_ids = new Dictionary<string, HashSet<int>>();

            if (!data_to_save.Any() && !data_to_update.Any() && !data_to_delete.Any())
            {
                return;
            }
            //save
            foreach (var ent in data_to_save)
            {
                if (!ids.ContainsKey(ent.Key))
                {
                    ids[ent.Key] = new HashSet<int>();
                }
                foreach (var item in ent.Value)
                {
                    var id = Convert.ToInt32(item.GetType().GetProperty("Id").GetValue(item));
                    if (ids[ent.Key].Contains(id))
                    {
                        throw new LinqDbException("Linqdb: same entity cannot be modified twice in a transaction. " + ent.Key + ", id " + id);
                    }
                    else
                    {
                        ids[ent.Key].Add(id);
                    }
                }

                CommandCount++;
                var result = _db.SaveBatch(ent.Value, defs[ent.Key].Item1, defs[ent.Key].Item2);
                Command cm = CommandHelper.GetCommand(new List<ClientResult>() { result }, defs[ent.Key].Item1, defs[ent.Key].Item2, ent.Key, _db.User, _db.Pass);
                var bcm = CommandHelper.GetBytes(cm, true);
                Data.AddRange(BitConverter.GetBytes(bcm.Length));
                Data.AddRange(bcm);
            }

            //update
            foreach (var ent in data_to_update)
            {
                if (!ids.ContainsKey(ent.Key))
                {
                    ids[ent.Key] = new HashSet<int>();
                }
                foreach (var selector in ent.Value)
                {
                    if (!update_ids.ContainsKey(ent.Key + "|" + selector.Key))
                    {
                        update_ids[ent.Key + "|" + selector.Key] = new HashSet<int>();
                    }
                    foreach (var id in selector.Value.Keys)
                    {
                        if (ids[ent.Key].Contains(id))
                        {
                            throw new LinqDbException("Linqdb: same entity cannot be modified twice in a transaction. " + ent.Key + ", id " + id);
                        }
                        if (update_ids[ent.Key + "|" + selector.Key].Contains(id))
                        {
                            throw new LinqDbException("Linqdb: same entity's field cannot be updated twice in a transaction. " + ent.Key + ", field " + selector.Key + ", id " + id);
                        }
                        else
                        {
                            update_ids[ent.Key + "|" + selector.Key].Add(id);
                        }
                    }

                    CommandCount++;
                    var result = _db.GenericUpdate(selector.Key, selector.Value);
                    Command cm = CommandHelper.GetCommand(new List<ClientResult>() { result }, defs[ent.Key].Item1, defs[ent.Key].Item2, ent.Key, _db.User, _db.Pass);
                    var bcm = CommandHelper.GetBytes(cm, true);
                    Data.AddRange(BitConverter.GetBytes(bcm.Length));
                    Data.AddRange(bcm);
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
            }

            //delete
            foreach (var ent in data_to_delete)
            {
                if (!ids.ContainsKey(ent.Key))
                {
                    ids[ent.Key] = new HashSet<int>();
                }
                foreach (var id in ent.Value)
                {
                    if (ids[ent.Key].Contains(id))
                    {
                        throw new LinqDbException("Linqdb: same entity cannot be modified twice in a transaction. " + ent.Key + ", id " + id);
                    }
                    else
                    {
                        ids[ent.Key].Add(id);
                    }
                }
                CommandCount++;
                var result = _db.DeleteBatch(ent.Value);
                Command cm = CommandHelper.GetCommand(new List<ClientResult>() { result }, defs[ent.Key].Item1, defs[ent.Key].Item2, ent.Key, _db.User, _db.Pass);
                var bcm = CommandHelper.GetBytes(cm, true);
                Data.AddRange(BitConverter.GetBytes(bcm.Length));
                Data.AddRange(bcm);
            }

            Data.AddRange(BitConverter.GetBytes(CommandCount));
            byte[] length = BitConverter.GetBytes(Data.Count - 4);
            Data[0] = length[0];
            Data[1] = length[1];
            Data[2] = length[2];
            Data[3] = length[3];
            var bres = _db.CallServer(Data.ToArray());
            var res_obj = ServerResultHelper.GetServerResult(bres);
        }
        public void Dispose()
        {
            data_to_save = null;
            data_to_update = null;
            data_to_delete = null;
        }
    }
}
