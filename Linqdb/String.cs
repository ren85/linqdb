using RocksDbSharp;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public int GetLastStep<T>(QueryTree tree)
        {
            CheckTableInfo<T>();
            var table_info = GetTableInfo(typeof(T).Name);
            return GetLastStep(table_info);
        }
        int GetLastStep(TableInfo table_info)
        {
            return GetMaxId(table_info.Name) / PhaseStep;
        }

        List<int> MakeSearch(string search_query, TableInfo table_info, string name, ReadOptions ro, bool partial, int? steps = null, int? start_step = null)
        {
            List<int> res = null;
            if (string.IsNullOrEmpty(search_query))
            {
                return new List<int>();
            }
            search_query = search_query.ToLower(CultureInfo.InstalledUICulture);
            foreach (var s in search_query.ToLower().Split().Where(f => !string.IsNullOrEmpty(f)))
            {
                var wres = new List<int>();
                using (var it = leveld_db.NewIterator(null, ro))
                {
                    var key = MakeStringIndexKey(new IndexKeyInfo()
                    {
                        ColumnNumber = table_info.ColumnNumbers[name],
                        TableNumber = table_info.TableNumber
                    }, s, (steps != null && start_step != null) ? (int)start_step : 0);

                    var common = new byte[key.Length - 4];
                    for (int i = 0; i < key.Length - 4; i++)
                    {
                        common[i] = key[i];
                    }
                    if (steps != null && start_step != null)
                    {
                        it.Seek(key);
                    }
                    else
                    {   
                        it.Seek(common);
                    }

                    while (true)
                    {
                        if (!it.Valid())
                        {
                            break;
                        }
                        var v = it.Key();
                        if (v == null || v.Length <= 5)
                        {
                            break;
                        }

                        if (steps != null && start_step != null)
                        {
                            var phase_bytes = new byte[4] { v[v.Length - 1], v[v.Length - 2], v[v.Length - 3], v[v.Length - 4] };
                            int phase = BitConverter.ToInt32(phase_bytes, 0);
                            if (phase < start_step)
                            {
                                it.Next();
                                continue;
                            }
                            if (phase > (start_step + steps) - 1)
                            {
                                break;
                            }
                        }
                        byte[] v_common = new byte[v.Length - 4];
                        for (int i = 0; i < v.Length - 4; i++)
                        {
                            v_common[i] = v[i];
                        }
                        if (!partial)
                        {
                            if (!ValsEqual(common, v_common))
                            {
                                break;
                            }
                        }
                        else
                        {
                            if (!ValsContains(common, v_common))
                            {
                                break;
                            }
                        }
                        var val = it.Value();
                        if (val != null)
                        {
                            wres.AddRange(ReadHashsetFromBytesList(val));
                        }
                        it.Next();
                    }
                }

                if (res == null)
                {
                    res = new List<int>(wres);
                }
                else
                {
                    res = res.MyIntersect(wres);
                }
            }

            if (res == null)
            {
                return new List<int>();
            }
            else
            {
                return res;
            }
        }
        int PhaseStep = 1000;
        void UpdateIndex(string old_val, string new_val, int id, WriteBatchWithConstraints batch, short ColumnNumber, short TableNumber, Dictionary<string, KeyValuePair<byte[], HashSet<int>>> cache)
        {
            int phase = id / PhaseStep;
            if (old_val != null)
            {
                old_val = old_val.ToLower(CultureInfo.InvariantCulture);
            }
            if (new_val != null)
            {
                new_val = new_val.ToLower(CultureInfo.InvariantCulture);
            }

            if (old_val == new_val)
            {
                return;
            }
            var removed = GetRemovedWords(old_val, new_val);
            foreach (var r in removed)
            {
                var kinfo = new IndexKeyInfo()
                {
                    ColumnNumber = ColumnNumber,
                    TableNumber = TableNumber,
                    Id = id
                };
                var key = MakeStringIndexKey(kinfo, r, phase);
                HashSet<int> old_index = null;
                if (GetFromStringIndexCache(kinfo, r, cache, phase, out old_index, 0))
                {
                    old_index.Add(id);
                    PutToStringIndexCache(kinfo, r, key, old_index, cache, phase, 0);
                }
                else
                {
                    //var val = leveld_db.Get(key);
                    //if (val != null)
                    //{
                    //  old_index = ReadHashsetFromBytes(val);
                        old_index = new HashSet<int>();
                        old_index.Add(id);
                        PutToStringIndexCache(kinfo, r, key, old_index, cache, phase, 0);
                    //}
                }
            }
            var new_words = GetAddedWords(old_val, new_val);
            foreach (var n in new_words)
            {
                var kinfo = new IndexKeyInfo()
                {
                    ColumnNumber = ColumnNumber,
                    TableNumber = TableNumber,
                    Id = id
                };
                var key = MakeStringIndexKey(kinfo, n, phase);
                HashSet<int> old_index = null;

                if (GetFromStringIndexCache(kinfo, n, cache, phase, out old_index, 1))
                {
                    old_index.Add(id);
                    PutToStringIndexCache(kinfo, n, key, old_index, cache, phase, 1);
                }
                else
                {
                    //var val = leveld_db.Get(key);
                    //if (val != null)
                    //{
                    //    old_index = ReadHashsetFromBytes(val);
                    //    old_index.Add(id);
                    //    PutToStringIndexCache(kinfo, n, key, old_index, cache, phase);
                    //}
                    //else
                    //{
                        var index = new HashSet<int>();
                        index.Add(id);
                        PutToStringIndexCache(kinfo, n, key, index, cache, phase, 1);
                    //}
                }
            }
        }
        void Updateue(string old_val, string val, int id, WriteBatchWithConstraints batch, TableInfo table_info, string name)
        {
            if (old_val != null && old_val == val)
            {
                return;
            }
            var key = MakeStringValueKey(new IndexKeyInfo()
            {
                ColumnNumber = table_info.ColumnNumbers[name],
                TableNumber = table_info.TableNumber,
                Id = id
            });
            if (val != null)
            {
                var vb = Encoding.Unicode.GetBytes(val);
                byte[] br = new byte[1 + vb.Length];
                br[0] = BinaryOrStringValuePrefix[0];
                for (int i = 0; i < vb.Length; i++)
                {
                    br[i + 1] = vb[i];
                }
                batch.Put(key, br);
            }
            else
            {
                batch.Put(key, BinaryOrStringValuePrefix);
            }
        }

        static string AsciiNonReadableString
        {
            get
            {
                return "~#@$^&*=,.!?:;-\"'`()[]<>{}/·–|↑•%°„_\\ ";
            }
        }
        //ConcurrentDictionary<char, byte> _asciiNonReadable = new ConcurrentDictionary<char, byte>();
        //public ConcurrentDictionary<char, byte> AsciiNonReadable
        //{
        //    get
        //    {
        //        if (_asciiNonReadable == null)
        //        {
        //            _asciiNonReadable = new ConcurrentDictionary<char, byte>(AsciiNonReadableString.Select(f => new KeyValuePair<char, byte>(f, (byte)1)));
        //        }
        //        return _asciiNonReadable;
        //    }
        //}

        HashSet<string> GetDistinctWords(string val)
        {
            if (string.IsNullOrEmpty(val))
            {
                return new HashSet<string>();
            }

            var res = new HashSet<string>(val.Split().Where(f => !string.IsNullOrEmpty(f)));
            var more = new HashSet<string>(val.Trim(AsciiNonReadableString.ToArray())
                                              .Split(AsciiNonReadableString.ToArray(), StringSplitOptions.RemoveEmptyEntries)
                                              .Select(f => f.Trim())
                                              .Where(f => !string.IsNullOrEmpty(f)));
            res.UnionWith(more);

            //var long_words = res.Where(f => f.Length > 5).ToList();
            //foreach (var w in long_words)
            //{
            //    var cw = w.Substring(0, 4);
            //    for (int i = 4; i < w.Length; i++)
            //    {
            //        cw += w[i];
            //        res.Add(cw);
            //    }
            //}

            return res;
        }

        HashSet<string> GetAddedWords(string old_val, string new_val)
        {
            var oldw = GetDistinctWords(old_val);
            var neww = GetDistinctWords(new_val);

            neww.ExceptWith(oldw);

            return neww;
        }

        HashSet<string> GetRemovedWords(string old_val, string new_val)
        {
            var oldw = GetDistinctWords(old_val);
            var neww = GetDistinctWords(new_val);

            oldw.ExceptWith(neww);

            return oldw;
        }

        byte[] WriteHashsetToBytes(HashSet<int> set)
        {
            using (var ms = new MemoryStream())
            using (var bw = new BinaryWriter(ms))
            {
                bw.Write(set.Count());
                foreach (var i in set)
                {
                    bw.Write(i);
                }
                return ms.ToArray();
            }
        }

        HashSet<int> ReadHashsetFromBytes(byte[] bytes)
        {
            using (var ms = new MemoryStream(bytes))
            using (var br = new BinaryReader(ms))
            {
                var count = br.ReadInt32();
                int[] tmp = new int[count];
                for (int i = 0; i < count; i++)
                {
                    var val = br.ReadInt32();
                    tmp[i] = val;
                }
                return new HashSet<int>(tmp);
            }
        }
        int[] ReadHashsetFromBytesList(byte[] bytes)
        {
            using (var ms = new MemoryStream(bytes))
            using (var br = new BinaryReader(ms))
            {
                var count = br.ReadInt32();
                int[] tmp = new int[count];
                for (int i = 0; i < count; i++)
                {
                    var val = br.ReadInt32();
                    tmp[i] = val;
                }
                return tmp;
            }
        }
    }

    //public class StringIndexCache
    //{
    //    public Dictionary<string, KeyValuePair<byte[], HashSet<int>>> to_insert = new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>();
    //    public Dictionary<string, KeyValuePair<byte[], HashSet<int>>>  to_remove
    //}
}
