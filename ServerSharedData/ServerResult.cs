using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ServerSharedData
{
    public class ServerResultHelper
    {
        public static ServerResult GetServerResult(byte[] sr)
        {
            var res = new ServerResult();
            int current = 0;
            short slen = BitConverter.ToInt16(new byte[2] { sr[current], sr[current + 1] }, 0);
            current += 2;
            if (slen != 0)
            {
                res.SelectEntityResult = new Dictionary<string, Tuple<List<int>, List<byte>>>();
                for (int i = 0; i < slen; i++)
                {
                    short klen = BitConverter.ToInt16(new byte[2] { sr[current], 0 }, 0);
                    //res.SelectEntityResult = new Dictionary<string, Tuple<List<int>, List<byte>>>(klen);
                    current++;
                    byte[] k = new byte[klen];
                    for (int j = 0; j < klen; j++, current++)
                    {
                        k[j] = sr[current];
                    }
                    var key = Encoding.UTF8.GetString(k);
                    int ilen = BitConverter.ToInt32(new byte[4] { sr[current], sr[current + 1], sr[current + 2], sr[current + 3] }, 0);
                    var ids = new List<int>(ilen);
                    current += 4;
                    for (int z = 0; z < ilen; z++)
                    {
                        ids.Add(BitConverter.ToInt32(new byte[4] { sr[current], sr[current + 1], sr[current + 2], sr[current + 3] }, 0));
                        current += 4;
                    }
                    ilen = BitConverter.ToInt32(new byte[4] { sr[current], sr[current + 1], sr[current + 2], sr[current + 3] }, 0);
                    var list = new List<byte>(ilen);
                    current += 4;
                    for (int z = 0; z < ilen; z++)
                    {
                        list.Add(sr[current]);
                        current++;
                    }
                    res.SelectEntityResult[key] = new Tuple<List<int>, List<byte>>(ids, list);
                }
            }

            res.SelectGroupResult = new Dictionary<int, List<object>>();
            slen = BitConverter.ToInt16(new byte[2] { sr[current], sr[current + 1] }, 0);
            current += 2;
            if (slen != 0)
            {
                res.SelectGroupResult = new Dictionary<int, List<object>>(slen);
                for (int j = 0; j < slen; j++)
                {
                    int key = BitConverter.ToInt32(new byte[4] { sr[current], sr[current + 1], sr[current + 2], sr[current + 3] }, 0);
                    current += 4;
                    int count = BitConverter.ToInt32(new byte[4] { sr[current], sr[current + 1], sr[current + 2], sr[current + 3] }, 0);
                    current += 4;
                    var list = new List<object>(count);
                    for (int z = 0; z < count; z++)
                    {
                        var bcount = (int)sr[current];
                        current += 1;
                        if (bcount == 4)
                        {
                            var int_val = BitConverter.ToInt32(new byte[4] { sr[current], sr[current + 1], sr[current + 2], sr[current + 3] }, 0);
                            list.Add(int_val);
                            current += 4;
                        }
                        else
                        {
                            var double_val = BitConverter.ToDouble(new byte[8] { sr[current], sr[current + 1], sr[current + 2], sr[current + 3], sr[current + 4], sr[current + 5], sr[current + 6], sr[current + 7] }, 0);
                            list.Add(double_val);
                            current += 8;
                        }
                    }
                    res.SelectGroupResult[key] = list;
                }
            }

            res.Total = BitConverter.ToInt32(new byte[4] { sr[current], sr[current + 1], sr[current + 2], sr[current + 3] }, 0);
            current += 4;
            res.Count = BitConverter.ToInt32(new byte[4] { sr[current], sr[current + 1], sr[current + 2], sr[current + 3] }, 0);
            current += 4;
            int len = BitConverter.ToInt32(new byte[4] { sr[current], sr[current + 1], sr[current + 2], sr[current + 3] }, 0);
            current += 4;
            if (len != 0)
            {
                res.Ids = new List<int>(len);
                for (int i = 0; i < len; i++, current += 4)
                {
                    res.Ids.Add(BitConverter.ToInt32(new byte[4] { sr[current], sr[current + 1], sr[current + 2], sr[current + 3] }, 0));
                }
            }
            res.AllIds = sr[current] == 1;
            current++;
            res.LastStep = BitConverter.ToInt32(new byte[4] { sr[current], sr[current + 1], sr[current + 2], sr[current + 3] }, 0);
            current += 4;
            res.IsOrdered = sr[current] == 1;
            current++;
            len = BitConverter.ToInt32(new byte[4] { sr[current], sr[current + 1], sr[current + 2], sr[current + 3] }, 0);
            current += 4;
            if (len != 0)
            {
                res.OrderedIds = new Dictionary<int, int>();
                for (int i = 0; i < len; i++)
                {
                    var okey = BitConverter.ToInt32(new byte[4] { sr[current], sr[current + 1], sr[current + 2], sr[current + 3] }, 0);
                    current += 4;
                    var oval = BitConverter.ToInt32(new byte[4] { sr[current], sr[current + 1], sr[current + 2], sr[current + 3] }, 0);
                    current += 4;
                    res.OrderedIds[okey] = oval;
                }
            }
            byte old_val = sr[current];
            current++;
            if (old_val == 1)
            {
                res.Old_value = BitConverter.ToInt32(new byte[4] { sr[current], sr[current + 1], sr[current + 2], sr[current + 3] }, 0);
                current += 4;
            }
            slen = BitConverter.ToInt16(new byte[2] { sr[current], sr[current + 1] }, 0);
            current += 2;
            if (slen != 0)
            {
                byte[] k = new byte[slen];
                for (int j = 0; j < slen; j++, current++)
                {
                    k[j] = sr[current];
                }
                res.ServerError = Encoding.UTF8.GetString(k);
            }
            if (!string.IsNullOrEmpty(res.ServerError))
            {
                throw new Exception("Server " + res.ServerError);
            }
            slen = BitConverter.ToInt16(new byte[2] { sr[current], sr[current + 1] }, 0);
            current += 2;
            if (slen != 0)
            {
                byte[] k = new byte[slen];
                for (int j = 0; j < slen; j++, current++)
                {
                    k[j] = sr[current];
                }
                res.TableInfo = Encoding.UTF8.GetString(k);
            }
            sr = null;
            return res;
        }

        public static byte[] GetBytes(ServerResult sr)
        {
            var result = new List<byte>(50) { 0, 0, 0, 0 };
            byte[] tn;
            if (sr.SelectEntityResult != null && sr.SelectEntityResult.Any())
            {
                result.AddRange(BitConverter.GetBytes((short)sr.SelectEntityResult.Count()));
                foreach (var td in sr.SelectEntityResult)
                {
                    tn = Encoding.UTF8.GetBytes(td.Key);
                    result.Add((byte)tn.Length);
                    result.AddRange(tn);
                    result.AddRange(BitConverter.GetBytes(td.Value.Item1.Count()));
                    for(int i=0; i< td.Value.Item1.Count(); i++)
                    {
                        result.AddRange(BitConverter.GetBytes(td.Value.Item1[i]));
                    }
                    result.AddRange(BitConverter.GetBytes(td.Value.Item2.Count()));
                    result.AddRange(td.Value.Item2);
                }
            }
            else
            {
                result.Add(0);
                result.Add(0);
            }

            if (sr.SelectGroupResult != null && sr.SelectGroupResult.Any())
            {
                result.AddRange(BitConverter.GetBytes((short)sr.SelectGroupResult.Count()));
                foreach (var td in sr.SelectGroupResult)
                {
                    tn = BitConverter.GetBytes(td.Key);
                    result.AddRange(tn);
                    result.AddRange(BitConverter.GetBytes(td.Value.Count()));
                    for (int i = 0; i < td.Value.Count(); i++)
                    {
                        byte[] bytes = null;
                        if (td.Value[i] is double)
                        {
                            bytes = BitConverter.GetBytes((double)td.Value[i]);
                        }
                        else if (td.Value[i] is int)
                        {
                            bytes = BitConverter.GetBytes((int)td.Value[i]);
                        }
                        result.Add((byte)bytes.Count());
                        result.AddRange(bytes);
                    }
                }
            }
            else
            {
                result.Add(0);
                result.Add(0);
            }

            result.AddRange(BitConverter.GetBytes(sr.Total));
            result.AddRange(BitConverter.GetBytes(sr.Count));
            if (sr.Ids != null && sr.Ids.Any())
            {
                result.AddRange(BitConverter.GetBytes(sr.Ids.Count()));
                foreach (var id in sr.Ids)
                {
                    result.AddRange(BitConverter.GetBytes(id));
                }
            }
            else
            {
                result.AddRange(BitConverter.GetBytes(0));
            }
            result.Add(sr.AllIds ? (byte)1 : (byte)0);
            result.AddRange(BitConverter.GetBytes(sr.LastStep));
            result.Add(sr.IsOrdered ? (byte)1 : (byte)0);
            if (sr.OrderedIds != null && sr.OrderedIds.Any())
            {
                result.AddRange(BitConverter.GetBytes(sr.OrderedIds.Count()));
                foreach (var oid in sr.OrderedIds)
                {
                    result.AddRange(BitConverter.GetBytes(oid.Key));
                    result.AddRange(BitConverter.GetBytes(oid.Value));
                }
            }
            else
            {
                result.AddRange(BitConverter.GetBytes(0));
            }
            if (sr.Old_value != null)
            {
                result.Add(1);
                result.AddRange(BitConverter.GetBytes((int)sr.Old_value));
            }
            else
            {
                result.Add(0);
            }
            if (!string.IsNullOrEmpty(sr.ServerError))
            {
                tn = Encoding.UTF8.GetBytes(sr.ServerError);
                result.AddRange(BitConverter.GetBytes((short)sr.ServerError.Length));
                result.AddRange(tn);
            }
            else
            {
                result.Add(0);
                result.Add(0);
            }
            if (!string.IsNullOrEmpty(sr.TableInfo))
            {
                tn = Encoding.UTF8.GetBytes(sr.TableInfo);
                result.AddRange(BitConverter.GetBytes((short)sr.TableInfo.Length));
                result.AddRange(tn);
            }
            else
            {
                result.Add(0);
                result.Add(0);
            }

            var lb = BitConverter.GetBytes(result.Count - 4);
            result[0] = lb[0];
            result[1] = lb[1];
            result[2] = lb[2];
            result[3] = lb[3];
            sr = null;
            return result.ToArray();
        }
    }
    public class ServerResult
    {
        public Dictionary<string, Tuple<List<int>, List<byte>>> SelectEntityResult { get; set; }
        public int Total { get; set; }
        public int Count { get; set; }
        public List<int> Ids { get; set; }
        public bool AllIds { get; set; }
        public int LastStep { get; set; }
        public bool IsOrdered { get; set; }
        public Dictionary<int, int> OrderedIds { get; set; }
        public string ServerError { get; set; }
        public int? Old_value { get; set; }
        public string TableInfo { get; set; }
        public Dictionary<int, List<object>> SelectGroupResult { get; set; }
    }
}
