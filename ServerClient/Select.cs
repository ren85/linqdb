using ServerSharedData;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbClientInternal
{
    public partial class Ldb
    {
        Func<byte[], byte[]> _callServer = null;
        public ClientSockets Sock { get; set; }
        public Func<byte[], byte[]> CallServer
        {
            get
            {
                return _callServer;
            }
            set
            {
                if (_callServer == null)
                {
                    _callServer = value;
                }
            }
        }

        public ClientResult GetFromQueue<T>()
        {
            var res = new ClientResult();
            res.Type = "getqueue";
            return res;
        }

        public ClientResult SelectEntity<T>()
        {
            var res = new ClientResult();
            res.Type = "select";
            return res;
        }

        public ClientResult Select<T, R>(Expression<Func<T, R>> predicate)
        {
            var res = new ClientResult();
            res.Type = "selectanon";
            res.AnonSelect = new List<string>();
            var body = predicate.Body;
            var new_expr = body as NewExpression;
            if (new_expr == null)
            {
                throw new LinqDbException("Linqdb: Select must be with anonymous type, for example, .Select(f => new { Id = f.Id })");
            }
            foreach (var a in new_expr.Arguments)
            {
                var name = a.ToString().Split(".".ToCharArray())[1];
                res.AnonSelect.Add(name);
            }
            return res;
        }

        public ClientResult SelectGrouped<T, R>(Expression<Func<T, R>> predicate, int? total = null)
        {
            var res = new ClientResult();
            res.Type = "selectgrouped";
            res.AnonSelect = new List<string>();
            var body = predicate.Body;
            var new_expr = body as NewExpression;
            if (new_expr == null)
            {
                throw new LinqDbException("Linqdb: Select must be with anonymous type, for example, .Select(f => new { f.Key })");
            }
            foreach (var a in new_expr.Arguments)
            {
                //var name = a.ToString().Split(".".ToCharArray())[1];
                res.AnonSelect.Add(a.ToString());
            }
            return res;
        }

        public ClientResult GetLastStep<T>()
        {
            var res = new ClientResult();
            res.Type = "last";
            return res;
        }

        ConcurrentDictionary<string, object> compiled_cache = new ConcurrentDictionary<string, object>();
        public T CreateType<T>(CompiledInfo<T> info, List<object> fields)
        {
            string key = info.ToString();
            if (!compiled_cache.ContainsKey(key))
            {
                compiled_cache[key] = info.GetActivator();
            }
            //create an instance:
            T instance = (compiled_cache[key] as CompiledInfo<T>.ObjectActivator)(fields.ToArray());
            return instance;
        }


        static bool ValsEqual(byte[] a, byte[] b)
        {
            if (a.Length != b.Length)
            {
                return false;
            }
            for (int i = 0; i < a.Length; i++)
            {
                if (a[i] != b[i])
                {
                    return false;
                }
            }
            return true;
        }
        static byte[] NullConstant = new List<byte>() { (byte)45 }.ToArray();
        //static string NullConstantString = Convert.ToBase64String(new List<byte>() { (byte)45 }.ToArray());
        static byte[] MyReverseWithCopy(byte[] val)
        {
            if (val == null || val.Length == 0)
            {
                return val;
            }

            var res = new byte[val.Length];
            for (int i = 0; i < val.Length; i++)
            {
                res[i] = val[val.Length - i - 1];
            }

            return res;
        }
        public static List<R> CreateGrouppedAnonymousType<R>(ServerResult server_res, Tuple<Dictionary<string, string>, Dictionary<string, short>> def, Ldb _db, List<string> props)
        {
            CompiledInfo<R> cinfo = new CompiledInfo<R>();
            var cdata = server_res.SelectGroupResult;
            HashSet<int> distinct_groups = new HashSet<int>(cdata.Select(f => f.Key));

            List<R> result = new List<R>();
            foreach (var gr in distinct_groups)
            {
                var cresult = _db.CreateType<R>(cinfo, cdata[gr]);
                result.Add(cresult);
            }

            return result;
        }
        public static List<R> CreateAnonymousType<R>(ServerResult server_res, Tuple<Dictionary<string, string>, Dictionary<string, short>> def, Ldb _db, List<string> props)
        {
            List<R> result = new List<R>();
            List<KeyValuePair<int, R>> ordered_result = new List<KeyValuePair<int, R>>();
            var data = server_res.SelectEntityResult;
            var fv = data[data.Keys.First()];
            if (fv == null)
            {
                return new List<R>();
            }
            int[] currents = new int[def.Item1.Count() + 1];
            int[] currents_ids = new int[def.Item1.Count() + 1];
            int id_nr = 0;
            Tuple<List<int>, List<byte>>[] data_array = new Tuple<List<int>, List<byte>>[def.Item1.Count() + 1];
            int c = -1;
            foreach (var item in def.Item1)
            {
                c++;
                if (item.Key == "Id")
                {
                    id_nr = c;
                }
                currents[c] = 0;
                currents_ids[c] = 0;
            }

            int cc_ = -1;
            var cc = new Dictionary<string, int>();
            foreach (var name in props)
            {
                cc_++;
                if (cc_ == id_nr && name != "Id")
                {
                    cc_++;
                }
                data_array[cc_] = data[name];
                cc[name] = cc_;
            }
            cc["Id"] = id_nr;
            data_array[id_nr] = data["Id"];
            for (int i = 0; i < server_res.Count; i++)
            {
                int id = data_array[id_nr].Item1[currents_ids[id_nr]];
                if (!props.Contains("Id"))
                {
                    currents_ids[id_nr]++;
                }
                List<object> cdata = new List<object>();
                foreach (var name in props)
                {
                    int nr = cc[name];
                    if (!data.ContainsKey(name))
                    {
                        continue;
                    }
                    var type = _db.StringTypeToLinqType(def.Item1[name]);
                    var by = data_array[nr];
                    byte[] val = null;
                    bool skip = false;
                    if (currents_ids[nr] >= by.Item1.Count || id < by.Item1[currents_ids[nr]])
                    {
                        skip = true;
                    }
                    else
                    {
                        currents_ids[nr]++;
                    }
                    if (by.Item2.Any() && !skip)
                    {
                        if (type == LinqDbTypes.int_)
                        {
                            if (by.Item2[currents[nr]] == 1)
                            {
                                currents[nr]++;
                                val = new byte[4];
                                for (int j = 0; j < 4; j++)
                                {
                                    val[j] = by.Item2[currents[nr]];
                                    currents[nr]++;
                                }
                            }
                            else if (by.Item2[currents[nr]] == 250)
                            {
                                currents[nr]++;
                                val = new byte[1];
                                val[0] = NullConstant[0];
                            }
                            else
                            {
                                currents[nr]++;
                            }
                        }
                        else if (type == LinqDbTypes.double_ || type == LinqDbTypes.DateTime_)
                        {
                            if (by.Item2[currents[nr]] == 1)
                            {
                                currents[nr]++;
                                val = new byte[8];
                                for (int j = 0; j < 8; j++)
                                {
                                    val[j] = by.Item2[currents[nr]];
                                    currents[nr]++;
                                }
                            }
                            else if (by.Item2[currents[nr]] == 250)
                            {
                                currents[nr]++;
                                val = new byte[1];
                                val[0] = NullConstant[0];
                            }
                            else
                            {
                                currents[nr]++;
                            }
                        }
                        else
                        {
                            int size = BitConverter.ToInt32(new byte[4] { by.Item2[currents[nr]], by.Item2[currents[nr] + 1], by.Item2[currents[nr] + 2], by.Item2[currents[nr] + 3] }, 0);
                            currents[nr] += 4;
                            if (size > 0)
                            {
                                val = new byte[size];
                                for (int j = 0; j < size; j++)
                                {
                                    val[j] = by.Item2[currents[nr]];
                                    currents[nr]++;
                                }
                            }
                            else if (size == -2)
                            {
                                val = new byte[1];
                                val[0] = NullConstant[0];
                            }
                        }
                    }
                    if (type != LinqDbTypes.binary_ && type != LinqDbTypes.string_)
                    {
                        if (val == null)
                        {
                            switch (type)
                            {
                                case LinqDbTypes.DateTime_:
                                    cdata.Add(new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc));
                                    break;
                                case LinqDbTypes.double_:
                                    cdata.Add((double)0);
                                    break;
                                case LinqDbTypes.int_:
                                    cdata.Add(0);
                                    break;
                                default:
                                    throw new LinqDbException("Linqdb: Unsupported type");
                            }
                        }
                        else if (ValsEqual(val, NullConstant))
                        {
                            cdata.Add(null);
                        }
                        else
                        {
                            switch (type)
                            {
                                case LinqDbTypes.DateTime_:
                                    cdata.Add(new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(BitConverter.ToDouble(MyReverseWithCopy(val), 0)));
                                    break;
                                case LinqDbTypes.double_:
                                    cdata.Add(BitConverter.ToDouble(MyReverseWithCopy(val), 0));
                                    break;
                                case LinqDbTypes.int_:
                                    int int_v = BitConverter.ToInt32(MyReverseWithCopy(val), 0);
                                    cdata.Add(int_v);
                                    break;
                                default:
                                    throw new LinqDbException("Linqdb: Unsupported type");
                            }
                        }
                    }
                    else if (type == LinqDbTypes.binary_)
                    {
                        if (val == null || val.Length == 1)
                        {
                            cdata.Add(null);
                        }
                        else
                        {
                            cdata.Add(val.Skip(1).ToArray());
                        }
                    }
                    else
                    {
                        if (val == null || val.Length == 1)
                        {
                            cdata.Add(null);
                        }
                        else
                        {
                            cdata.Add(Encoding.Unicode.GetString(val.Skip(1).ToArray()));
                        }
                    }
                }

                CompiledInfo<R> cinfo = new CompiledInfo<R>();
                if (server_res.IsOrdered)
                {
                    var cresult = _db.CreateType<R>(cinfo, cdata);
                    //if (id != null)
                    //{
                        ordered_result.Add(new KeyValuePair<int, R>((int)id, cresult));
                    //}
                    //else
                    //{
                    //    var by = data["Id"];
                    //    currents["Id"] += 1;
                    //    byte[] val = new byte[4];
                    //    for (int j = 0; j < 4; j++)
                    //    {
                    //        val[j] = by[currents["Id"]];
                    //        currents["Id"]++;
                    //    }
                    //    id = BitConverter.ToInt32(MyReverseWithCopy(val), 0);
                    //    ordered_result.Add(new KeyValuePair<int, R>((int)id, cresult));
                    //}
                }
                else
                {
                    var cresult = _db.CreateType<R>(cinfo, cdata);
                    result.Add(cresult);
                }
            }

            if (server_res.IsOrdered)
            {
                result = ordered_result.OrderBy(f => server_res.OrderedIds[f.Key]).Select(f => f.Value).ToList();
            }

            return result;
        }

        public static List<T> CreateType<T>(ServerResult server_res, Tuple<Dictionary<string, string>, Dictionary<string, short>> def, Ldb _db) where T : new()
        {
            List<T> result = new List<T>();
            List<KeyValuePair<int, T>> ordered_result = new List<KeyValuePair<int, T>>();
            var data = server_res.SelectEntityResult;
            var fv = data[data.Keys.First()];
            if (fv == null || fv.Item2 == null)
            {
                return new List<T>();
            }
            //current_ids fuss is about the fact that any column may have data gaps in multiple places (column added, removed, added ...)
            //the only column that doesn't have gaps is Id
            //assumption is that ids in data[name].Item1 is in ascending order      
            int[] currents = new int[def.Item2.Count()];
            int[] currents_ids = new int[def.Item2.Count()];
            Tuple<List<int>, List<byte>>[] data_array = new Tuple<List<int>, List<byte>>[def.Item2.Count()];
            int id_nr = 0;
            int c = -1;
            foreach (var name in def.Item2.OrderBy(f => f.Value).Select(f => f.Key))
            {
                c++;
                if (name == "Id")
                {
                    id_nr = c;
                }
                currents[c] = 0;
                currents_ids[c] = 0;
                data_array[c] = data[name];
            }
            for (int i = 0; i < server_res.Count; i++)
            {
                int id = data_array[id_nr].Item1[currents_ids[id_nr]];
                List<object> cdata = new List<object>();
                int cc = -1;
                foreach (var name in def.Item2.OrderBy(f => f.Value).Select(f => f.Key))
                {
                    cc++;
                    var type = _db.StringTypeToLinqType(def.Item1[name]);
                    var by = data_array[cc];
                    byte[] val = null;
                    bool skip = false;
                    if (currents_ids[cc] >= by.Item1.Count || id < by.Item1[currents_ids[cc]])
                    {
                        skip = true;
                    }
                    else
                    {
                        currents_ids[cc]++;
                    }
                    if (by.Item2.Any() && !skip)
                    {
                        if (type == LinqDbTypes.int_)
                        {
                            if (by.Item2[currents[cc]] == 1)
                            {
                                currents[cc]++;
                                val = new byte[4];
                                for (int j = 0; j < 4; j++)
                                {
                                    val[j] = by.Item2[currents[cc]];
                                    currents[cc]++;
                                }
                            }
                            else if (by.Item2[currents[cc]] == 250)
                            {
                                currents[cc]++;
                                val = new byte[1];
                                val[0] = NullConstant[0];
                            }
                            else
                            {
                                currents[cc]++;
                            }
                        }
                        else if (type == LinqDbTypes.double_ || type == LinqDbTypes.DateTime_)
                        {
                            if (by.Item2[currents[cc]] == 1)
                            {
                                currents[cc]++;
                                val = new byte[8];
                                for (int j = 0; j < 8; j++)
                                {
                                    val[j] = by.Item2[currents[cc]];
                                    currents[cc]++;
                                }
                            }
                            else if (by.Item2[currents[cc]] == 250)
                            {
                                currents[cc]++;
                                val = new byte[1];
                                val[0] = NullConstant[0];
                            }
                            else
                            {
                                currents[cc]++;
                            }
                        }
                        else
                        {
                            int size = BitConverter.ToInt32(new byte[4] { by.Item2[currents[cc]], by.Item2[currents[cc] + 1], by.Item2[currents[cc] + 2], by.Item2[currents[cc] + 3] }, 0);
                            currents[cc] += 4;
                            if (size > 0)
                            {
                                val = new byte[size];
                                for (int j = 0; j < size; j++)
                                {
                                    val[j] = by.Item2[currents[cc]];
                                    currents[cc]++;
                                }
                            }
                            else if (size == -2)
                            {
                                val = new byte[1];
                                val[0] = NullConstant[0];
                            }
                        }
                    }
                    if (type != LinqDbTypes.binary_ && type != LinqDbTypes.string_)
                    {
                        if (val == null)
                        {
                            switch (type)
                            {
                                case LinqDbTypes.DateTime_:
                                    cdata.Add(new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc));
                                    break;
                                case LinqDbTypes.double_:
                                    cdata.Add((double)0);
                                    break;
                                case LinqDbTypes.int_:
                                    cdata.Add(0);
                                    break;
                                default:
                                    throw new LinqDbException("Linqdb: Unsupported type");
                            }
                        }
                        else if (ValsEqual(val, NullConstant))
                        {
                            cdata.Add(null);
                        }
                        else
                        {
                            switch (type)
                            {
                                case LinqDbTypes.DateTime_:
                                    cdata.Add(new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(BitConverter.ToDouble(MyReverseWithCopy(val), 0)));
                                    break;
                                case LinqDbTypes.double_:
                                    cdata.Add(BitConverter.ToDouble(MyReverseWithCopy(val), 0));
                                    break;
                                case LinqDbTypes.int_:
                                    int int_v = BitConverter.ToInt32(MyReverseWithCopy(val), 0);
                                    //if (name == "Id")
                                    //{
                                    //    id = int_v;
                                    //}
                                    cdata.Add(int_v);
                                    break;
                                default:
                                    throw new LinqDbException("Linqdb: Unsupported type");
                            }
                        }
                    }
                    else if (type == LinqDbTypes.binary_)
                    {
                        if (val == null || val.Length == 1)
                        {
                            cdata.Add(null);
                        }
                        else
                        {
                            cdata.Add(val.Skip(1).ToArray());
                        }
                    }
                    else
                    {
                        if (val == null || val.Length == 1)
                        {
                            cdata.Add(null);
                        }
                        else
                        {
                            cdata.Add(Encoding.Unicode.GetString(val.Skip(1).ToArray()));
                        }
                    }
                }
                if (server_res.IsOrdered)
                {
                    var cresult = new T();
                    int j = 0;
                    var ps = typeof(T).GetProperties().ToDictionary(f => f.Name, z => z);
                    foreach (var propInfo in def.Item2.OrderBy(f => f.Value).Select(f => f.Key))
                    {
                        ps[propInfo].SetValue(cresult, cdata[j]);
                        j++;
                    }
                    ordered_result.Add(new KeyValuePair<int, T>(id, cresult));
                }
                else
                {
                    var cresult = new T();
                    int j = 0;
                    var ps = typeof(T).GetProperties().ToDictionary(f => f.Name, z => z);
                    foreach (var propInfo in def.Item2.OrderBy(f => f.Value).Select(f => f.Key))
                    {
                        ps[propInfo].SetValue(cresult, cdata[j]);
                        j++;
                    }
                    result.Add(cresult);
                }
            }

            if (server_res.IsOrdered)
            {
                result = ordered_result.OrderBy(f => server_res.OrderedIds[f.Key]).Select(f => f.Value).ToList();
            }
            return result;
        }
    }
    public class CompiledInfo<T>
    {
        public delegate T ObjectActivator(params object[] args);
        public ObjectActivator Compiled
        {
            get;
            set;
        }
        public ObjectActivator GetActivator()
        {
            if (Compiled != null)
            {
                return Compiled;
            }
            var ctor = typeof(T).GetConstructors().First();
            Type type = ctor.DeclaringType;
            ParameterInfo[] paramsInfo = ctor.GetParameters();

            //create a single param of type object[]
            ParameterExpression param = Expression.Parameter(typeof(object[]), "args");

            Expression[] argsExp = new Expression[paramsInfo.Length];

            //pick each arg from the params array 
            //and create a typed expression of them
            for (int i = 0; i < paramsInfo.Length; i++)
            {
                Expression index = Expression.Constant(i);
                Type paramType = paramsInfo[i].ParameterType;

                Expression paramAccessorExp = Expression.ArrayIndex(param, index);

                Expression paramCastExp = Expression.Convert(paramAccessorExp, paramType);

                argsExp[i] = paramCastExp;
            }

            //make a NewExpression that calls the
            //ctor with the args we just created
            NewExpression newExp = Expression.New(ctor, argsExp);

            //create a lambda with the New
            //Expression as body and our param object[] as arg
            LambdaExpression lambda = Expression.Lambda(typeof(ObjectActivator), newExp, param);

            //compile it
            ObjectActivator compiled = (ObjectActivator)lambda.Compile();
            Compiled = compiled;
            return compiled;
        }
    }
}
