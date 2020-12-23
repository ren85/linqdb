using ServerSharedData;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LinqDbClientInternal
{
    
    public partial class Ldb
    {
        public ClientResult PutToQueue<T>(T item)
        {
            var res = new ClientResult();
            res.Type = "queue";
            var sitem = SharedUtils.SerializeToBytes(item);
            sitem = SharedUtils.Compress(sitem);
            if (sitem.Length > 1000000)
            {
                throw new LinqDbException("Linqdb: item too big for a queue");
            }
            res.QueueData = sitem;
            return res;
        }
        public ClientResult Save<T>(T item, Dictionary<string, string> def, Dictionary<string, short> order)
        {
            var res = new ClientResult();
            res.Type = "save";
            res.Data = GetData<T>(new List<T>() { item }, def, order);
            return res;
        }
        public ClientResult SaveBatch<T>(List<T> items, Dictionary<string, string> def, Dictionary<string, short> order)
        {
            var res = new ClientResult();
            res.Type = "save";
            res.Data = GetData<T>(items, def, order);
            return res;
        }

        List<BinData> GetData<T>(List<T> items, Dictionary<string, string> def, Dictionary<string, short> order)
        {
            var res = new List<BinData>();
            int i = 0;
            foreach (var item in items)
            {
                res.Add(new BinData() { Bytes = new List<byte[]>() });
                foreach (var p in order.OrderBy(f => f.Value))
                {
                    var info = item.GetType().GetProperty(p.Key);
                    var value = info.GetValue(item);
                    var t = StringTypeToLinqType(def[p.Key]);
                    if (t == LinqDbTypes.string_)
                    {
                        if (value == null)
                        {
                            res[i].Bytes.Add(new byte[0]);
                        }
                        else
                        {
                            if (value is string)
                            {
                                res[i].Bytes.Add(Encoding.UTF8.GetBytes(value as string));
                            }
                            else
                            {
                                throw new LinqDbException("Linqdb: Column type cannot be changed: " + p.Key);
                            }
                        }
                    }
                    else if (t == LinqDbTypes.int_)
                    {
                        if (value == null)
                        {
                            res[i].Bytes.Add(new byte[0]);
                        }
                        else
                        {
                            if (value is int)
                            {
                                res[i].Bytes.Add(BitConverter.GetBytes((int)value));
                            }
                            else
                            {
                                throw new LinqDbException("Linqdb: Column type cannot be changed: " + p.Key);
                            }
                        }
                    }
                    else if (t == LinqDbTypes.double_)
                    {
                        if (value == null)
                        {
                            res[i].Bytes.Add(new byte[0]);
                        }
                        else
                        {
                            if (value is double)
                            {
                                res[i].Bytes.Add(BitConverter.GetBytes((double)value));
                            }
                            else
                            {
                                throw new LinqDbException("Linqdb: Column type cannot be changed: " + p.Key);
                            }
                        }
                    }
                    else if (t == LinqDbTypes.DateTime_)
                    {
                        if (value == null)
                        {
                            res[i].Bytes.Add(new byte[0]);
                        }
                        else
                        {
                            if (value is DateTime)
                            {
                                res[i].Bytes.Add(BitConverter.GetBytes(((DateTime)value - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds));
                            }
                            else
                            {
                                throw new LinqDbException("Linqdb: Column type cannot be changed: " + p.Key);
                            }
                        }
                    }
                    else if (t == LinqDbTypes.binary_)
                    {
                        if (value == null)
                        {
                            res[i].Bytes.Add(new byte[0]);
                        }
                        else
                        {
                            res[i].Bytes.Add((byte[])value);
                        }
                    }
                    else
                    {
                        res[i].Bytes.Add(new byte[0]);
                    }
                }
                i++;
            }

            return res;
        }

        public LinqDbTypes StringTypeToLinqType(string p)
        {

            if (p == "int" || p == "int?")
            {
                return LinqDbTypes.int_;
            }
            else if (p == "DateTime" || p == "DateTime?")
            {
                return LinqDbTypes.DateTime_;
            }
            else if (p == "double" || p == "double?")
            {
                return LinqDbTypes.double_;
            }
            else if (p == "byte[]")
            {
                return LinqDbTypes.binary_;
            }
            else if (p == "string")
            {
                return LinqDbTypes.string_;
            }
            else if (p == "bool" || p == "bool?")
            {
                throw new Exception("Linqdb: bool type is not supported, use int instead.");
            }
            else if (p == "decimal" || p == "decimal?")
            {
                throw new Exception("Linqdb: decimal type is not supported, use int or double instead.");
            }
            else if (p == "float" || p == "float?")
            {
                throw new Exception("Linqdb: float type is not supported, use double instead.");
            }
            else if (p == "long" || p == "long?")
            {
                throw new Exception("Linqdb: long type is not supported, use int instead.");
            }
            else
            {
                return LinqDbTypes.unknown_;
            }
        }

        object _def_lock = new object();
        Dictionary<string, Tuple<Dictionary<string, string>, Dictionary<string, short>>> _def_cache { get; set; }
        public void _InternalClearCache()
        {
            lock (_def_lock)
            {
                _def_cache = new Dictionary<string, Tuple<Dictionary<string, string>, Dictionary<string, short>>>();
            }
        }
        public Tuple<Dictionary<string, string>, Dictionary<string, short>> GetTableDefinition<T>()
        {
            if (_def_cache == null || !_def_cache.ContainsKey(typeof(T).Name))
            {
                lock (_def_lock)
                {
                    if (_def_cache == null || !_def_cache.ContainsKey(typeof(T).Name))
                    {
                        if (_def_cache == null)
                        {
                            _def_cache = new Dictionary<string, Tuple<Dictionary<string, string>, Dictionary<string, short>>>();
                        }
                        var tableDef = new Dictionary<string, string>();
                        var tableOrder = new Dictionary<string, short>();
                        var res = new Tuple<Dictionary<string, string>, Dictionary<string, short>>(tableDef, tableOrder);
                        
                        PropertyInfo[] properties = typeof(T).GetProperties();
                        short id = 0;
                        foreach (PropertyInfo property in properties)
                        {
                            string type = "";
                            switch (property.PropertyType.Name)
                            {
                                case "Int32":
                                    type = "int";
                                    break;
                                case "String":
                                    type = "string";
                                    break;
                                case "Byte[]":
                                    type = "byte[]";
                                    break;
                                case "Double":
                                    type = "double";
                                    break;
                                case "DateTime":
                                    type = "DateTime";
                                    break;
                                default:
                                    if (property.PropertyType.FullName.StartsWith("System.Nullable") && property.PropertyType.FullName.Contains("System.Int32"))
                                    {
                                        type = "int?";
                                    }
                                    else if (property.PropertyType.FullName.StartsWith("System.Nullable") && property.PropertyType.FullName.Contains("System.Double"))
                                    {
                                        type = "double?";
                                    }
                                    else if (property.PropertyType.FullName.StartsWith("System.Nullable") && property.PropertyType.FullName.Contains("System.DateTime"))
                                    {
                                        type = "DateTime?";
                                    }
                                    else
                                    {
                                        throw new LinqDbException("Property's '" + property.Name + "' type is not supported.");
                                    }
                                    break;
                            }
                            if (string.IsNullOrEmpty(type))
                            {
                                continue;
                            }
                            tableDef[property.Name] = type;
                            tableOrder[property.Name] = id;
                            id++;
                        }

                        _def_cache[typeof(T).Name] = res; //must be last in locked segment
                    }
                }
            }
            return new Tuple<Dictionary<string, string>, Dictionary<string, short>>(_def_cache[typeof(T).Name].Item1, _def_cache[typeof(T).Name].Item2);
        }
    }
    public enum LinqDbTypes
    {
        int_,
        double_,
        DateTime_,
        binary_,
        string_,
        unknown_
    }
}
