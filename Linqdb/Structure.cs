using RocksDbSharp;
using ProtoBuf;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        ConcurrentDictionary<string, TableInfo> struct_cache { get; set; }
        TableInfo GetTableInfo(string name)
        {
            if (struct_cache.ContainsKey(name))
            {
                return struct_cache[name];
            }

            var t = struct_db.Get(Encoding.UTF8.GetBytes(name));
            if (t == null)
            {
                return null;
            }
            else
            {
                using (var memoryStream = new MemoryStream(t))
                {
                    var info = Serializer.Deserialize<TableInfo>(memoryStream);
                    struct_cache[name] = info;
                    return info;
                }
            }
        }
        object _lock = new object();
        void UpdateTableInfo(string name, TableInfo new_info, bool can_write)
        {
            var old_info = GetTableInfo(name);
            if (old_info == null)
            {
                if (!can_write)
                {
                    throw new LinqDbException("Linqdb: insufficient permissions for type creation");
                }
                lock (_lock)
                {
                    old_info = GetTableInfo(name);
                    if (old_info == null)
                    {
                        new_info.TableNumber = GetNewTableCount();
                        new_info.ColumnNumbers = new ConcurrentDictionary<string, short>();
                        foreach (var c in new_info.Columns)
                        {
                            new_info.ColumnNumbers[c.Key] = GetColumnsNumber(new_info.TableNumber, c.Key, c.Value);
                        }
                        using (var memoryStream = new MemoryStream())
                        {
                            Serializer.Serialize(memoryStream, new_info);
                            struct_db.Put(Encoding.UTF8.GetBytes(name), memoryStream.ToArray());
                            struct_cache[name] = new_info;
                        }
                    }
                    else
                    {
                        new_info.TableNumber = old_info.TableNumber;
                        new_info.ColumnNumbers = old_info.ColumnNumbers;
                    }
                }
            }
            else
            {
                bool needs_updating = false;
                foreach (var c in new_info.Columns)
                {
                    if (old_info.Columns.ContainsKey(c.Key) && old_info.Columns[c.Key] != new_info.Columns[c.Key])
                    {
                        throw new LinqDbException("Linqdb: Column type cannot be changed: " + c.Key);
                    }
                }
                foreach (var c in new_info.Columns)
                {
                    if (!old_info.Columns.ContainsKey(c.Key))
                    {
                        needs_updating = true;
                        break;
                    }
                }

                if (needs_updating)
                {
                    if (!can_write)
                    {
                        throw new LinqDbException("Linqdb: insufficient permissions for adding new columns.");
                    }
                    new_info.TableNumber = old_info.TableNumber;
                    new_info.ColumnNumbers = new ConcurrentDictionary<string, short>();
                    foreach (var c in old_info.Columns)
                    {
                        if (new_info.Columns.ContainsKey(c.Key))
                        {
                            new_info.ColumnNumbers[c.Key] = old_info.ColumnNumbers[c.Key];
                        }
                    }

                    foreach (var c in new_info.Columns)
                    {
                        if (!old_info.ColumnNumbers.ContainsKey(c.Key))
                        {
                            new_info.ColumnNumbers[c.Key] = GetColumnsNumber(old_info.TableNumber, c.Key, c.Value);
                        }
                    }

                    var new_copy = new TableInfo()
                    {
                        ColumnNumbers = new ConcurrentDictionary<string, short>(new_info.ColumnNumbers.Select(f => new KeyValuePair<string, short>(f.Key, f.Value))),
                        Columns = new ConcurrentDictionary<string, LinqDbTypes>(new_info.Columns.Select(f => new KeyValuePair<string, LinqDbTypes>(f.Key, f.Value))),
                        Name = new_info.Name,
                        TableNumber = new_info.TableNumber
                    };
                    foreach (var c in old_info.Columns)
                    {
                        if (!new_copy.Columns.ContainsKey(c.Key))
                        {
                            new_copy.ColumnNumbers[c.Key] = old_info.ColumnNumbers[c.Key];
                            new_copy.Columns[c.Key] = old_info.Columns[c.Key];
                        }
                    }
                    using (var memoryStream = new MemoryStream())
                    {
                        Serializer.Serialize(memoryStream, new_copy);
                        struct_db.Put(Encoding.UTF8.GetBytes(name), memoryStream.ToArray());
                        struct_cache[name] = new_copy;
                    }
                }
                else
                {
                    new_info.TableNumber = old_info.TableNumber;
                    new_info.ColumnNumbers = old_info.ColumnNumbers;
                }
            }
        }

        public short GetNextTableColumnNumberNoIncrease(short table_number)
        {
            lock (_counter_lock_)
            {
                var res = struct_db.Get(":next_column:" + table_number);
                if (string.IsNullOrEmpty(res))
                {
                    return 1;
                }
                else
                {
                    var val = Convert.ToInt16(res);
                    return val;
                }
            }
        }
        short GetNextTableColumnNumber(short table_number)
        {
            lock (_counter_lock_)
            {
                var res = struct_db.Get(":next_column:" + table_number);
                if (string.IsNullOrEmpty(res))
                {
                    struct_db.Put(":next_column:" + table_number, "2");
                    return 1; //columns start with 1, because every column might have negative counterpart (for negative numbers)
                }
                else
                {
                    var val = Convert.ToInt16(res);
                    struct_db.Put(":next_column:" + table_number, (val + 1).ToString());
                    return val;
                }
            }
        }
        public short GetColumnNumber(short table_number, string column_name)
        {
            var res = struct_db.Get(":column_number:" + table_number + ":" + column_name);
            return Convert.ToInt16(res);
        }
        public LinqDbTypes GetColumnType(short table_number, short column_number)
        {
            var res = struct_db.Get(":column_type:" + table_number + ":" + column_number);
            return StringTypeToLinqType(res);
        }
        short GetColumnsNumber(short table_number, string column_name, LinqDbTypes column_type)
        {
            lock (_counter_lock_)
            {
                var res = struct_db.Get(":column_number:" + table_number + ":" + column_name);
                if (string.IsNullOrEmpty(res))
                {
                    var r = GetNextTableColumnNumber(table_number);
                    struct_db.Put(":column_number:" + table_number + ":" + column_name, r.ToString());
                    struct_db.Put(":column_type:" + table_number + ":" + r, LinqTypeToString(column_type));
                    return r;
                }
                else
                {
                    return Convert.ToInt16(res);
                }
            }
        }
        public List<Tuple<short, LinqDbTypes>> GetAllColumnsWithHistoric(short table_number)
        {
            var res = new List<Tuple<short, LinqDbTypes>>();
            lock (_counter_lock_)
            {
                var last_column = GetNextTableColumnNumberNoIncrease(table_number);
                for (short i = 1; i < last_column; i++)
                {
                    string type = struct_db.Get(":column_type:" + table_number + ":" + i);
                    res.Add(new Tuple<short, LinqDbTypes>(i, StringTypeToLinqType(type)));
                }
                return res;
            }
        }
        object _id_lock = new object();
        int GetNextId(string table_name, int given_id)
        {
            lock (_id_lock)
            {
                if (given_id == 0)
                {
                    var id = struct_db.Get(":id:" + table_name);
                    if (string.IsNullOrEmpty(id))
                    {
                        struct_db.Put(":id:" + table_name, "1");
                        return 1;
                    }
                    else
                    {
                        int id_val = Convert.ToInt32(id);
                        id_val++;
                        struct_db.Put(":id:" + table_name, id_val.ToString());
                        return id_val;
                    }
                }
                else
                {
                    var id = struct_db.Get(":id:" + table_name);
                    if (string.IsNullOrEmpty(id))
                    {
                        struct_db.Put(":id:" + table_name, given_id.ToString());
                        return given_id;
                    }
                    else
                    {
                        int id_val = Convert.ToInt32(id);
                        if (given_id > id_val)
                        {
                            struct_db.Put(":id:" + table_name, given_id.ToString());
                            return given_id;
                        }
                        return given_id;
                    }
                }
            }
        }

        int GetMaxId(string table_name)
        {
            var id = struct_db.Get(":id:" + table_name);
            if (string.IsNullOrEmpty(id))
            {
                return 0;
            }
            else
            {
                return Convert.ToInt32(id);
            }
        }
        LinqDbTypes TypeToLinqType(PropertyInfo p)
        {

            if (p.PropertyType == typeof(int) || p.PropertyType == typeof(int?))
            {
                return LinqDbTypes.int_;
            }
            else if (p.PropertyType == typeof(DateTime) || p.PropertyType == typeof(DateTime?))
            {
                return LinqDbTypes.DateTime_;
            }
            else if (p.PropertyType == typeof(double) || p.PropertyType == typeof(double?))
            {
                return LinqDbTypes.double_;
            }
            else if (p.PropertyType == typeof(byte[]))
            {
                return LinqDbTypes.binary_;
            }
            else if (p.PropertyType == typeof(string))
            {
                return LinqDbTypes.string_;
            }
            else if (p.PropertyType == typeof(bool) || p.PropertyType == typeof(bool?))
            {
                throw new Exception("Linqdb: bool type is not supported, use int instead.");
            }
            else if (p.PropertyType == typeof(decimal) || p.PropertyType == typeof(decimal?))
            {
                throw new Exception("Linqdb: decimal type is not supported, use int or double instead.");
            }
            else if (p.PropertyType == typeof(float) || p.PropertyType == typeof(float?))
            {
                throw new Exception("Linqdb: float type is not supported, use double instead.");
            }
            else if (p.PropertyType == typeof(long) || p.PropertyType == typeof(long?))
            {
                throw new Exception("Linqdb: long type is not supported, use int instead.");
            }
            else
            {
                return LinqDbTypes.unknown_;
            }
        }

        LinqDbTypes StringTypeToLinqType(string p)
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

        public string LinqTypeToString(LinqDbTypes type)
        {

            if (type == LinqDbTypes.int_)
            {
                return "int";
            }
            else if (type == LinqDbTypes.string_)
            {
                return "string";
            }
            else if (type == LinqDbTypes.double_)
            {
                return "double";
            }
            else if (type == LinqDbTypes.DateTime_)
            {
                return "DateTime";
            }
            else if (type == LinqDbTypes.binary_)
            {
                return "byte[]";
            }
            else
            {
                throw new LinqDbException("Linqdb: unknown column type.");
            }
        }

        object _counter_lock_ = new object();
        short GetNewTableCount()
        {
            lock (_counter_lock_)
            {
                var c = struct_db.Get(":counter:");
                if (string.IsNullOrEmpty(c))
                {
                    struct_db.Put(":counter:", "1");
                    return 0;
                }
                else
                {
                    short counter = Convert.ToInt16(c);
                    short res = counter;
                    counter++;
                    struct_db.Put(":counter:", counter.ToString());
                    return res;
                }
            }
        }

        object _count_lock = new object();
        Dictionary<string, object> _write_locks = new Dictionary<string, object>();
        public object GetTableWriteLock(string name)
        {
            lock (_count_lock)
            {
                if (!_write_locks.ContainsKey(name))
                {
                    _write_locks[name] = new object();
                }
                return _write_locks[name];
            }
        }
        public List<string> GetTables()
        {
            var res = new List<string>();
            using (var snapshot = struct_db.CreateSnapshot())
            {
                var ro = new ReadOptions().SetSnapshot(snapshot);
                using (var it = struct_db.NewIterator(null, ro))
                {
                    it.SeekToFirst();
                    while (it.Valid())
                    {
                        var key = it.Key();
                        if (key[0] == 58)
                        {
                            it.Next();
                            continue;
                        }
                        res.Add(Encoding.UTF8.GetString(key));
                        it.Next();
                    }
                }
            }
            return res;
        }
        public string GetTableDefinition(string name)
        {
            var info = GetTableInfo(name);
            if (info == null)
            {
                return null;
            }
            StringBuilder sb = new StringBuilder();
            sb.Append("public class ");
            sb.Append(info.Name);
            sb.Append(" { ");
            sb.Append("public int Id { get; set; } ");
            foreach (var column in info.Columns.Where(f => f.Key != "Id"))
            {
                sb.Append("public ");
                switch (column.Value)
                {
                    case LinqDbTypes.binary_:
                        sb.Append("byte[] ");
                        break;
                    case LinqDbTypes.DateTime_:
                        sb.Append("DateTime? ");
                        break;
                    case LinqDbTypes.double_:
                        sb.Append("double? ");
                        break;
                    case LinqDbTypes.int_:
                        sb.Append("int? ");
                        break;
                    case LinqDbTypes.string_:
                        sb.Append("string ");
                        break;
                    default:
                        throw new LinqDbException("Linqdb: unknown type.");
                }
                sb.Append(column.Key);
                sb.Append(" { get; set; } ");
            }
            sb.Append(" } ");
            return sb.ToString();
        }
    }


    [ProtoContract]
    public class TableInfo
    {
        [ProtoMember(1)]
        public string Name { get; set; }

        [ProtoMember(2)]
        public ConcurrentDictionary<string, LinqDbTypes> Columns { get; set; }

        [ProtoMember(3)]
        public short TableNumber { get; set; }

        [ProtoMember(4)]
        public ConcurrentDictionary<string, short> ColumnNumbers { get; set; }
    }

    [ProtoContract]
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
