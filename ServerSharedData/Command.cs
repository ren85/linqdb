using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace ServerSharedData
{
    public enum Role : int
    {
        Reader = 0,
        Writer = 1,
        Admin = 2
    }
    public class CommandHelper
    {
        //public static string BytesToString(byte[] val)
        //{
        //    if (val == null)
        //    {
        //        return null;
        //    }

        //    return Convert.ToBase64String(val);
        //}
        //public static byte[] StringToBytes(string val)
        //{
        //    if (val == null)
        //    {
        //        return null;
        //    }

        //    return Convert.FromBase64String(val);
        //}

        static Dictionary<string, string> config = new Dictionary<string, string>();
        public static ConcurrentDictionary<string, Tuple<string, Role>> users = new ConcurrentDictionary<string, Tuple<string, Role>>();
        public static string ServerName = string.Empty;
        public static void ReadConfig(out string db_path, out int port)
        {
            //read config
            string path = System.Reflection.Assembly.GetEntryAssembly().Location;
            var directory = System.IO.Path.GetDirectoryName(path);
            var config_lines = new List<string>(File.ReadAllLines(Path.Combine(directory, "config.txt")))
                               .Where(f => !string.IsNullOrEmpty(f.Trim()) && !f.Trim().StartsWith("#"))
                               .Select(f => f.Trim())
                               .ToList();

            foreach (var line in config_lines)
            {
                var index = line.IndexOf('=');
                var left = line.Substring(0, index).Trim().ToLower();
                var right = line.Substring(index + 1).Trim();
                config[left] = right;
            }
            foreach (var user in config.Where(f => f.Key.StartsWith("user.")).GroupBy(f => f.Key.Replace(".pass", "").Replace(".role", "")))
            {
                string name = user.Key.Replace("user.", "");
                string pass = user.Where(f => f.Key == "user." + name + ".pass").First().Value;
                string hpass = ServerSharedData.CommandHelper.CalculateMD5Hash(pass);
                string role = user.Where(f => f.Key == "user." + name + ".role").First().Value;
                Role role_enum = Role.Reader;
                switch (role.ToLower())
                {
                    case "reader":
                        role_enum = Role.Reader;
                        break;
                    case "writer":
                        role_enum = Role.Writer;
                        break;
                    case "admin":
                        role_enum = Role.Admin;
                        break;
                    default:
                        throw new Exception("Linqdb: unknown role");
                }
                CommandHelper.users[name] = new Tuple<string, Role>(hpass, role_enum);
            }
            if (config.ContainsKey("db_path"))
            {
                db_path = config["db_path"];
            }
            else
            {
                db_path = "DATA";
            }
            if (config.ContainsKey("port"))
            {
                port = Convert.ToInt32(config["port"]);
            }
            else
            {
                port = 2055;
            }
            if (config.ContainsKey("servername"))
            {
                ServerName = config["servername"];
            }
            else
            {
                ServerName = "unknown";
            }
        }
        public static bool CanRead(string user, string pass)
        {
            if (!users.ContainsKey(user))
            {
                throw new Exception("Linqdb: user doesn't exist");
            }
            if (users[user].Item1 != pass || (int)users[user].Item2 < (int)Role.Reader)
            {
                return false;
            }
            else
            {
                return true;
            }
        }
        public static bool CanWrite(string user, string pass)
        {
            if (!users.ContainsKey(user))
            {
                throw new Exception("Linqdb: user doesn't exist");
            }
            if (users[user].Item1 != pass || (int)users[user].Item2 < (int)Role.Writer)
            {
                return false;
            }
            else
            {
                return true;
            }
        }
        public static bool CanAdmin(string user, string pass)
        {
            if (!users.ContainsKey(user))
            {
                throw new Exception("Linqdb: user doesn't exist");
            }
            if (users[user].Item1 != pass || (int)users[user].Item2 < (int)Role.Admin)
            {
                return false;
            }
            else
            {
                return true;
            }
        }
        public static string GetServerName()
        {
            return ServerName;
        }
        public static string CalculateMD5Hash(string input)
        {
            var md5 = MD5.Create();
            byte[] inputBytes = Encoding.Unicode.GetBytes(input);
            byte[] hash = md5.ComputeHash(inputBytes);

            // step 2, convert byte array to hex string
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < hash.Length; i++)
            {
                sb.Append(hash[i].ToString("X2"));
            }
            return sb.ToString();
        }
        public static Command GetCommand(List<ClientResult> result, Dictionary<string, string> def, Dictionary<string, short> order, string type_name, string user, string pass)
        {
            var cm = new Command();
            cm.Type = (byte)CommandType.DbCommand;
            cm.TableName = type_name;
            cm.Commands = result;
            for (short i = 0; i < cm.Commands.Count; i++)
            {
                cm.Commands[i].Id = i;
            }
            cm.TableDef = def;
            cm.TableDefOrder = order;
            cm.User = user;
            cm.Pass = pass;
            return cm;
        }

        public static byte[] GetBytes(Command comm, bool no_prefix = false)
        {
            var result = new List<byte>(250);
            if (!no_prefix)
            {
                result = new List<byte>(250) { 0, 0, 0, 0 };
            }
            result.Add(comm.Type);
            var tn = Encoding.UTF8.GetBytes(comm.TableName);
            result.Add((byte)tn.Length);
            result.AddRange(tn);
            if (!string.IsNullOrEmpty(comm.User))
            {
                var userb = Encoding.UTF8.GetBytes(comm.User);
                result.Add((byte)userb.Length);
                result.AddRange(userb);
            }
            else
            {
                result.Add(0);
            }
            if (!string.IsNullOrEmpty(comm.Pass))
            {
                var hpass = CalculateMD5Hash(comm.Pass);
                var passb = Encoding.UTF8.GetBytes(hpass);
                result.Add((byte)passb.Length);
                result.AddRange(passb);
            }
            else
            {
                result.Add(0);
            }
            if (comm.TableDef != null && comm.TableDef.Any())
            {
                result.AddRange(BitConverter.GetBytes((short)comm.TableDef.Count()));
                foreach (var td in comm.TableDef)
                {
                    tn = Encoding.UTF8.GetBytes(td.Key);
                    result.Add((byte)tn.Length);
                    result.AddRange(tn);
                    tn = Encoding.UTF8.GetBytes(td.Value);
                    result.Add((byte)tn.Length);
                    result.AddRange(tn);
                }
            }
            else
            {
                result.Add(0);
                result.Add(0);
            }
            if (comm.Type == (int)CommandType.GetNewIds)
            {
                result.AddRange(BitConverter.GetBytes(comm.HowManyNewIds));
                var ids_lb = BitConverter.GetBytes(result.Count - 4);
                result[0] = ids_lb[0];
                result[1] = ids_lb[1];
                result[2] = ids_lb[2];
                result[3] = ids_lb[3];
                return result.ToArray();
            }
            if (comm.TableDefOrder != null && comm.TableDefOrder.Any())
            {
                result.AddRange(BitConverter.GetBytes((short)comm.TableDefOrder.Count()));
                foreach (var td in comm.TableDefOrder)
                {
                    tn = Encoding.UTF8.GetBytes(td.Key);
                    result.Add((byte)tn.Length);
                    result.AddRange(tn);
                    result.AddRange(BitConverter.GetBytes(td.Value));
                }
            }
            else
            {
                result.Add(0);
                result.Add(0);
            }
            if (comm.Commands != null && comm.Commands.Any())
            {
                result.AddRange(BitConverter.GetBytes((short)comm.Commands.Count()));
                foreach (var cr in comm.Commands)
                {
                    result.AddRange(BitConverter.GetBytes(cr.Id));
                    tn = Encoding.UTF8.GetBytes(cr.Type);
                    result.Add((byte)tn.Length);
                    result.AddRange(tn);
                    if (!string.IsNullOrEmpty(cr.Selector))
                    {
                        tn = Encoding.UTF8.GetBytes(cr.Selector);
                        result.Add((byte)tn.Length);
                        result.AddRange(tn);
                    }
                    else
                    {
                        result.Add((byte)0);
                    }
                    if (cr.AnonSelect != null && cr.AnonSelect.Any())
                    {
                        result.AddRange(BitConverter.GetBytes((short)cr.AnonSelect.Count()));
                        foreach (var an in cr.AnonSelect)
                        {
                            tn = Encoding.UTF8.GetBytes(an);
                            result.Add((byte)tn.Length);
                            result.AddRange(tn);
                        }
                    }
                    else
                    {
                        result.Add(0);
                        result.Add(0);
                    }
                    if (cr.Opers != null && cr.Opers.Any())
                    {
                        result.AddRange(BitConverter.GetBytes((short)cr.Opers.Count()));
                        foreach (var op in cr.Opers)
                        {
                            result.AddRange(BitConverter.GetBytes(op.Id));
                            result.Add(op.IsOperator ? (byte)1 : (byte)0);
                            result.AddRange(BitConverter.GetBytes(op.Type));
                            if (!string.IsNullOrEmpty(op.ColumnName))
                            {
                                tn = Encoding.UTF8.GetBytes(op.ColumnName);
                                result.Add((byte)tn.Length);
                                result.AddRange(tn);
                            }
                            else
                            {
                                result.Add(0);
                            }
                            if (op.NonDbValue != null && op.NonDbValue.Any())
                            {
                                result.AddRange(BitConverter.GetBytes((short)op.NonDbValue.Count()));
                                result.AddRange(op.NonDbValue);
                            }
                            else
                            {
                                result.Add(0);
                                result.Add(0);
                            }
                            result.Add(op.IsDb ? (byte)1 : (byte)0);
                            result.Add(op.IsResult ? (byte)1 : (byte)0);
                        }
                    }
                    else
                    {
                        result.Add(0);
                        result.Add(0);
                    }
                    if (cr.From == 0 && cr.To == 0)
                    {
                        result.Add(0);
                    }
                    else
                    {
                        result.Add(1);
                        result.AddRange(BitConverter.GetBytes(cr.Boundaries));
                        result.AddRange(BitConverter.GetBytes(cr.From));
                        result.AddRange(BitConverter.GetBytes(cr.To));
                    }
                    if ((cr.Int_set == null || !cr.Int_set.Any()) && !cr.Int_null &&
                       (cr.Double_set == null || !cr.Double_set.Any()) && !cr.Double_null &&
                       (cr.Date_set == null || !cr.Date_set.Any()) && !cr.Date_null &&
                       (cr.String_set == null || !cr.String_set.Any()) && !cr.String_null)
                    {
                        result.Add(0);
                    }
                    else
                    {
                        result.Add(1);
                        if (cr.Int_set != null && cr.Int_set.Any())
                        {
                            result.AddRange(BitConverter.GetBytes(cr.Int_set.Count()));
                            foreach (var item in cr.Int_set)
                            {
                                result.AddRange(BitConverter.GetBytes(item));
                            }
                        }
                        else
                        {
                            result.Add(0);
                            result.Add(0);
                            result.Add(0);
                            result.Add(0);
                        }
                        result.Add(cr.Int_null ? (byte)1 : (byte)0);

                        if (cr.Double_set != null && cr.Double_set.Any())
                        {
                            result.AddRange(BitConverter.GetBytes(cr.Double_set.Count()));
                            foreach (var item in cr.Double_set)
                            {
                                result.AddRange(BitConverter.GetBytes(item));
                            }
                        }
                        else
                        {
                            result.Add(0);
                            result.Add(0);
                            result.Add(0);
                            result.Add(0);
                        }
                        result.Add(cr.Double_null ? (byte)1 : (byte)0);

                        if (cr.Date_set != null && cr.Date_set.Any())
                        {
                            result.AddRange(BitConverter.GetBytes(cr.Date_set.Count()));
                            foreach (var item in cr.Date_set)
                            {
                                result.AddRange(BitConverter.GetBytes(item));
                            }
                        }
                        else
                        {
                            result.Add(0);
                            result.Add(0);
                            result.Add(0);
                            result.Add(0);
                        }
                        result.Add(cr.Date_null ? (byte)1 : (byte)0);

                        if (cr.String_set != null && cr.String_set.Any())
                        {
                            result.AddRange(BitConverter.GetBytes(cr.String_set.Count()));
                            foreach (var item in cr.String_set)
                            {
                                tn = Encoding.UTF8.GetBytes(item);
                                result.AddRange(BitConverter.GetBytes((short)tn.Length));
                                result.AddRange(tn);
                            }
                        }
                        else
                        {
                            result.Add(0);
                            result.Add(0);
                            result.Add(0);
                            result.Add(0);
                        }
                        result.Add(cr.String_null ? (byte)1 : (byte)0);
                    }
                    if (string.IsNullOrEmpty(cr.Query))
                    {
                        result.Add(0);
                    }
                    else
                    {
                        result.Add(1);
                        if (cr.Start_step != null)
                        {
                            result.AddRange(BitConverter.GetBytes((int)cr.Start_step));
                        }
                        else
                        {
                            result.AddRange(BitConverter.GetBytes(-1));
                        }
                        if (cr.Steps != null)
                        {
                            result.AddRange(BitConverter.GetBytes((int)cr.Steps));
                        }
                        else
                        {
                            result.AddRange(BitConverter.GetBytes(-1));
                        }
                        tn = Encoding.UTF8.GetBytes(cr.Query);
                        result.AddRange(BitConverter.GetBytes((short)tn.Length));
                        result.AddRange(tn);
                    }
                    result.AddRange(BitConverter.GetBytes(cr.Skip));
                    result.AddRange(BitConverter.GetBytes(cr.Take));
                    if (cr.Data != null && cr.Data.Any())
                    {
                        result.AddRange(BitConverter.GetBytes(cr.Data.Count()));
                        foreach (var d in cr.Data)
                        {
                            if (d.Bytes != null && d.Bytes.Any())
                            {
                                result.AddRange(BitConverter.GetBytes(d.Bytes.Count()));
                                foreach (var bl in d.Bytes)
                                {
                                    result.AddRange(BitConverter.GetBytes(bl.Count()));
                                    result.AddRange(bl);
                                }
                            }
                            else
                            {
                                result.AddRange(BitConverter.GetBytes(0));
                            }
                        }
                    }
                    else
                    {
                        result.AddRange(BitConverter.GetBytes(0));
                    }
                    if (cr.UpdateData != null && cr.UpdateData.Any())
                    {
                        result.AddRange(BitConverter.GetBytes(cr.UpdateData.Count()));
                        foreach (var ud in cr.UpdateData)
                        {
                            result.AddRange(BitConverter.GetBytes(ud.Key));
                            if (ud.Value != null)
                            {
                                result.AddRange(BitConverter.GetBytes(ud.Value.Length));
                                result.AddRange(ud.Value);
                            }
                            else
                            {
                                result.AddRange(BitConverter.GetBytes(0));
                            }
                        }
                    }
                    else
                    {
                        result.AddRange(BitConverter.GetBytes(0));
                    }
                    if (cr.DeleteIds != null && cr.DeleteIds.Any())
                    {
                        result.AddRange(BitConverter.GetBytes(cr.DeleteIds.Count()));
                        foreach (var id in cr.DeleteIds)
                        {
                            result.AddRange(BitConverter.GetBytes(id));
                        }
                    }
                    else
                    {
                        result.AddRange(BitConverter.GetBytes(0));
                    }
                    if (!string.IsNullOrEmpty(cr.Replicate))
                    {
                        tn = Encoding.UTF8.GetBytes(cr.Replicate);
                        result.AddRange(BitConverter.GetBytes((short)tn.Length));
                        result.AddRange(tn);
                    }
                    else
                    {
                        result.AddRange(BitConverter.GetBytes((short)0));
                    }
                    if (cr.Type == "increment" || cr.Type == "increment1")
                    {
                        result.Add(1);
                        result.AddRange(BitConverter.GetBytes(cr.Inc_val));
                        result.Add(cr.Inc_old_val == null ? (byte)0 : (byte)1);
                        if (cr.Inc_old_val != null)
                        {
                            result.AddRange(BitConverter.GetBytes((int)cr.Inc_old_val));
                        }
                    }
                    else
                    {
                        result.Add(0);
                    }

                }
            }
            else
            {
                result.Add(0);
                result.Add(0);
            }

            if (!no_prefix)
            {
                var lb = BitConverter.GetBytes(result.Count - 4);
                result[0] = lb[0];
                result[1] = lb[1];
                result[2] = lb[2];
                result[3] = lb[3];
            }

            return result.ToArray();
        }
        public static Command GetCommands(byte[] comms)
        {
            var result = new Command();
            int current = 0;
            result.Type = comms[current];
            current++;
            byte[] val = new byte[(int)comms[current]];
            current++;
            for (int i = 0; i < val.Length; i++, current++)
            {
                val[i] = comms[current];
            }
            result.TableName = Encoding.UTF8.GetString(val);

            if ((int)comms[current] > 0)
            {
                val = new byte[(int)comms[current]];
                current++;
                for (int i = 0; i < val.Length; i++, current++)
                {
                    val[i] = comms[current];
                }
                result.User = Encoding.UTF8.GetString(val);
            }
            else
            {
                current++;
            }
            if ((int)comms[current] > 0)
            {
                val = new byte[(int)comms[current]];
                current++;
                for (int i = 0; i < val.Length; i++, current++)
                {
                    val[i] = comms[current];
                }
                result.Pass = Encoding.UTF8.GetString(val);
            }
            else
            {
                current++;
            }

            result.TableDef = new Dictionary<string, string>();
            int ilen = 0;
            short slen = BitConverter.ToInt16(new byte[2] { comms[current], comms[current + 1] }, 0);
            current += 2;
            if (slen != 0)
            {
                for (int i = 0; i < slen; i++)
                {
                    string key = null;
                    string value = null;
                    short len = BitConverter.ToInt16(new byte[2] { comms[current], 0 }, 0);
                    current++;
                    byte[] k = new byte[len];
                    for (int j = 0; j < len; j++, current++)
                    {
                        k[j] = comms[current];
                    }
                    key = Encoding.UTF8.GetString(k);
                    len = BitConverter.ToInt16(new byte[2] { comms[current], 0 }, 0);
                    current++;
                    k = new byte[len];
                    for (int j = 0; j < len; j++, current++)
                    {
                        k[j] = comms[current];
                    }
                    value = Encoding.UTF8.GetString(k);
                    result.TableDef[key] = value;
                }
            }
            result.TableDefOrder = new Dictionary<string, short>();
            if (result.Type == (int)CommandType.GetNewIds)
            {
                result.HowManyNewIds = BitConverter.ToInt32(new byte[4] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3] }, 0);
                current += 4;
                return result;
            }
            slen = BitConverter.ToInt16(new byte[2] { comms[current], comms[current + 1] }, 0);
            current += 2;
            if (slen != 0)
            {
                for (int i = 0; i < slen; i++)
                {
                    string key = null;
                    short value = 0;
                    short len = BitConverter.ToInt16(new byte[2] { comms[current], 0 }, 0);
                    current++;
                    byte[] k = new byte[len];
                    for (int j = 0; j < len; j++, current++)
                    {
                        k[j] = comms[current];
                    }
                    key = Encoding.UTF8.GetString(k);
                    value = BitConverter.ToInt16(new byte[2] { comms[current], comms[current + 1] }, 0);
                    current += 2;
                    result.TableDefOrder[key] = value;
                }
            }
            result.Commands = new List<ClientResult>();
            var rcount = BitConverter.ToInt16(new byte[2] { comms[current], comms[current + 1] }, 0);
            current += 2;
            for (int z = 0; z < rcount; z++)
            {
                var cr = new ClientResult();
                cr.Id = BitConverter.ToInt16(new byte[2] { comms[current], comms[current + 1] }, 0);
                current += 2;
                short len = 0;
                if (comms[current] != 0)
                {
                    len = BitConverter.ToInt16(new byte[2] { comms[current], 0 }, 0);
                    current++;
                    byte[] k = new byte[len];
                    for (int j = 0; j < len; j++, current++)
                    {
                        k[j] = comms[current];
                    }
                    cr.Type = Encoding.UTF8.GetString(k);
                }
                else
                {
                    current++;
                }
                if (comms[current] != 0)
                {
                    len = BitConverter.ToInt16(new byte[2] { comms[current], 0 }, 0);
                    current++;
                    byte[] k = new byte[len];
                    for (int j = 0; j < len; j++, current++)
                    {
                        k[j] = comms[current];
                    }
                    cr.Selector = Encoding.UTF8.GetString(k);
                }
                else
                {
                    current++;
                }

                len = BitConverter.ToInt16(new byte[2] { comms[current], comms[current + 1], }, 0);
                current += 2;
                cr.AnonSelect = new List<string>();
                if (len != 0)
                {
                    for (int j = 0; j < len; j++)
                    {
                        slen = BitConverter.ToInt16(new byte[2] { comms[current], 0 }, 0);
                        current++;
                        byte[] k = new byte[slen];
                        for (int h = 0; h < slen; h++, current++)
                        {
                            k[h] = comms[current];
                        }
                        cr.AnonSelect.Add(Encoding.UTF8.GetString(k));
                    }
                }
                cr.Opers = new List<SharedOper>();
                len = BitConverter.ToInt16(new byte[2] { comms[current], comms[current + 1] }, 0);
                current += 2;
                if (len != 0)
                {
                    for (int j = 0; j < len; j++)
                    {
                        var op = new SharedOper();
                        op.Id = BitConverter.ToInt16(new byte[2] { comms[current], comms[current + 1] }, 0);
                        current += 2;
                        op.IsOperator = comms[current] == 1 ? true : false;
                        current++;
                        op.Type = BitConverter.ToInt16(new byte[2] { comms[current], comms[current + 1] }, 0);
                        current += 2;
                        slen = BitConverter.ToInt16(new byte[2] { comms[current], 0 }, 0);
                        current++;
                        if (slen != 0)
                        {
                            byte[] k = new byte[slen];
                            for (int h = 0; h < slen; h++, current++)
                            {
                                k[h] = comms[current];
                            }
                            op.ColumnName = Encoding.UTF8.GetString(k);
                        }
                        slen = BitConverter.ToInt16(new byte[2] { comms[current], comms[current + 1] }, 0);
                        current += 2;
                        if (slen != 0)
                        {
                            byte[] k = new byte[slen];
                            for (int h = 0; h < slen; h++, current++)
                            {
                                k[h] = comms[current];
                            }
                            op.NonDbValue = k;
                        }
                        op.IsDb = comms[current] == 1 ? true : false;
                        current++;
                        op.IsResult = comms[current] == 1 ? true : false;
                        current++;
                        cr.Opers.Add(op);
                    }
                }
                len = BitConverter.ToInt16(new byte[2] { comms[current], 0 }, 0);
                current++;
                if (len != 0)
                {
                    cr.Boundaries = BitConverter.ToInt16(new byte[2] { comms[current], comms[current + 1] }, 0);
                    current += 2;
                    cr.From = BitConverter.ToDouble(new byte[8] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3], comms[current + 4], comms[current + 5], comms[current + 6], comms[current + 7] }, 0);
                    current += 8;
                    cr.To = BitConverter.ToDouble(new byte[8] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3], comms[current + 4], comms[current + 5], comms[current + 6], comms[current + 7] }, 0);
                    current += 8;
                }
                len = BitConverter.ToInt16(new byte[2] { comms[current], 0 }, 0);
                current++;
                if (len != 0)
                {
                    ilen = BitConverter.ToInt32(new byte[4] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3] }, 0);
                    current += 4;
                    if (ilen != 0)
                    {
                        cr.Int_set = new HashSet<int>();
                        for (int h = 0; h < ilen; h++, current += 4)
                        {
                            cr.Int_set.Add(BitConverter.ToInt32(new byte[4] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3] }, 0));
                        }
                    }
                    cr.Int_null = comms[current] == 1;
                    current++;

                    ilen = BitConverter.ToInt32(new byte[4] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3] }, 0);
                    current += 4;
                    if (ilen != 0)
                    {
                        cr.Double_set = new HashSet<double>();
                        for (int h = 0; h < ilen; h++, current += 8)
                        {
                            cr.Double_set.Add(BitConverter.ToDouble(new byte[8] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3], comms[current + 4], comms[current + 5], comms[current + 6], comms[current + 7] }, 0));
                        }
                    }

                    cr.Double_null = comms[current] == 1;
                    current++;


                    ilen = BitConverter.ToInt32(new byte[4] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3] }, 0);
                    current += 4;
                    if (ilen != 0)
                    {
                        cr.Date_set = new HashSet<double>();
                        for (int h = 0; h < ilen; h++, current += 8)
                        {
                            cr.Date_set.Add(BitConverter.ToDouble(new byte[8] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3], comms[current + 4], comms[current + 5], comms[current + 6], comms[current + 7] }, 0));
                        }
                    }

                    cr.Date_null = comms[current] == 1;
                    current++;

                    ilen = BitConverter.ToInt32(new byte[4] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3] }, 0);
                    current += 4;
                    if (ilen != 0)
                    {
                        cr.String_set = new HashSet<string>();
                        for (int h = 0; h < ilen; h++)
                        {
                            slen = BitConverter.ToInt16(new byte[2] { comms[current], comms[current + 1] }, 0);
                            current += 2;
                            if (slen != 0)
                            {
                                byte[] k = new byte[slen];
                                for (int x = 0; x < slen; x++, current++)
                                {
                                    k[x] = comms[current];
                                }
                                cr.String_set.Add(Encoding.UTF8.GetString(k));
                            }
                            else
                            {
                                cr.String_set.Add("");
                            }
                        }
                    }

                    cr.String_null = comms[current] == 1;
                    current++;
                }
                len = BitConverter.ToInt16(new byte[2] { comms[current], 0 }, 0);
                current++;
                if (len != 0)
                {
                    ilen = BitConverter.ToInt32(new byte[4] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3] }, 0);
                    current += 4;
                    if (ilen != -1)
                    {
                        cr.Start_step = ilen;
                    }
                    ilen = BitConverter.ToInt32(new byte[4] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3] }, 0);
                    current += 4;
                    if (ilen != -1)
                    {
                        cr.Steps = ilen;
                    }
                    slen = BitConverter.ToInt16(new byte[2] { comms[current], 0 }, 0);
                    current += 2;
                    byte[] k = new byte[slen];
                    for (int x = 0; x < slen; x++, current++)
                    {
                        k[x] = comms[current];
                    }
                    cr.Query = Encoding.UTF8.GetString(k);
                }
                cr.Skip = BitConverter.ToInt32(new byte[4] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3] }, 0);
                current += 4;
                cr.Take = BitConverter.ToInt32(new byte[4] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3] }, 0);
                current += 4;

                ilen = BitConverter.ToInt32(new byte[4] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3] }, 0);
                current += 4;
                if (ilen != 0)
                {
                    cr.Data = new List<BinData>();
                    for (int x = 0; x < ilen; x++)
                    {
                        var bd = new BinData();
                        int dlen = BitConverter.ToInt32(new byte[4] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3] }, 0);
                        current += 4;
                        if (dlen != 0)
                        {
                            bd.Bytes = new List<byte[]>(dlen);
                            for (int h = 0; h < dlen; h++)
                            {
                                int blen = BitConverter.ToInt32(new byte[4] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3] }, 0);
                                current += 4;
                                var bres = new byte[blen];
                                for (int f = 0; f < blen; f++, current++)
                                {
                                    bres[f] = comms[current];
                                }
                                bd.Bytes.Add(bres);
                            }
                        }
                        cr.Data.Add(bd);
                    }
                }
                ilen = BitConverter.ToInt32(new byte[4] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3] }, 0);
                current += 4;
                if (ilen != 0)
                {
                    cr.UpdateData = new Dictionary<int, byte[]>();
                    for (int x = 0; x < ilen; x++)
                    {
                        int key = BitConverter.ToInt32(new byte[4] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3] }, 0);
                        current += 4;
                        int blen = BitConverter.ToInt32(new byte[4] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3] }, 0);
                        current += 4;
                        if (blen == 0)
                        {
                            cr.UpdateData[key] = null;
                        }
                        else
                        {
                            byte[] udata = new byte[blen];
                            for (int f = 0; f < blen; f++, current++)
                            {
                                udata[f] = comms[current];
                            }
                            cr.UpdateData[key] = udata;
                        }
                    }
                }
                ilen = BitConverter.ToInt32(new byte[4] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3] }, 0);
                current += 4;
                if (ilen != 0)
                {
                    cr.DeleteIds = new HashSet<int>();
                    for (int x = 0; x < ilen; x++, current += 4)
                    {
                        int did = BitConverter.ToInt32(new byte[4] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3] }, 0);
                        cr.DeleteIds.Add(did);
                    }
                }
                slen = BitConverter.ToInt16(new byte[2] { comms[current], comms[current + 1] }, 0);
                current += 2;
                if (slen != 0)
                {
                    byte[] k = new byte[slen];
                    for (int x = 0; x < slen; x++, current++)
                    {
                        k[x] = comms[current];
                    }
                    cr.Replicate = Encoding.UTF8.GetString(k);
                }
                byte inc = comms[current];
                current++;
                if (inc != 0)
                {
                    cr.Inc_val = BitConverter.ToInt32(new byte[4] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3] }, 0);
                    current += 4;
                    byte ov = comms[current];
                    current++;
                    if (ov != 0)
                    {
                        cr.Inc_old_val = BitConverter.ToInt32(new byte[4] { comms[current], comms[current + 1], comms[current + 2], comms[current + 3] }, 0);
                        current += 4;
                    }
                }
                result.Commands.Add(cr);
            }
            return result;
        }
    }
    public class Command
    {
        public byte Type { get; set; }
        public string TableName { get; set; }
        public Dictionary<string, string> TableDef { get; set; }
        public Dictionary<string, short> TableDefOrder { get; set; }
        public List<ClientResult> Commands { get; set; }
        public int HowManyNewIds { get; set; }
        public string User { get; set; }
        public string Pass { get; set; }
    }
    public class ClientResult
    {
        public short Id { get; set; }
        public string Type { get; set; }
        public string Selector { get; set; }
        public List<string> AnonSelect { get; set; }
        public List<SharedOper> Opers { get; set; }
        public short Boundaries { get; set; }
        public double From { get; set; }
        public double To { get; set; }
        public HashSet<int> Int_set { get; set; }
        public bool Int_null { get; set; }
        public HashSet<double> Double_set { get; set; }
        public bool Double_null { get; set; }
        public HashSet<double> Date_set { get; set; }
        public bool Date_null { get; set; }
        public HashSet<string> String_set { get; set; }
        public bool String_null { get; set; }
        public int? Start_step { get; set; }
        public int? Steps { get; set; }
        public string Query { get; set; }
        public int Skip { get; set; }
        public int Take { get; set; }
        public List<BinData> Data { get; set; }
        public Dictionary<int, byte[]> UpdateData { get; set; }
        public HashSet<int> DeleteIds { get; set; }
        public string Replicate { get; set; }
        public int Inc_val { get; set; }
        public int? Inc_old_val { get; set; }
    }
    public class SharedOper
    {
        public short Id { get; set; }
        public bool IsOperator { get; set; }
        public short Type { get; set; }
        public string ColumnName { get; set; }
        public byte[] NonDbValue { get; set; }
        public bool IsDb { get; set; }
        public bool IsResult { get; set; }
    }
    public class BinData
    {
        public List<byte[]> Bytes { get; set; }
    }

    public enum CommandType : byte
    {
        DbCommand = 0,
        GetAllTables = 1,
        GetGivenTable = 2,
        GetNewIds = 3,
        Transaction = 4,
        GetAllIndexes = 5,
        GetServerName = 6
    }
}
