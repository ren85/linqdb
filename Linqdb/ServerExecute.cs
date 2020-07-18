using RocksDbSharp;
using ServerSharedData;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        int _MaxSelectByteCount = 0;
        public int MaxSelectByteCount
        {
            get
            {
                if (_MaxSelectByteCount == 0)
                {
                    //if (IntPtr.Size == 4) //32-bit
                    //{
                    //    _MaxSelectByteCount = 536870912; //512 Mb
                    //}
                    //else if (IntPtr.Size == 8) //64-bit
                    //{
                    //    _MaxSelectByteCount = 1073741824; //1Gb
                    //}
                    if (!Environment.Is64BitProcess) //32-bit
                    {
                        _MaxSelectByteCount = 157286400; //150 Mb
                    }
                    else //64-bit
                    {
                        _MaxSelectByteCount = 314572800; //300 Mb
                    }

                }
                return _MaxSelectByteCount;
            }
        }
        public ServerResult Execute(Command comm, WriteBatchWithConstraints trans_batch, Dictionary<string, int> trans_count_cache, Dictionary<string, KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>> scache)
        {
            var server_result = new ServerResult();

            if (comm.Type == (int)CommandType.GetNewIds)
            {
                if (!CommandHelper.CanWrite(comm.User, comm.Pass))
                {
                    throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                }
                server_result.Ids = new List<int>();
                var table_info = CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
                for (int i = 0; i < comm.HowManyNewIds; i++)
                {
                    int newid = GetNextId(comm.TableName, 0);
                    server_result.Ids.Add(newid);
                }
                return server_result;
            }

            if (comm.Type == (int)CommandType.GetAllTables)
            {
                if (!CommandHelper.CanRead(comm.User, comm.Pass))
                {
                    throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                }
                server_result.TableInfo = GetTables().Aggregate((a, b) => a + "|" + b);
                return server_result;
            }
            if (comm.Type == (int)CommandType.GetAllIndexes)
            {
                if (!CommandHelper.CanRead(comm.User, comm.Pass))
                {
                    throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                }
                server_result.TableInfo = GetExistingIndexes();
                return server_result;
            }
            if (comm.Type == (int)CommandType.GetGivenTable)
            {
                if (!CommandHelper.CanRead(comm.User, comm.Pass))
                {
                    throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                }
                server_result.TableInfo = GetTableDefinition(comm.TableName);
                return server_result;
            }

            var iq = new IDbQueryable<object>() { _db = this };
            var oiq = new IDbOrderedQueryable<object>() { _db = this };

            foreach (var inst in comm.Commands)
            {
                if (inst.Type == "where")
                {
                    var table_info = CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));

                    Stack<Oper> stack = new Stack<Oper>();
                    foreach (var op in inst.Opers.OrderBy(f => f.Id))
                    {
                        var nop = new Oper();
                        object ndb = null;
                        if (op.NonDbValue != null)
                        {
                            if (table_info.Columns[op.ColumnName] == LinqDbTypes.int_)
                            {
                                ndb = BitConverter.ToInt32(op.NonDbValue, 0);
                            }
                            else if (table_info.Columns[op.ColumnName] == LinqDbTypes.double_)
                            {
                                ndb = BitConverter.ToDouble(op.NonDbValue, 0);
                            }
                            else if (table_info.Columns[op.ColumnName] == LinqDbTypes.DateTime_)
                            {
                                var ms = BitConverter.ToDouble(op.NonDbValue, 0);
                                ndb = new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(ms);
                            }
                            else if (table_info.Columns[op.ColumnName] == LinqDbTypes.string_)
                            {
                                ndb = Encoding.UTF8.GetString(op.NonDbValue);
                            }
                            else if (table_info.Columns[op.ColumnName] == LinqDbTypes.binary_)
                            {
                                ndb = op.NonDbValue;
                            }
                        }
                        if (op.ColumnName != null)
                        {
                            if (table_info.Columns[op.ColumnName] == LinqDbTypes.int_)
                            {
                                nop.ColumnType = LinqDbTypes.int_;
                            }
                            else if (table_info.Columns[op.ColumnName] == LinqDbTypes.double_)
                            {
                                nop.ColumnType = LinqDbTypes.double_;
                            }
                            else if (table_info.Columns[op.ColumnName] == LinqDbTypes.DateTime_)
                            {
                                nop.ColumnType = LinqDbTypes.DateTime_;
                            }
                            else if (table_info.Columns[op.ColumnName] == LinqDbTypes.string_)
                            {
                                nop.ColumnType = LinqDbTypes.string_;
                            }
                            else if (table_info.Columns[op.ColumnName] == LinqDbTypes.binary_)
                            {
                                nop.ColumnType = LinqDbTypes.binary_;
                            }
                        }
                        nop.IsOperator = op.IsOperator;
                        nop.Type = (ExpressionType)op.Type;
                        nop.ColumnName = op.ColumnName;
                        nop.TableName = table_info.Name;
                        nop.NonDbValue = ndb;
                        nop.IsDb = op.IsDb;
                        nop.IsResult = op.IsResult;
                        nop.ColumnNumber = op.IsOperator ? (short)0 : table_info.ColumnNumbers[op.ColumnName];
                        nop.TableNumber = table_info.TableNumber;
                        stack.Push(nop);
                    }

                    if (iq.LDBTree.WhereInfo == null)
                    {
                        iq.LDBTree.WhereInfo = new List<WhereInfo>();
                    }
                    var info = new WhereInfo() { Opers = stack };
                    iq.LDBTree.WhereInfo.Add(info);

                    iq.LDBTree.Prev = info;
                    iq.LDBTree.Prev.Id = iq.LDBTree.Counter + 1;
                    iq.LDBTree.Counter++;
                }
                else if (inst.Type == "between")
                {
                    CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));

                    var tree = iq.LDBTree;
                    if (tree.BetweenInfo == null)
                    {
                        tree.BetweenInfo = new List<BetweenInfo>();
                    }
                    var info = new BetweenInfo();
                    tree.BetweenInfo.Add(info);
                    info.BetweenBoundaries = (BetweenBoundariesInternal)inst.Boundaries;
                    info.From = inst.From;
                    info.To = inst.To;

                    var name = inst.Selector;
                    var table_info = GetTableInfo(comm.TableName);
                    info.TableNumber = table_info.TableNumber;
                    info.ColumnNumber = table_info.ColumnNumbers[name];
                    info.ColumnType = table_info.Columns[name];
                    info.IdNumber = table_info.ColumnNumbers["Id"];
                    info.TableName = comm.TableName;
                    info.ColumnName = name;

                    iq.LDBTree.Prev = info;
                    iq.LDBTree.Prev.Id = iq.LDBTree.Counter + 1;
                    iq.LDBTree.Counter++;
                }
                else if (inst.Type == "intersect")
                {
                    CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));

                    var new_set = new List<EncodedValue>();
                    if (inst.String_set != null || inst.String_null)
                    {
                        if (inst.String_set != null)
                        {
                            foreach (var s in inst.String_set)
                            {
                                new_set.Add(EncodeValue(LinqDbTypes.string_, s));
                            }
                        }
                        if (inst.String_null)
                        {
                            new_set.Add(EncodeValue(LinqDbTypes.string_, null));
                        }
                    }
                    else if (inst.Int_set != null || inst.Int_null)
                    {
                        if (inst.Int_set != null)
                        {
                            foreach (var s in inst.Int_set)
                            {
                                new_set.Add(EncodeValue(LinqDbTypes.int_, s));
                            }
                        }
                        if (inst.Int_null)
                        {
                            new_set.Add(EncodeValue(LinqDbTypes.int_, null));
                        }
                    }
                    else if (inst.Double_set != null || inst.Double_null)
                    {
                        if (inst.Double_set != null)
                        {
                            foreach (var s in inst.Double_set)
                            {
                                new_set.Add(EncodeValue(LinqDbTypes.double_, s));
                            }
                        }
                        if (inst.Double_null)
                        {
                            new_set.Add(EncodeValue(LinqDbTypes.double_, null));
                        }
                    }
                    else if (inst.Date_set != null || inst.Date_null)
                    {
                        if (inst.Date_set != null)
                        {
                            foreach (var s in inst.Date_set)
                            {
                                var val = new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(s);
                                new_set.Add(EncodeValue(LinqDbTypes.DateTime_, val));
                            }
                        }
                        if (inst.Date_null)
                        {
                            new_set.Add(EncodeValue(LinqDbTypes.DateTime_, null));
                        }
                    }
                    var tree = iq.LDBTree;
                    if (tree.IntersectInfo == null)
                    {
                        tree.IntersectInfo = new List<IntersectInfo>();
                    }
                    var info = new IntersectInfo();
                    tree.IntersectInfo.Add(info);
                    info.Set = new_set;

                    var name = inst.Selector;
                    var table_info = GetTableInfo(comm.TableName);
                    info.TableNumber = table_info.TableNumber;
                    info.ColumnNumber = table_info.ColumnNumbers[name];
                    info.ColumnType = table_info.Columns[name];

                    iq.LDBTree.Prev = info;
                    iq.LDBTree.Prev.Id = iq.LDBTree.Counter + 1;
                    iq.LDBTree.Counter++;
                }
                else if (inst.Type == "search")
                {
                    CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));

                    if (!string.IsNullOrEmpty(inst.Query))
                    {
                        var tree = iq.LDBTree;
                        if (tree.SearchInfo == null)
                        {
                            tree.SearchInfo = new List<SearchInfo>();
                        }
                        var info = new SearchInfo();
                        tree.SearchInfo.Add(info);
                        info.SearchQuery = inst.Query;
                        info.Start_step = inst.Start_step;
                        info.Steps = inst.Steps;
                        info.Partial = inst.Double_null;

                        var name = inst.Selector;
                        var table_info = GetTableInfo(comm.TableName);
                        info.TableInfo = table_info;
                        info.Name = name;

                        iq.LDBTree.Prev = info;
                        iq.LDBTree.Prev.Id = iq.LDBTree.Counter + 1;
                        iq.LDBTree.Counter++;
                    }
                }
                else if (inst.Type == "or")
                {
                    iq.LDBTree.Prev.OrWith = iq.LDBTree.Prev.Id + 1;
                }
                else if (inst.Type == "order")
                {
                    CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
                    oiq.LDBTree = iq.LDBTree;
                    var table_info = GetTableInfo(comm.TableName);
                    var name = inst.Selector;
                    oiq.LDBTree.OrderingInfo = new OrderingInfo();
                    oiq.LDBTree.OrderingInfo.Orderings = new List<OrderByInfo>();
                    oiq.LDBTree.OrderingInfo.Orderings.Add(new OrderByInfo()
                    {
                        TableNumber = table_info.TableNumber,
                        ColumnNumber = table_info.ColumnNumbers[name],
                        ColumnType = table_info.Columns[name],
                        IsDescending = false,
                        ColumnName = name
                    });
                }
                else if (inst.Type == "orderdesc")
                {
                    CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
                    oiq.LDBTree = iq.LDBTree;
                    var table_info = GetTableInfo(comm.TableName);
                    var name = inst.Selector;
                    oiq.LDBTree.OrderingInfo = new OrderingInfo();
                    oiq.LDBTree.OrderingInfo.Orderings = new List<OrderByInfo>();
                    oiq.LDBTree.OrderingInfo.Orderings.Add(new OrderByInfo()
                    {
                        TableNumber = table_info.TableNumber,
                        ColumnNumber = table_info.ColumnNumbers[name],
                        ColumnType = table_info.Columns[name],
                        IsDescending = true,
                        ColumnName = name
                    });
                }
                else if (inst.Type == "skip")
                {
                    oiq.LDBTree.OrderingInfo.Skip = inst.Skip;
                }
                else if (inst.Type == "take")
                {
                    oiq.LDBTree.OrderingInfo.Take = inst.Take;
                }
                else if (inst.Type == "selectgrouped")
                {
                    if (!CommandHelper.CanRead(comm.User, comm.Pass))
                    {
                        throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                    }
                    SelectGrouped(comm, iq, server_result, inst.Type, inst.AnonSelect);
                }
                else if (inst.Type.StartsWith("select"))
                {
                    if (!CommandHelper.CanRead(comm.User, comm.Pass))
                    {
                        throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                    }
                    SelectServer(comm, iq, server_result, inst.Type, inst.AnonSelect);
                }
                else if (inst.Type == "last")
                {
                    if (!CommandHelper.CanRead(comm.User, comm.Pass))
                    {
                        throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                    }
                    var table_info = CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
                    server_result.LastStep = GetLastStep(table_info);
                }
                else if (inst.Type == "count")
                {
                    if (!CommandHelper.CanRead(comm.User, comm.Pass))
                    {
                        throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                    }
                    var tree = iq.LDBTree;
                    if ((tree.BetweenInfo == null || !tree.BetweenInfo.Any()) &&
                        (tree.IntersectInfo == null || !tree.IntersectInfo.Any()) &&
                        (tree.SearchInfo == null || !tree.SearchInfo.Any()) &&
                        (tree.WhereInfo == null || !tree.WhereInfo.Any()))
                    {
                        CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
                        var table_info = GetTableInfo(comm.TableName);
                        server_result.Count = GetTableRowCount(table_info, null);
                    }
                    else
                    {
                        CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
                        using (var snapshot = leveld_db.CreateSnapshot())
                        {
                            var ro = new ReadOptions().SetSnapshot(snapshot);
                            var table_info = GetTableInfo(comm.TableName);
                            var where_res = CalculateWhereResult<object>(tree, table_info, ro);
                            where_res = FindBetween(tree, where_res, ro);
                            where_res = Intersect(tree, where_res, table_info, tree.QueryCache, ro);
                            where_res = Search(tree, where_res, ro);
                            var fres = CombineData(where_res);
                            server_result.Count = fres.All ? GetTableRowCount(table_info, ro) : fres.ResIds.Count();
                        }
                    }
                    return server_result;
                }
                else if (inst.Type == "getids")
                {
                    if (!CommandHelper.CanRead(comm.User, comm.Pass))
                    {
                        throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                    }
                    var tree = iq.LDBTree;
                    if ((tree.BetweenInfo == null || !tree.BetweenInfo.Any()) &&
                        (tree.IntersectInfo == null || !tree.IntersectInfo.Any()) &&
                        (tree.SearchInfo == null || !tree.SearchInfo.Any()) &&
                        (tree.WhereInfo == null || !tree.WhereInfo.Any()))
                    {
                        //.Where(f => f.Id > 0)
                        CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
                        var table_info = GetTableInfo(comm.TableName);
                        var op1 = new Oper()
                        {
                            ColumnName = null,
                            ColumnNumber = 0,
                            ColumnType = LinqDbTypes.int_,
                            IsDb = false,
                            IsOperator = true,
                            IsResult = false,
                            NonDbValue = null,
                            Result = null,
                            TableName = table_info.Name,
                            TableNumber = table_info.TableNumber,
                            Type = System.Linq.Expressions.ExpressionType.GreaterThan
                        };
                        var op2 = new Oper()
                        {
                            ColumnName = "Id",
                            ColumnNumber = table_info.ColumnNumbers["Id"],
                            ColumnType = LinqDbTypes.int_,
                            IsDb = true,
                            IsOperator = false,
                            IsResult = false,
                            NonDbValue = null,
                            Result = null,
                            TableName = table_info.Name,
                            TableNumber = table_info.TableNumber,
                            Type = System.Linq.Expressions.ExpressionType.MemberAccess
                        };
                        var op3 = new Oper()
                        {
                            ColumnName = null,
                            ColumnNumber = 0,
                            ColumnType = LinqDbTypes.int_,
                            IsDb = false,
                            IsOperator = false,
                            IsResult = false,
                            NonDbValue = 0,
                            Result = null
                        };
                        tree.WhereInfo = new List<WhereInfo>();
                        tree.WhereInfo.Add(new WhereInfo()
                        {
                            Id = 1,
                            Opers = new Stack<Oper>()
                        });
                        tree.WhereInfo[0].Opers.Push(op1);
                        tree.WhereInfo[0].Opers.Push(op2);
                        tree.WhereInfo[0].Opers.Push(op3);
                    }

                    CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
                    using (var snapshot = leveld_db.CreateSnapshot())
                    {
                        var ro = new ReadOptions().SetSnapshot(snapshot);
                        var table_info = GetTableInfo(comm.TableName);
                        var where_res = CalculateWhereResult<object>(tree, table_info, ro);
                        where_res = FindBetween(tree, where_res, ro);
                        where_res = Intersect(tree, where_res, table_info, tree.QueryCache, ro);
                        where_res = Search(tree, where_res, ro);
                        var fres = CombineData(where_res);

                        var total = GetTableRowCount(table_info, ro);
                        var all = total == fres.ResIds.Count();
                        server_result.AllIds = all;
                        server_result.Ids = fres.ResIds.ToList();
                    }

                    return server_result;
                }
                else if (inst.Type == "save")
                {
                    if (!CommandHelper.CanWrite(comm.User, comm.Pass))
                    {
                        throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                    }
                    SaveServer(inst.Data, comm, server_result, trans_batch, trans_count_cache, scache);
                }
                else if (inst.Type == "update")
                {
                    if (!CommandHelper.CanWrite(comm.User, comm.Pass))
                    {
                        throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                    }
                    UpdateServer(inst.UpdateData, comm, inst.Selector, trans_batch, scache);
                }
                else if (inst.Type == "delete")
                {
                    if (!CommandHelper.CanWrite(comm.User, comm.Pass))
                    {
                        throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                    }
                    DeleteServer(inst.DeleteIds, comm, trans_batch, trans_count_cache, scache);
                }
                else if (inst.Type == "replicate")
                {
                    if (!CommandHelper.CanAdmin(comm.User, comm.Pass))
                    {
                        throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                    }
                    CopyTo(inst.Replicate);
                }
                else if (inst.Type == "increment")
                {
                    if (!CommandHelper.CanWrite(comm.User, comm.Pass))
                    {
                        throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                    }
                    IncrementServer(comm, server_result, iq, inst, trans_batch, trans_count_cache, scache);
                }
                //else if (inst.Type == "increment1")
                //{
                //    if (!CommandHelper.CanWrite(comm.User, comm.Pass))
                //    {
                //        throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                //    }
                //    IncrementServer1(comm, server_result, iq, inst, trans_batch, trans_count_cache, scache);
                //}
                else if (inst.Type == "increment2")
                {
                    if (!CommandHelper.CanWrite(comm.User, comm.Pass))
                    {
                        throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                    }
                    IncrementServer2(comm, server_result, iq, inst, trans_batch, trans_count_cache, scache);
                }
                else if (inst.Type == "propertyindex")
                {
                    if (!CommandHelper.CanRead(comm.User, comm.Pass))
                    {
                        throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                    }
                    CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
                    CreatePropIndex(comm.TableName, inst.Selector);
                    AddIndexToFile(comm.TableName, null, inst.Selector);
                }
                else if (inst.Type == "removepropertyindex")
                {
                    if (!CommandHelper.CanRead(comm.User, comm.Pass))
                    {
                        throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                    }
                    CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
                    RemoveIndexFromFile(comm.TableName, null, inst.Selector);
                    RemoveIndex(comm.TableName, null, inst.Selector);
                }
                else if (inst.Type == "groupindex")
                {
                    if (!CommandHelper.CanRead(comm.User, comm.Pass))
                    {
                        throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                    }
                    CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
                    CreateGroupByIndexString(comm.TableName, inst.Selector, inst.Query);
                    AddIndexToFile(comm.TableName, inst.Selector, inst.Query);
                }
                else if (inst.Type == "removegroupindex")
                {
                    if (!CommandHelper.CanRead(comm.User, comm.Pass))
                    {
                        throw new LinqDbException("Linqdb: insufficient permissions for the user.");
                    }
                    CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
                    RemoveIndexFromFile(comm.TableName, inst.Selector, inst.Query);
                    RemoveIndex(comm.TableName, inst.Selector, inst.Query);
                }
            }

            return server_result;
        }
        List<int> SaveItems(List<BinData> items, Dictionary<string, short> TableDefOrder, WriteBatchWithConstraints batch, TableInfo table_info, string table_name, Dictionary<string, int> trans_count_cache, Dictionary<string, KeyValuePair<byte[], HashSet<int>>> string_cache, Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> memory_index_meta, bool is_trans, List<int> new_ids = null)
        {
            var result_ids = new List<int>();
            //var string_cache = new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>();
            var ids = new HashSet<int>();
            foreach (var item in items)
            {
                bool is_new = false;
                if (!TableDefOrder.ContainsKey("Id"))
                {
                    throw new LinqDbException("Linqdb: type must have integer Id property");
                }
                var id = BitConverter.ToInt32(item.Bytes[TableDefOrder["Id"]], 0);
                if (id == 0)
                {
                    id = GetNextId(table_name, 0);
                    is_new = true;
                    item.Bytes[TableDefOrder["Id"]] = BitConverter.GetBytes(id);
                    if (new_ids != null)
                    {
                        new_ids.Add(id);
                    }
                }
                else
                {
                    GetNextId(table_name, id);
                }
                result_ids.Add(id);
                if (ids.Contains(id))
                {
                    continue;
                }
                else
                {
                    ids.Add(id);
                }
                if (!is_new) //maybe new if id is non 0 (but not existing)
                {
                    var key_info = new IndexKeyInfo()
                    {
                        ColumnNumber = table_info.ColumnNumbers["Id"],
                        TableNumber = table_info.TableNumber,
                        ColumnType = table_info.Columns["Id"],
                        Id = id
                    };
                    var value_key = MakeValueKey(key_info);
                    var v = leveld_db.Get(value_key);
                    if (v == null)
                    {
                        is_new = true;
                        if (id > Int32.MaxValue / 2)
                        {
                            if (!is_trans)
                            {
                                throw new LinqDbException("Linqdb: max Id value of new item is " + (Int32.MaxValue / 2));
                            }
                            else if (GetMaxId(table_name) < id)
                            {
                                throw new LinqDbException("Linqdb: max Id value of new item is " + (Int32.MaxValue / 2));
                            }
                        }
                    }
                }
                foreach (var p in table_info.Columns)
                {
                    object value = null;
                    if (p.Value == LinqDbTypes.int_)
                    {
                        if (item.Bytes[TableDefOrder[p.Key]].Any())
                        {
                            value = BitConverter.ToInt32(item.Bytes[TableDefOrder[p.Key]], 0);
                        }
                    }
                    else if (p.Value == LinqDbTypes.double_)
                    {
                        if (item.Bytes[TableDefOrder[p.Key]].Any())
                        {
                            value = BitConverter.ToDouble(item.Bytes[TableDefOrder[p.Key]], 0);
                        }
                    }
                    else if (p.Value == LinqDbTypes.DateTime_)
                    {
                        if (item.Bytes[TableDefOrder[p.Key]].Any())
                        {
                            var ms = BitConverter.ToDouble(item.Bytes[TableDefOrder[p.Key]], 0);
                            value = new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(ms);
                        }
                    }
                    else if (p.Value == LinqDbTypes.string_)
                    {
                        if (item.Bytes[TableDefOrder[p.Key]].Any())
                        {
                            value = Encoding.UTF8.GetString(item.Bytes[TableDefOrder[p.Key]]);
                        }
                    }
                    else if (p.Value == LinqDbTypes.binary_)
                    {
                        if (item.Bytes[TableDefOrder[p.Key]].Any())
                        {
                            value = item.Bytes[TableDefOrder[p.Key]];
                        }
                    }


                    if (p.Value == LinqDbTypes.string_)
                    {
                        SaveStringData(batch, (string)value, p.Key, table_info, id, string_cache, is_new);
                    }
                    else if (p.Value == LinqDbTypes.binary_)
                    {
                        SaveBinaryColumn(batch, value, p.Key, table_info, id, is_new);
                    }
                    else
                    {
                        IndexDeletedData index_deleted = null;
                        IndexNewData index_new = null;
                        IndexChangedData index_changed = null;
                        if (memory_index_meta.ContainsKey(p.Key))
                        {
                            index_deleted = memory_index_meta[p.Key].Item2;
                            index_new = memory_index_meta[p.Key].Item1;
                            index_changed = memory_index_meta[p.Key].Item3;
                        }
                        SaveDataColumn(batch, value, p.Key, p.Value, table_info, id, is_new, index_new, index_changed);
                    }
                }
            }
            IncrementCount(table_info, ids, batch, trans_count_cache);
            //WriteStringCacheToBatch(batch, string_cache, table_info);

            return result_ids;
        }

        void UpdateItems(UpdateInfo info, Dictionary<int, byte[]> values, TableInfo table_info, WriteBatchWithConstraints batch, Dictionary<string, KeyValuePair<byte[], HashSet<int>>> string_cache, Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> memory_index_meta)
        {
            foreach (var item in values)
            {
                var key = MakeIndexKey(new IndexKeyInfo()
                {
                    TableNumber = info.TableNumber,
                    ColumnNumber = table_info.ColumnNumbers["Id"],
                    Val = BitConverter.GetBytes(item.Key).MyReverseNoCopy(),
                    Id = item.Key
                });
                var index_val = leveld_db.Get(key);
                if (index_val == null)
                {
                    continue;
                }

                object value = null;
                if (info.ColumnType == LinqDbTypes.int_)
                {
                    if (item.Value != null)
                    {
                        value = BitConverter.ToInt32(item.Value, 0);
                    }
                }
                else if (info.ColumnType == LinqDbTypes.double_)
                {
                    if (item.Value != null)
                    {
                        value = BitConverter.ToDouble(item.Value, 0);
                    }
                }
                else if (info.ColumnType == LinqDbTypes.DateTime_)
                {
                    if (item.Value != null)
                    {
                        var ms = BitConverter.ToDouble(item.Value, 0);
                        value = new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(ms);
                    }
                }
                else if (info.ColumnType == LinqDbTypes.string_)
                {
                    if (item.Value != null)
                    {
                        value = Encoding.UTF8.GetString(item.Value);
                    }
                }
                else if (info.ColumnType == LinqDbTypes.binary_)
                {
                    value = item.Value;
                }


                if (info.ColumnType == LinqDbTypes.string_)
                {
                    SaveStringData(batch, (string)value, info.ColumnName, info.TableInfo, item.Key, string_cache, false);
                }
                else if (info.ColumnType == LinqDbTypes.binary_)
                {
                    SaveBinaryColumn(batch, value, info.ColumnName, info.TableInfo, item.Key, false);
                }
                else
                {
                    IndexDeletedData index_deleted = null;
                    IndexNewData index_new = null;
                    IndexChangedData index_changed = null;
                    if (memory_index_meta.ContainsKey(info.ColumnName))
                    {
                        index_deleted = memory_index_meta[info.ColumnName].Item2;
                        index_new = memory_index_meta[info.ColumnName].Item1;
                        index_changed = memory_index_meta[info.ColumnName].Item3;
                    }
                    SaveDataColumn(batch, value, info.ColumnName, info.ColumnType, info.TableInfo, item.Key, false, index_new, index_changed);
                }
            }
            //WriteStringCacheToBatch(batch, string_cache, table_info);
        }

        void SaveServerIncrement(List<BinData> Data, Command comm, ServerResult server_result, WriteBatchWithConstraints trans_batch, Dictionary<string, int> trans_count_cache, Dictionary<string, KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>> scache)
        {
            if (Data != null && Data.Any())
            {
                var _write_lock = GetTableWriteLock(comm.TableName);
                lock (_write_lock)
                {

                    if (trans_batch == null)
                    {
                        using (WriteBatchWithConstraints batch = new WriteBatchWithConstraints())
                        {
                            var table_info = CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
                            Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> meta_index = BuildMetaOnIndex(table_info);
                            var string_cache = new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>();
                            server_result.Ids = SaveItems(Data, comm.TableDefOrder, batch, table_info, comm.TableName, trans_count_cache, string_cache, meta_index, false);
                            WriteStringCacheToBatch(batch, string_cache, table_info);
                            var snapshots_dic = InsertIndexChanges(table_info, meta_index);
                            foreach (var snap in snapshots_dic)
                            {
                                var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[snap.Key]);
                                batch.Put(skey, Encoding.UTF8.GetBytes(snap.Value));
                            }
                            leveld_db.Write(batch._writeBatch);
                        }
                    }
                    else
                    {
                        var table_info = CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
                        if (!scache.ContainsKey(table_info.Name))
                        {
                            scache[table_info.Name] = new KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>(table_info, new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>());
                        }
                        Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> meta_index = BuildMetaOnIndex(table_info);
                        server_result.Ids = SaveItems(Data, comm.TableDefOrder, trans_batch, table_info, comm.TableName, trans_count_cache, scache[table_info.Name].Value, meta_index, true);
                        var snapshots_dic = InsertIndexChanges(table_info, meta_index);
                        foreach (var snap in snapshots_dic)
                        {
                            var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[snap.Key]);
                            trans_batch.Put(skey, Encoding.UTF8.GetBytes(snap.Value));
                        }
                    }

                }
            }
        }

        Tuple<HashSet<int>, List<BinData>> GetIdsServer(List<BinData> items, HashSet<int> exclude, List<BinData> old_items, Dictionary<string, short> TableDefOrder, List<int> all_ids, out int count)
        {
            count = 0;
            if (exclude == null)
            {
                var res = new HashSet<int>();
                foreach (var item in items)
                {
                    if (!TableDefOrder.ContainsKey("Id"))
                    {
                        throw new LinqDbException("Linqdb: type must have integer Id property");
                    }
                    var id = BitConverter.ToInt32(item.Bytes[TableDefOrder["Id"]], 0);
                    if (id == 0)
                    {
                        count++;
                    }
                    all_ids.Add(id);
                    res.Add(id);
                }
                return new Tuple<HashSet<int>, List<BinData>>(res, null);
            }
            else
            {
                var res = new HashSet<int>();
                foreach (var item in items)
                {
                    if (!TableDefOrder.ContainsKey("Id"))
                    {
                        throw new LinqDbException("Linqdb: type must have integer Id property");
                    }
                    var id = BitConverter.ToInt32(item.Bytes[TableDefOrder["Id"]], 0);
                    if (id == 0 || !exclude.Contains(id))
                    {
                        res.Add(id);
                        old_items.Add(item);
                    }
                    if (id == 0)
                    {
                        count++;
                    }
                    all_ids.Add(id);
                }
                exclude.UnionWith(res);
                return new Tuple<HashSet<int>, List<BinData>>(exclude, old_items);
            }
        }
        void SaveServer(List<BinData> Data, Command comm, ServerResult server_result, WriteBatchWithConstraints trans_batch, Dictionary<string, int> trans_count_cache, Dictionary<string, KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>> scache)
        {
            if (Data != null && Data.Any())
            {
                var _write_lock = GetTableWriteLock(comm.TableName);

                if (trans_batch == null)
                {
                    var table_info = CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));

                    bool done = false;
                    string error = null;
                    var all_ids = new List<int>();
                    var ilock = ModifyBatch.GetTableSaveBatchLock(table_info.Name);
                    lock (ilock)
                    {
                        if (!ModifyBatch._save_batch.ContainsKey(table_info.Name))
                        {
                            int count = 0;
                            var ids = GetIdsServer(Data, null, null, comm.TableDefOrder, all_ids, out count).Item1;
                            ModifyBatch._save_batch[table_info.Name] = new SaveData() { CallbacksServer = new List<Tuple<int, Action<string, List<int>>>>(), Ids = ids, ItemsServer = Data };

                            ModifyBatch._save_batch[table_info.Name].CallbacksServer.Add(new Tuple<int, Action<string, List<int>>>(count, (f, sids) =>
                            {
                                done = true;
                                error = f;

                                if (string.IsNullOrEmpty(error))
                                {
                                    for (int i = 0, j = 0; i < all_ids.Count(); i++)
                                    {
                                        if (all_ids[i] == 0)
                                        {
                                            all_ids[i] = sids[j];
                                            j++;
                                        }
                                    }

                                    server_result.Ids = all_ids;
                                }
                            }));
                        }
                        else
                        {
                            int count = 0;
                            var ids = GetIdsServer(Data, ModifyBatch._save_batch[table_info.Name].Ids, ModifyBatch._save_batch[table_info.Name].ItemsServer, comm.TableDefOrder, all_ids, out count);
                            ModifyBatch._save_batch[table_info.Name].Ids = ids.Item1;
                            ModifyBatch._save_batch[table_info.Name].ItemsServer = ids.Item2;

                            ModifyBatch._save_batch[table_info.Name].CallbacksServer.Add(new Tuple<int, Action<string, List<int>>>(count, (f, sids) =>
                            {
                                done = true;
                                error = f;

                                if (string.IsNullOrEmpty(error))
                                {
                                    for (int i = 0, j = 0; i < all_ids.Count(); i++)
                                    {
                                        if (all_ids[i] == 0)
                                        {
                                            all_ids[i] = sids[j];
                                            j++;
                                        }
                                    }

                                    server_result.Ids = all_ids;
                                }
                            }));
                        }

                    }


                    bool lockAcquired = false;
                    int maxWaitMs = 60000;
                    SaveData _save_data = null;
                    try
                    {
                        DateTime start = DateTime.Now;
                        while (!done)
                        {
                            lockAcquired = Monitor.TryEnter(_write_lock, 0);
                            if (lockAcquired)
                            {
                                if (done)
                                {
                                    Monitor.Exit(_write_lock);
                                    lockAcquired = false;
                                    break;
                                }
                                else
                                {
                                    break;
                                }
                            }
                            Thread.Sleep(250);
                            //if ((DateTime.Now - start).TotalMilliseconds > maxWaitMs)
                            //{
                            //    throw new LinqDbException("Linqdb: Save waited too long to acquire write lock. Is the load too high?");
                            //}
                        }
                        if (done)
                        {
                            if (!string.IsNullOrEmpty(error))
                            {
                                throw new LinqDbException(error);
                            }
                            else
                            {
                                return;
                            }
                        }

                        //not done, but have write lock for the table
                        lock (ilock)
                        {
                            _save_data = ModifyBatch._save_batch[table_info.Name];
                            var oval = new SaveData();
                            ModifyBatch._save_batch.TryRemove(table_info.Name, out oval);
                        }
                        var new_ids = new List<int>();
                        if (_save_data.ItemsServer.Any())
                        {
                            using (WriteBatchWithConstraints batch = new WriteBatchWithConstraints())
                            {
                                Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> meta_index = BuildMetaOnIndex(table_info);
                                var string_cache = new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>();
                                var rids = SaveItems(_save_data.ItemsServer, comm.TableDefOrder, batch, table_info, comm.TableName, trans_count_cache, string_cache, meta_index, false, new_ids);
                                WriteStringCacheToBatch(batch, string_cache, table_info);
                                var snapshots_dic = InsertIndexChanges(table_info, meta_index);
                                foreach (var snap in snapshots_dic)
                                {
                                    var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[snap.Key]);
                                    batch.Put(skey, Encoding.UTF8.GetBytes(snap.Value));
                                }
                                leveld_db.Write(batch._writeBatch);
                            }
                        }
                        int cs = 0;
                        foreach (var cb in _save_data.CallbacksServer)
                        {
                            int count = cb.Item1;
                            var crids = new List<int>();
                            for (int j = 0; j < count; j++, cs++)
                            {
                                crids.Add(new_ids[cs]);
                            }
                            cb.Item2(null, crids);
                        }
                    }
                    catch (Exception ex)
                    {
                        if (_save_data != null)
                        {
                            var additionalInfo = ex.Message;
                            if (_save_data.CallbacksServer.Count() > 1)
                            {
                                additionalInfo += " This error could belong to another entity which happened to be in the same batch.";
                            }
                            foreach (var cb in _save_data.CallbacksServer)
                            {
                                cb.Item2(additionalInfo, new List<int>());
                            }
                        }
                        throw;
                    }
                    finally
                    {
                        if (lockAcquired)
                        {
                            Monitor.Exit(_write_lock);
                        }
                    }
                }
                else
                {
                    var table_info = CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
                    if (!scache.ContainsKey(table_info.Name))
                    {
                        scache[table_info.Name] = new KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>(table_info, new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>());
                    }
                    Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> meta_index = BuildMetaOnIndex(table_info);
                    server_result.Ids = SaveItems(Data, comm.TableDefOrder, trans_batch, table_info, comm.TableName, trans_count_cache, scache[table_info.Name].Value, meta_index, true);
                    var snapshots_dic = InsertIndexChanges(table_info, meta_index);
                    foreach (var snap in snapshots_dic)
                    {
                        var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[snap.Key]);
                        trans_batch.Put(skey, Encoding.UTF8.GetBytes(snap.Value));
                    }
                }
            }
        }
        void UpdateServer(Dictionary<int, byte[]> UpdateData, Command comm, string selector, WriteBatchWithConstraints trans_batch, Dictionary<string, KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>> scache)
        {
            if (UpdateData != null && UpdateData.Any())
            {
                if (trans_batch == null)
                {
                    var table_info = CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
                    bool done = false;
                    string error = null;
                    var ilock = ModifyBatch.GetTableUpdateBatchLock(table_info.Name);
                    var key = table_info.Name + "|" + selector;
                    lock (ilock)
                    {
                        if (!ModifyBatch._update_batch.ContainsKey(key))
                        {
                            ModifyBatch._update_batch[key] = new UpdateData() { Callbacks = new List<Action<string>>(), valuesServer = UpdateData };
                        }
                        else
                        {
                            var vals = ModifyBatch._update_batch[key].valuesServer;
                            foreach (var v in UpdateData)
                            {
                                if (!vals.ContainsKey(v.Key))
                                {
                                    vals[v.Key] = v.Value;
                                }
                            }
                        }
                        ModifyBatch._update_batch[key].Callbacks.Add(f =>
                        {
                            done = true;
                            error = f;
                        });
                    }

                    var _write_lock = GetTableWriteLock(comm.TableName);

                    bool lockAcquired = false;
                    int maxWaitMs = 60000;
                    UpdateData _update_data = null;
                    try
                    {
                        DateTime start = DateTime.Now;
                        while (!done)
                        {
                            lockAcquired = Monitor.TryEnter(_write_lock, 0);
                            if (lockAcquired)
                            {
                                if (done)
                                {
                                    Monitor.Exit(_write_lock);
                                    lockAcquired = false;
                                    break;
                                }
                                else
                                {
                                    break;
                                }
                            }
                            Thread.Sleep(250);
                            //if ((DateTime.Now - start).TotalMilliseconds > maxWaitMs)
                            //{
                            //    throw new LinqDbException("Linqdb: Update waited too long to acquire write lock. Is the load too high?");
                            //}
                        }
                        if (done)
                        {
                            if (!string.IsNullOrEmpty(error))
                            {
                                throw new LinqDbException(error);
                            }
                            else
                            {
                                return;
                            }
                        }

                        //not done, but have write lock for the table
                        lock (ilock)
                        {
                            _update_data = ModifyBatch._update_batch[key];
                            var oval = new UpdateData();
                            ModifyBatch._update_batch.TryRemove(key, out oval);
                        }
                        if (_update_data.valuesServer.Any())
                        {
                            using (WriteBatchWithConstraints batch = new WriteBatchWithConstraints())
                            {
                                Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> meta_index = BuildMetaOnIndex(table_info);
                                var string_cache = new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>();
                                var name = selector;
                                var info = new UpdateInfo()
                                {
                                    TableNumber = table_info.TableNumber,
                                    ColumnNumber = table_info.ColumnNumbers[name],
                                    ColumnType = table_info.Columns[name],
                                    TableInfo = table_info,
                                    ColumnName = name
                                };
                                UpdateItems(info, _update_data.valuesServer, table_info, batch, string_cache, meta_index);
                                WriteStringCacheToBatch(batch, string_cache, table_info);
                                var snapshots_dic = InsertIndexChanges(table_info, meta_index);
                                foreach (var snap in snapshots_dic)
                                {
                                    var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[snap.Key]);
                                    batch.Put(skey, Encoding.UTF8.GetBytes(snap.Value));
                                }
                                leveld_db.Write(batch._writeBatch);
                            }

                        }
                        foreach (var cb in _update_data.Callbacks)
                        {
                            cb(null);
                        }
                    }
                    catch (Exception ex)
                    {
                        if (_update_data != null)
                        {
                            var additionalInfo = ex.Message;
                            if (_update_data.Callbacks.Count() > 1)
                            {
                                additionalInfo += " This error could belong to another entity which happened to be in the same batch.";
                            }
                            foreach (var cb in _update_data.Callbacks)
                            {
                                cb(additionalInfo);
                            }
                        }
                        throw;
                    }
                    finally
                    {
                        if (lockAcquired)
                        {
                            Monitor.Exit(_write_lock);
                        }
                    }
                }
                else
                {
                    var table_info = CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
                    if (!scache.ContainsKey(table_info.Name))
                    {
                        scache[table_info.Name] = new KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>(table_info, new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>());
                    }
                    var name = selector;
                    var info = new UpdateInfo()
                    {
                        TableNumber = table_info.TableNumber,
                        ColumnNumber = table_info.ColumnNumbers[name],
                        ColumnType = table_info.Columns[name],
                        TableInfo = table_info,
                        ColumnName = name
                    };
                    Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> meta_index = BuildMetaOnIndex(table_info);
                    UpdateItems(info, UpdateData, table_info, trans_batch, scache[table_info.Name].Value, meta_index);
                    var snapshots_dic = InsertIndexChanges(table_info, meta_index);
                    foreach (var snap in snapshots_dic)
                    {
                        var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[snap.Key]);
                        trans_batch.Put(skey, Encoding.UTF8.GetBytes(snap.Value));
                    }
                }
            }
        }
        void DeleteServer(HashSet<int> DeleteIds, Command comm, WriteBatchWithConstraints trans_batch, Dictionary<string, int> trans_count_cache, Dictionary<string, KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>> scache)
        {
            if (DeleteIds != null && DeleteIds.Any())
            {
                if (trans_batch == null)
                {
                    var _write_lock = GetTableWriteLock(comm.TableName);

                    var table_info = CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
                    bool done = false;
                    string error = null;
                    var ilock = ModifyBatch.GetTableDeleteBatchLock(table_info.Name);
                    lock (ilock)
                    {
                        if (!ModifyBatch._delete_batch.ContainsKey(table_info.Name))
                        {
                            ModifyBatch._delete_batch[table_info.Name] = new DeleteData() { Callbacks = new List<Action<string>>(), ids = DeleteIds };
                        }
                        else
                        {
                            ModifyBatch._delete_batch[table_info.Name].ids.UnionWith(DeleteIds);
                        }
                        ModifyBatch._delete_batch[table_info.Name].Callbacks.Add(f =>
                        {
                            done = true;
                            error = f;
                        });
                    }

                    bool lockAcquired = false;
                    int maxWaitMs = 60000;
                    DeleteData _delete_data = null;
                    try
                    {
                        DateTime start = DateTime.Now;
                        while (!done)
                        {
                            lockAcquired = Monitor.TryEnter(_write_lock, 0);
                            if (lockAcquired)
                            {
                                if (done)
                                {
                                    Monitor.Exit(_write_lock);
                                    lockAcquired = false;
                                    break;
                                }
                                else
                                {
                                    break;
                                }
                            }
                            Thread.Sleep(250);
                            //if ((DateTime.Now - start).TotalMilliseconds > maxWaitMs)
                            //{
                            //    throw new LinqDbException("Linqdb: Delete waited too long to acquire write lock. Is the load too high?");
                            //}
                        }
                        if (done)
                        {
                            if (!string.IsNullOrEmpty(error))
                            {
                                throw new LinqDbException(error);
                            }
                            else
                            {
                                return;
                            }
                        }

                        //not done, but have write lock for the table
                        lock (ilock)
                        {
                            _delete_data = ModifyBatch._delete_batch[table_info.Name];
                            var oval = new DeleteData();
                            ModifyBatch._delete_batch.TryRemove(table_info.Name, out oval);
                        }
                        if (_delete_data.ids.Any())
                        {
                            using (WriteBatchWithConstraints batch = new WriteBatchWithConstraints())
                            {
                                var string_cache = new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>();
                                Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> meta_index = BuildMetaOnIndex(table_info);
                                Deleteitems(_delete_data.ids, batch, table_info, trans_count_cache, string_cache, meta_index);
                                WriteStringCacheToBatch(batch, string_cache, table_info);
                                var snapshots_dic = InsertIndexChanges(table_info, meta_index);
                                foreach (var snap in snapshots_dic)
                                {
                                    var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[snap.Key]);
                                    batch.Put(skey, Encoding.UTF8.GetBytes(snap.Value));
                                }
                                leveld_db.Write(batch._writeBatch);
                            }
                        }
                        foreach (var cb in _delete_data.Callbacks)
                        {
                            cb(null);
                        }
                    }
                    catch (Exception ex)
                    {
                        if (_delete_data != null)
                        {
                            var additionalInfo = ex.Message;
                            if (_delete_data.Callbacks.Count() > 1)
                            {
                                additionalInfo += " This error could belong to another entity which happened to be in the same batch.";
                            }
                            foreach (var cb in _delete_data.Callbacks)
                            {
                                cb(additionalInfo);
                            }
                        }
                        throw;
                    }
                    finally
                    {
                        if (lockAcquired)
                        {
                            Monitor.Exit(_write_lock);
                        }
                    }
                }
                else
                {
                    var table_info = CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
                    if (!scache.ContainsKey(table_info.Name))
                    {
                        scache[table_info.Name] = new KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>(table_info, new Dictionary<string, KeyValuePair<byte[], HashSet<int>>>());
                    }
                    Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> meta_index = BuildMetaOnIndex(table_info);
                    Deleteitems(DeleteIds, trans_batch, table_info, trans_count_cache, scache[table_info.Name].Value, meta_index);
                    var snapshots_dic = InsertIndexChanges(table_info, meta_index);
                    foreach (var snap in snapshots_dic)
                    {
                        var skey = MakeSnapshotKey(table_info.TableNumber, table_info.ColumnNumbers[snap.Key]);
                        trans_batch.Put(skey, Encoding.UTF8.GetBytes(snap.Value));
                    }
                }
            }
        }
        void IncrementServer(Command comm, ServerResult server_result, IDbQueryable<object> iq, ClientResult inst, WriteBatchWithConstraints trans_batch, Dictionary<string, int> trans_count_cache, Dictionary<string, KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>> scache)
        {
            if (inst.Data == null || !inst.Data.Any())
            {
                throw new LinqDbException("Linqdb: new_item_if_doesnt_exist can't be null");
            }
            if (trans_batch != null)
            {
                throw new LinqDbException("Linqdb: transactions are not supported with AtomicIncrement.");
            }
            var table_info = CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
            var _write_lock = GetTableWriteLock(comm.TableName);
            lock (_write_lock)
            {
                SelectServer(comm, iq, server_result, "select", inst.AnonSelect);

                if (server_result.Total > 1)
                {
                    throw new LinqDbException("Linqdb: more than one item identified");
                }
                if (server_result.Total == 0)
                {
                    SaveServerIncrement(inst.Data, comm, server_result, trans_batch, trans_count_cache, scache);
                    return;
                }
                else
                {
                    if (inst.Selector == "Id")
                    {
                        throw new LinqDbException("Linqdb: can't modify Id property");
                    }
                    var bov = server_result.SelectEntityResult[inst.Selector].Item2.Skip(1).Take(4).ToArray();
                    int old_val = BitConverter.ToInt32(new byte[4] { bov[3], bov[2], bov[1], bov[0] }, 0);
                    if (inst.Inc_old_val == null || inst.Inc_old_val == old_val)
                    {
                        var bid = server_result.SelectEntityResult["Id"].Item2.Skip(1).Take(4).ToArray();
                        int id = BitConverter.ToInt32(new byte[4] { bid[3], bid[2], bid[1], bid[0] }, 0);
                        var dic = new Dictionary<int, byte[]>() { { id, BitConverter.GetBytes(old_val + inst.Inc_val) } };
                        UpdateServer(dic, comm, inst.Selector, trans_batch, scache);
                    }
                    server_result.Old_value = old_val;
                }
            }
        }
        //void IncrementServer1(Command comm, ServerResult server_result, IDbQueryable<object> iq, ClientResult inst, WriteBatchWithConstraints trans_batch, Dictionary<string, int> trans_count_cache, Dictionary<string, KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>> scache)
        //{
        //    if (inst.Data == null || !inst.Data.Any())
        //    {
        //        throw new LinqDbException("Linqdb: new_item_if_doesnt_exist can't be null");
        //    }
        //    if (trans_batch != null)
        //    {
        //        throw new LinqDbException("Linqdb: transactions are not supported with AtomicIncrement.");
        //    }
        //    var table_info = CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));

        //    ulong where_hash = GetWhereHash(comm.TableName, iq.LDBTree);
        //    bool done = false;
        //    string error = null;
        //    var ilock = ModifyBatch.GetTableIncrementBatchLock(comm.TableName);
        //    lock (ilock)
        //    {
        //        if (!ModifyBatch._increment_batch.ContainsKey(where_hash))
        //        {
        //            ModifyBatch._increment_batch[where_hash] = new IncrementData()
        //            {
        //                Callbacks = new List<Action<string>>(),
        //                NewObject = inst.Data,
        //                Value = 0,
        //                ValueIfNew = 0
        //            };
        //        }
        //        else
        //        {
        //            ModifyBatch._increment_batch[where_hash].ValueIfNew += inst.Inc_val;
        //        }
        //        ModifyBatch._increment_batch[where_hash].Value += inst.Inc_val;
        //        ModifyBatch._increment_batch[where_hash].Callbacks.Add(f =>
        //        {
        //            done = true;
        //            error = f;
        //        });
        //    }

        //    var _write_lock = GetTableWriteLock(comm.TableName);

        //    bool lockAcquired = false;
        //    int maxWaitMs = 60000;
        //    IncrementData _increment_data = null;
        //    try
        //    {
        //        DateTime start = DateTime.Now;
        //        while (!done)
        //        {
        //            lockAcquired = Monitor.TryEnter(_write_lock, 0);
        //            if (lockAcquired)
        //            {
        //                if (done)
        //                {
        //                    Monitor.Exit(_write_lock);
        //                    lockAcquired = false;
        //                    break;
        //                }
        //                else
        //                {
        //                    break;
        //                }
        //            }
        //            Thread.Sleep(250);
        //            //if ((DateTime.Now - start).TotalMilliseconds > maxWaitMs)
        //            //{
        //            //    throw new LinqDbException("Linqdb: AtomicIncrement waited too long to acquire write lock. Is the load too high?");
        //            //}
        //        }
        //        if (done)
        //        {
        //            if (!string.IsNullOrEmpty(error))
        //            {
        //                throw new LinqDbException(error);
        //            }
        //            else
        //            {
        //                return;
        //            }
        //        }

        //        //not done, but have write lock for the table
        //        lock (ilock)
        //        {
        //            _increment_data = ModifyBatch._increment_batch[where_hash];
        //            var oval = new IncrementData();
        //            ModifyBatch._increment_batch.TryRemove(where_hash, out oval);
        //        }


        //        SelectServer(comm, iq, server_result, "select", inst.AnonSelect);
        //        if (server_result.Total > 1)
        //        {
        //            throw new LinqDbException("Linqdb: more than one item identified");
        //        }
        //        if (server_result.Total == 0)
        //        {
        //            var bytes = ((List<BinData>)_increment_data.NewObject)[0].Bytes;
        //            var initial = BitConverter.ToInt32(bytes[comm.TableDefOrder[inst.Selector]], 0);
        //            var new_value = initial + _increment_data.ValueIfNew;
        //            var new_bytes = BitConverter.GetBytes(new_value);

        //            bytes[comm.TableDefOrder[inst.Selector]][0] = new_bytes[0];
        //            bytes[comm.TableDefOrder[inst.Selector]][1] = new_bytes[1];
        //            bytes[comm.TableDefOrder[inst.Selector]][2] = new_bytes[2];
        //            bytes[comm.TableDefOrder[inst.Selector]][3] = new_bytes[3];

        //            SaveServerIncrement(inst.Data, comm, server_result, trans_batch, trans_count_cache, scache);
        //        }
        //        else
        //        {
        //            if (inst.Selector == "Id")
        //            {
        //                throw new LinqDbException("Linqdb: can't modify Id property");
        //            }
        //            var bov = server_result.SelectEntityResult[inst.Selector].Item2.Skip(1).Take(4).ToArray();
        //            int old_val = BitConverter.ToInt32(new byte[4] { bov[3], bov[2], bov[1], bov[0] }, 0);
        //            var bid = server_result.SelectEntityResult["Id"].Item2.Skip(1).Take(4).ToArray();
        //            int id = BitConverter.ToInt32(new byte[4] { bid[3], bid[2], bid[1], bid[0] }, 0);
        //            var dic = new Dictionary<int, byte[]>() { { id, BitConverter.GetBytes(old_val + _increment_data.Value) } };
        //            UpdateServer(dic, comm, inst.Selector, trans_batch, scache);

        //        }
        //        foreach (var cb in _increment_data.Callbacks)
        //        {
        //            cb(null);
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        if (_increment_data != null)
        //        {
        //            var additionalInfo = ex.Message;
        //            if (_increment_data.Callbacks.Count() > 1)
        //            {
        //                additionalInfo += " This error could belong to another entity which happened to be in the same batch.";
        //            }
        //            foreach (var cb in _increment_data.Callbacks)
        //            {
        //                cb(additionalInfo);
        //            }
        //        }
        //        throw;
        //    }
        //    finally
        //    {
        //        if (lockAcquired)
        //        {
        //            Monitor.Exit(_write_lock);
        //        }
        //    }
        //}
        void IncrementServer2(Command comm, ServerResult server_result, IDbQueryable<object> iq, ClientResult inst, WriteBatchWithConstraints trans_batch, Dictionary<string, int> trans_count_cache, Dictionary<string, KeyValuePair<TableInfo, Dictionary<string, KeyValuePair<byte[], HashSet<int>>>>> scache)
        {
            if (inst.Data == null || !inst.Data.Any())
            {
                throw new LinqDbException("Linqdb: new_item_if_doesnt_exist can't be null");
            }
            if (trans_batch != null)
            {
                throw new LinqDbException("Linqdb: transactions are not supported with AtomicIncrement.");
            }
            var table_info = CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
            var _write_lock = GetTableWriteLock(comm.TableName);
            lock (_write_lock)
            {
                SelectServer(comm, iq, server_result, "select", inst.AnonSelect);

                if (server_result.Total > 1)
                {
                    throw new LinqDbException("Linqdb: more than one item identified");
                }
                if (server_result.Total == 0)
                {
                    SaveServerIncrement(inst.Data, comm, server_result, trans_batch, trans_count_cache, scache);
                    return;
                }
                else
                {
                    string[] sels = inst.Selector.Split("|".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
                    if (sels[0] == "Id" || sels[1] == "Id")
                    {
                        throw new LinqDbException("Linqdb: can't modify Id property");
                    }
                    //sels[0]
                    var bov = server_result.SelectEntityResult[sels[0]].Item2.Skip(1).Take(4).ToArray();
                    int old_val = BitConverter.ToInt32(new byte[4] { bov[3], bov[2], bov[1], bov[0] }, 0);
                    var bid = server_result.SelectEntityResult["Id"].Item2.Skip(1).Take(4).ToArray();
                    int id = BitConverter.ToInt32(new byte[4] { bid[3], bid[2], bid[1], bid[0] }, 0);
                    var dic = new Dictionary<int, byte[]>() { { id, BitConverter.GetBytes(old_val + Convert.ToInt32(sels[2])) } };
                    UpdateServer(dic, comm, sels[0], trans_batch, scache);
                    //sels[1]
                    bov = server_result.SelectEntityResult[sels[1]].Item2.Skip(1).Take(4).ToArray();
                    old_val = BitConverter.ToInt32(new byte[4] { bov[3], bov[2], bov[1], bov[0] }, 0);
                    dic = new Dictionary<int, byte[]>() { { id, BitConverter.GetBytes(old_val + Convert.ToInt32(sels[3])) } };
                    UpdateServer(dic, comm, sels[1], trans_batch, scache);
                }
            }
        }

        void SelectServer(Command comm, IDbQueryable<object> iq, ServerResult server_result, string type, List<string> anonSelect)
        {
            CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
            using (var snapshot = leveld_db.CreateSnapshot())
            {
                var ro = new ReadOptions().SetSnapshot(snapshot);
                var table_info = GetTableInfo(comm.TableName);
                var where_res = CalculateWhereResult<object>(iq.LDBTree, table_info, ro);
                where_res = FindBetween(iq.LDBTree, where_res, ro);
                where_res = Intersect(iq.LDBTree, where_res, table_info, iq.LDBTree.QueryCache, ro);
                where_res = Search(iq.LDBTree, where_res, ro);
                int row_count = GetTableRowCount(table_info, ro);

                var fres = CombineData(where_res);
                var total = fres.All ? row_count : fres.ResIds.Count();
                server_result.Total = total;
                fres = OrderData(fres, iq.LDBTree, row_count, ro, table_info);
                Dictionary<string, Tuple<List<int>, List<byte>>> data = new Dictionary<string, Tuple<List<int>, List<byte>>>();
                server_result.SelectEntityResult = data;
                server_result.IsOrdered = fres.IsOrdered;
                server_result.OrderedIds = fres.OrderedIds;

                List<string> names;
                if (type == "select")
                {
                    names = comm.TableDef.Keys.ToList();
                }
                else
                {
                    names = anonSelect;
                }
                int count = fres.All ? row_count : fres.ResIds.Count();
                server_result.Count = count;
                var read_counter = new ReadByteCount();
                foreach (var name in names)
                {
                    if (!table_info.Columns.ContainsKey(name))
                    {
                        throw new LinqDbException("Linqdb: Select must not have any expressions, i.e. .Select(f => new { Id = f.Id + 1}) won't work.");
                    }
                    if (fres.All)
                    {
                        data[name] = ReadAllValues(name, table_info, fres, ro, read_counter, count);
                    }
                    else if ((fres.ResIds.Count() / (double)row_count) > 0.40 && fres.ResIds.Count() > 500000)
                    {
                        data[name] = ReadSomeValuesIterator(name, table_info, fres, ro, read_counter, count, row_count);
                    }
                    else
                    {
                        data[name] = ReadSomeValues(name, table_info, fres, iq.LDBTree.QueryCache, ro, read_counter, count, row_count);
                    }
                }

                if (/*fres.IsOrdered &&*/ !data.Keys.Contains("Id"))
                {
                    if (fres.All)
                    {
                        data["Id"] = ReadAllValues("Id", table_info, fres, ro, read_counter, count);
                    }
                    else if ((fres.ResIds.Count() / (double)row_count) > 0.40 && fres.ResIds.Count() > 500000)
                    {
                        data["Id"] = ReadSomeValuesIterator("Id", table_info, fres, ro, read_counter, count, row_count);
                    }
                    else
                    {
                        data["Id"] = ReadSomeValues("Id", table_info, fres, iq.LDBTree.QueryCache, ro, read_counter, count, row_count);
                    }
                }
            }
        }

        void SelectGrouped(Command comm, IDbQueryable<object> iq, ServerResult server_result, string type, List<string> anonSelect)
        {
            CheckTableInfo(comm.TableDef, comm.TableName, CommandHelper.CanWrite(comm.User, comm.Pass));
            using (var snapshot = leveld_db.CreateSnapshot())
            {
                var ro = new ReadOptions().SetSnapshot(snapshot);
                var table_info = GetTableInfo(comm.TableName);
                var where_res = CalculateWhereResult<object>(iq.LDBTree, table_info, ro);
                where_res = FindBetween(iq.LDBTree, where_res, ro);
                where_res = Intersect(iq.LDBTree, where_res, table_info, iq.LDBTree.QueryCache, ro);
                where_res = Search(iq.LDBTree, where_res, ro);
                int row_count = GetTableRowCount(table_info, ro);

                var fres = CombineData(where_res);
                var total = fres.All ? row_count : fres.ResIds.Count();
                server_result.Total = total;

                List<Tuple<string, string>> aggregates = new List<Tuple<string, string>>();
                foreach (var a in anonSelect)
                {
                    var parts = a.ToString().Split(".".ToCharArray());
                    var function = parts[1];
                    string field = null;
                    if (parts.Count() > 2)
                    {
                        field = parts[2].Trim(" (),;".ToCharArray());
                    }
                    aggregates.Add(new Tuple<string, string>(function, field));
                }

                HashSet<int> distinct_groups = new HashSet<int>();
                var cname = comm.Commands.Where(f => f.Type == "groupby").First().Selector;
                server_result.SelectGroupResult = SelectGrouppedCommon(distinct_groups, fres, aggregates, table_info, table_info.ColumnNumbers[cname], cname, ro, row_count);
            }
        }

        void Deleteitems(HashSet<int> ids, WriteBatchWithConstraints batch, TableInfo table_info, Dictionary<string, int> trans_count_cache, Dictionary<string, KeyValuePair<byte[], HashSet<int>>> string_cache, Dictionary<string, Tuple<IndexNewData, IndexDeletedData, IndexChangedData>> memory_index_meta)
        {
            var existing_ids = new HashSet<int>();
            var historic_columns = GetAllColumnsWithHistoric(table_info.TableNumber);
            foreach (var id in ids)
            {
                var key = MakeIndexKey(new IndexKeyInfo()
                {
                    TableNumber = table_info.TableNumber,
                    ColumnNumber = table_info.ColumnNumbers["Id"],
                    Val = BitConverter.GetBytes(id).MyReverseNoCopy(),
                    Id = id
                });
                var index_val = leveld_db.Get(key);
                if (index_val == null)
                {
                    continue;
                }
                existing_ids.Add(id);
                foreach (var column in historic_columns)
                {
                    if (column.Item2 != LinqDbTypes.binary_ && column.Item2 != LinqDbTypes.string_)
                    {
                        var column_name = table_info.ColumnNumbers.Select(f => new { f.Key, f.Value }).Where(f => f.Value == column.Item1).FirstOrDefault();
                        IndexDeletedData index_deleted = null;
                        if (column_name != null && memory_index_meta.ContainsKey(column_name.Key))
                        {
                            index_deleted = memory_index_meta[column_name.Key].Item2;
                        }
                        DeleteDataColumn(batch, table_info.TableNumber, column.Item1, column.Item2, id, index_deleted);
                    }
                    else if (column.Item2 == LinqDbTypes.binary_)
                    {
                        DeleteBinaryColumn(batch, table_info.TableNumber, column.Item1, column.Item2, id);
                    }
                    else
                    {
                        DeleteStringColumn(batch, table_info.TableNumber, column.Item1, table_info, column.Item2, id, string_cache);
                    }
                }
            }
            DecrementCount(table_info, existing_ids, batch, trans_count_cache);
            //WriteStringCacheToBatch(batch, string_cache, table_info);
        }
    }
}
