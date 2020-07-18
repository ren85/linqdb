using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        List<int> NotEqualOperator(Oper odb, EncodedValue val, Dictionary<long, byte[]> cache, ReadOptions ro, string snapshot_id, string id_snapshot_id)
        {
            var index_res = NotEqualOperatorWithIndex(odb, val, ro, snapshot_id, id_snapshot_id);
            if (index_res != null)
            {
                return index_res;
            }

            var r = NotEqualGeneric(odb, val, cache, ro);

            if (val.IsNull)
            {
                return r;
            }

            //negative  
            if (val.IsNull)
            {
                odb.ColumnNumber *= -1;
                var rn = OperatorGetAll(odb, cache, ro);
                r = r.MyUnion(rn);
            }
            else if (odb.ColumnType == LinqDbTypes.double_ || odb.ColumnType == LinqDbTypes.DateTime_)
            {
                if (val.DoubleVal < 0)
                {
                    odb.ColumnNumber *= -1;
                    val.DoubleVal = -1 * val.DoubleVal;
                }
                var rn = NotEqualGeneric(odb, val, cache, ro);
                r = r.MyUnion(rn);
            }
            else if (odb.ColumnType == LinqDbTypes.int_)
            {
                if (val.IntVal < 0)
                {
                    odb.ColumnNumber *= -1;
                    val.IntVal = -1 * val.IntVal;
                }
                var rn = NotEqualGeneric(odb, val, cache, ro);
                r = r.MyUnion(rn);
            }
            else if (odb.ColumnType == LinqDbTypes.string_)
            {
                var rn = NotEqualGeneric(odb, val, cache, ro);
                r = r.MyUnion(rn);
            }

            return r;
        }

        List<int> NotEqualGeneric(Oper odb, EncodedValue val, Dictionary<long, byte[]> cache, ReadOptions ro)
        {
            byte[] byte_val = null;
            if(val.IsNull)
            {
                byte_val = NullConstant;
            }
            else if (odb.ColumnType == LinqDbTypes.double_ || odb.ColumnType == LinqDbTypes.DateTime_)
            {
                byte_val = BitConverter.GetBytes(val.DoubleVal).MyReverseNoCopy();
            }
            else if ((odb.ColumnType == LinqDbTypes.int_))
            {
                byte_val = BitConverter.GetBytes(val.IntVal).MyReverseNoCopy();
            }
            else if ((odb.ColumnType == LinqDbTypes.string_))
            {
                byte_val = val.StringValue;
            }

            var result_set = new List<int>();
            var key = MakeIndexSearchKey(new IndexKeyInfo()
            {
                ColumnNumber = odb.ColumnNumber,
                TableNumber = odb.TableNumber,
                Val = PreNull,
                ColumnType = odb.ColumnType
            });
            using (var it = leveld_db.NewIterator(null, ro))
            {
                it.Seek(key);
                if (!it.Valid())
                {
                    return result_set;
                }
                var v = it.Key();
                if (v == null)
                {
                    return result_set;
                }
                var kinfo = GetIndexKey(v);
                if (kinfo.NotKey || kinfo.TableNumber != odb.TableNumber || kinfo.ColumnNumber != odb.ColumnNumber)
                {
                    return result_set;
                }
                if (!ValsEqual(kinfo.Val, byte_val))
                {
                    result_set.Add(kinfo.Id);
                    //PutToCache(kinfo, cache);
                }

                while (true)
                {
                    it.Next();
                    if (!it.Valid())
                    {
                        return result_set;
                    }
                    var ckey = it.Key();
                    if (ckey == null)
                    {
                        return result_set;
                    }
                    kinfo = GetIndexKey(ckey);
                    if (kinfo.NotKey || kinfo.TableNumber != odb.TableNumber || kinfo.ColumnNumber != odb.ColumnNumber)
                    {
                        return result_set;
                    }
                    if (!ValsEqual(kinfo.Val, byte_val))
                    {
                        result_set.Add(kinfo.Id);
                        //PutToCache(kinfo, cache);
                    }
                    else
                    {
                        continue;
                    }
                }
            }
        }

        List<int> NotEqualOperatorWithIndex(Oper odb, EncodedValue val, ReadOptions ro, string snapshot_id, string id_snapshot_id)
        {
            List<int> result = new List<int>();

            if (string.IsNullOrEmpty(snapshot_id))
            {
                return null;
            }
            if (string.IsNullOrEmpty(odb.TableName) || string.IsNullOrEmpty(odb.ColumnName))
            {
                throw new LinqDbException("Linqdb: bad indexes.");
            }
            if (!indexes.ContainsKey(odb.TableName + "|" + odb.ColumnName + "|" + snapshot_id))
            {
                return null;
            }
            var index = indexes[odb.TableName + "|" + odb.ColumnName + "|" + snapshot_id];
            var ids_index = indexes[odb.TableName + "|Id|" + id_snapshot_id];
            if (index.IndexType == IndexType.GroupOnly)
            {
                return null;
            }

            switch (odb.ColumnType)
            {
                case LinqDbTypes.int_:
                    if (val.IsNull)
                    {
                        int icount = index.Parts.Count();
                        for (int i = 0; i < icount; i++)
                        {
                            var ids = ids_index.Parts[i].IntValues;
                            var iv = index.Parts[i].IntValues;
                            int jcount = iv.Count();
                            for (int j = 0; j < jcount; j++)
                            {
                                if (iv[j] != null)
                                {
                                    result.Add((int)ids[j]);
                                }
                            }
                        }
                    }
                    else
                    {
                        int icount = index.Parts.Count();
                        for (int i = 0; i < icount; i++)
                        {
                            var ids = ids_index.Parts[i].IntValues;
                            var iv = index.Parts[i].IntValues;
                            int jcount = iv.Count();
                            for (int j = 0; j < jcount; j++)
                            {
                                if (iv[j] != val.IntVal)
                                {
                                    result.Add((int)ids[j]);
                                }
                            }
                        }
                    }
                    break;
                case LinqDbTypes.double_:
                case LinqDbTypes.DateTime_:
                    if (val.IsNull)
                    {
                        int icount = index.Parts.Count();
                        for (int i = 0; i < icount; i++)
                        {
                            var ids = ids_index.Parts[i].IntValues;
                            var iv = index.Parts[i].DoubleValues;
                            int jcount = iv.Count();
                            for (int j = 0; j < jcount; j++)
                            {
                                if (iv[j] != null)
                                {
                                    result.Add((int)ids[j]);
                                }
                            }
                        }
                    }
                    else
                    {
                        int icount = index.Parts.Count();
                        for (int i = 0; i < icount; i++)
                        {
                            var ids = ids_index.Parts[i].IntValues;
                            var iv = index.Parts[i].DoubleValues;
                            int jcount = iv.Count();
                            for (int j = 0; j < jcount; j++)
                            {
                                if (iv[j] != val.DoubleVal)
                                {
                                    result.Add((int)ids[j]);
                                }
                            }
                        }
                    }
                    break;
                default:
                    return null;
            }

            return result;
        }
    }
}
