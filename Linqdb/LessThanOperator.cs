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
        List<int> LessThanOperator(Oper odb, EncodedValue val, bool or_equal, Dictionary<long, byte[]> cache, ReadOptions ro, string snapshot_id, string id_snapshot_id)
        {
            if (!string.IsNullOrEmpty(snapshot_id))
            {
                var index_res = LessThanOperatorWithIndex(odb, val, or_equal, ro, snapshot_id, id_snapshot_id);
                if (index_res != null)
                {
                    return index_res;
                }
            }

            var result_set = new List<int>();

            if (val.Type == LinqDbTypes.double_ && val.DoubleVal < 0 || val.Type == LinqDbTypes.DateTime_ && val.DoubleVal < 0 || val.Type == LinqDbTypes.int_ && val.IntVal < 0)
            {
                return result_set;
            }
            else
            {
                byte[] byte_val = null;
                if (val.IsNull)
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

                using (var it = leveld_db.NewIterator(null, ro))
                {
                    var pkey = MakeIndexKey(new IndexKeyInfo()
                    {
                        TableNumber = odb.TableNumber,
                        ColumnNumber = odb.ColumnNumber,
                        Val = PostNullConstantPreEverythingElse,
                        ColumnType = odb.ColumnType
                    });
                    it.Seek(pkey); //skip null values

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

                    if (ValGreater(byte_val, kinfo.Val) || (or_equal && ValsEqual(kinfo.Val, byte_val)))
                    {
                        result_set.Add(kinfo.Id);
                        //PutToCache(kinfo, cache);
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
                            if (ValGreater(byte_val, kinfo.Val) || (or_equal && ValsEqual(kinfo.Val, byte_val)))
                            {
                                result_set.Add(kinfo.Id);
                                //PutToCache(kinfo, cache);
                            }
                            else
                            {
                                return result_set;
                            }
                        }
                    }
                }

                return result_set;
            }
        }

        List<int> LessThanNegativeOperator(Oper odb, EncodedValue val, bool is_equal, Dictionary<long, byte[]> cache, ReadOptions ro, string snapshot_id, string id_snapshot_id)
        {
            if (!string.IsNullOrEmpty(snapshot_id))
            {
                if (indexes.ContainsKey(odb.TableName + "|" + odb.ColumnName + "|" + snapshot_id))
                {
                    return new List<int>();
                }
            }            

            if (val.Type == LinqDbTypes.double_ && val.DoubleVal >= 0 || val.Type == LinqDbTypes.DateTime_ && val.DoubleVal >= 0 || val.Type == LinqDbTypes.int_ && val.IntVal >= 0)
            {
                return OperatorGetAll(odb, cache, ro);
            }
            else
            {
                if (val.Type == LinqDbTypes.double_)
                {
                    val.DoubleVal *= -1;
                }
                if (val.Type == LinqDbTypes.int_)
                {
                    val.IntVal *= -1;
                }
                return GreaterThanOperator(odb, val, is_equal, cache, ro, snapshot_id, id_snapshot_id);
            }
        }

        List<int> OperatorGetAll(Oper odb, Dictionary<long, byte[]> cache, ReadOptions ro)
        {
            var result_set = new List<int>();

            using (var it = leveld_db.NewIterator(null, ro))
            {
                var key = MakeIndexSearchKey(new IndexKeyInfo()
                {
                    ColumnNumber = odb.ColumnNumber,
                    TableNumber = odb.TableNumber,
                    Val = PostNullConstantPreEverythingElse,
                    ColumnType = odb.ColumnType
                });

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

                result_set.Add(kinfo.Id);
                //PutToCache(kinfo, cache);
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
                    result_set.Add(kinfo.Id);
                    //PutToCache(kinfo, cache);
                }
            }
        }


        List<int> LessThanOperatorWithIndex(Oper odb, EncodedValue val, bool is_equal, ReadOptions ro, string snapshot_id, string id_snapshot_id)
        {
            List<int> result = new List<int>();

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
                        throw new LinqDbException("Linqdb: can't evaluate > null");
                    }
                    else
                    {
                        if (!is_equal)
                        {
                            int icount = index.Parts.Count();
                            for (int i = 0; i < icount; i++)
                            {
                                var ids = ids_index.Parts[i].IntValues;
                                var iv = index.Parts[i].IntValues;
                                int jcount = iv.Count();
                                for (int j = 0; j < jcount; j++)
                                {
                                    if (val.IntVal > iv[j])
                                    {
                                        int id = (int)ids[j];
                                        result.Add(id);
                                    }
                                }
                            }
                        }
                        else if (is_equal)
                        {
                            int icount = index.Parts.Count();
                            for (int i = 0; i < icount; i++)
                            {
                                var ids = ids_index.Parts[i].IntValues;
                                var iv = index.Parts[i].IntValues;
                                int jcount = iv.Count();
                                for (int j = 0; j < jcount; j++)
                                {
                                    if (val.IntVal >= iv[j])
                                    {
                                        int id = (int)ids[j];
                                        result.Add(id);
                                    }
                                }
                            }
                        }
                    }
                    break;
                case LinqDbTypes.double_:
                case LinqDbTypes.DateTime_:
                    if (val.IsNull)
                    {
                        throw new LinqDbException("Linqdb: can't evaluate < null");
                    }
                    else
                    {
                        if (!is_equal)
                        {
                            int icount = index.Parts.Count();
                            for (int i = 0; i < icount; i++)
                            {
                                var ids = ids_index.Parts[i].IntValues;
                                var iv = index.Parts[i].DoubleValues;
                                int jcount = iv.Count();
                                for (int j = 0; j < jcount; j++)
                                {
                                    if (val.DoubleVal > iv[j])
                                    {
                                        result.Add((int)ids[j]);
                                    }
                                }
                            }
                        }
                        else if (is_equal)
                        {
                            int icount = index.Parts.Count();
                            for (int i = 0; i < icount; i++)
                            {
                                var ids = ids_index.Parts[i].IntValues;
                                var iv = index.Parts[i].DoubleValues;
                                int jcount = iv.Count();
                                for (int j = 0; j < jcount; j++)
                                {
                                    if (val.DoubleVal >= iv[j])
                                    {
                                        result.Add((int)ids[j]);
                                    }
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
