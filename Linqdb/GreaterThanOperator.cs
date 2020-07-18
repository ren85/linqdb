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
        List<int> GreaterThanOperator(Oper odb, EncodedValue val, bool is_equal, Dictionary<long, byte[]> cache, ReadOptions ro, string snapshot_id, string id_snapshot_id, double? stop_val = null, bool stop_equal = false)
        {
            var index_res = GreaterThanOperatorWithIndex(odb, val, is_equal, ro, snapshot_id, id_snapshot_id, stop_val, stop_equal);
            if (index_res != null)
            {
                return index_res;
            }

            if (val.Type == LinqDbTypes.double_ && val.DoubleVal < 0 || val.Type == LinqDbTypes.DateTime_ && val.DoubleVal < 0 || val.Type == LinqDbTypes.int_ && val.IntVal < 0)
            {
                return OperatorGetAll(odb, cache, ro);
            }
            else
            {
                var result_set = new List<int>();

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

                var key = MakeIndexSearchKey(new IndexKeyInfo()
                {
                    ColumnNumber = odb.ColumnNumber,
                    TableNumber = odb.TableNumber,
                    Val = byte_val,
                    ColumnType = odb.ColumnType
                });
                using (var it = leveld_db.NewIterator(null, ro))
                {
                    it.Seek(key);
                    if (!it.Valid())
                    {
                        return result_set;
                    }
                    var ck = it.Key();
                    if (ck == null)
                    {
                        return result_set;
                    }
                    var kinfo = GetIndexKey(ck);
                    if (kinfo.NotKey || kinfo.TableNumber != odb.TableNumber || kinfo.ColumnNumber != odb.ColumnNumber)
                    {
                        return result_set;
                    }

                    if (stop_val != null)
                    {
                        double cv = 0;
                        if (val.Type == LinqDbTypes.int_)
                        {
                            cv = BitConverter.ToInt32(kinfo.Val.MyReverseWithCopy(), 0);
                        }
                        if (val.Type == LinqDbTypes.double_ || val.Type == LinqDbTypes.DateTime_)
                        {
                            cv = BitConverter.ToDouble(kinfo.Val.MyReverseWithCopy(), 0);
                        }
                        if (cv == stop_val && !stop_equal || cv > stop_val)
                        {
                            return result_set;
                        }
                    }

                    while (ValsEqual(kinfo.Val, byte_val))
                    {
                        if (is_equal)
                        {
                            result_set.Add(kinfo.Id);
                            //PutToCache(kinfo, cache);
                        }
                        it.Next();
                        if (!it.Valid())
                        {
                            return result_set;
                        }
                        ck = it.Key();
                        if (ck == null)
                        {
                            return result_set;
                        }
                        kinfo = GetIndexKey(ck);
                        if (kinfo.NotKey || kinfo.TableNumber != odb.TableNumber || kinfo.ColumnNumber != odb.ColumnNumber)
                        {
                            return result_set;
                        }
                    }

                    if (stop_val != null)
                    {
                        double cv = 0;
                        if (val.Type == LinqDbTypes.int_)
                        {
                            cv = BitConverter.ToInt32(kinfo.Val.MyReverseWithCopy(), 0);
                        }
                        if (val.Type == LinqDbTypes.double_ || val.Type == LinqDbTypes.DateTime_)
                        {
                            cv = BitConverter.ToDouble(kinfo.Val.MyReverseWithCopy(), 0);
                        }
                        if (cv == stop_val && !stop_equal || cv > stop_val)
                        {
                            return result_set;
                        }
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
                        ck = it.Key();
                        if (ck == null)
                        {
                            return result_set;
                        }
                        kinfo = GetIndexKey(ck);
                        if (kinfo.NotKey || kinfo.TableNumber != odb.TableNumber || kinfo.ColumnNumber != odb.ColumnNumber)
                        {
                            return result_set;
                        }

                        if (stop_val != null)
                        {
                            double cv = 0;
                            if (val.Type == LinqDbTypes.int_)
                            {
                                cv = BitConverter.ToInt32(kinfo.Val.MyReverseWithCopy(), 0);
                            }
                            if (val.Type == LinqDbTypes.double_ || val.Type == LinqDbTypes.DateTime_)
                            {
                                cv = BitConverter.ToDouble(kinfo.Val.MyReverseWithCopy(), 0);
                            }
                            if (cv == stop_val && !stop_equal || cv > stop_val)
                            {
                                return result_set;
                            }
                        }

                        result_set.Add(kinfo.Id);
                        //PutToCache(kinfo, cache);
                    }
                }
            }
        }

        List<int> GreaterThanNegativeOperator(Oper odb, EncodedValue val, bool is_equal, Dictionary<long, byte[]> cache, ReadOptions ro, string snapshot_id, string id_snapshot_id)
        {
            if (!string.IsNullOrEmpty(snapshot_id))
            {
                if (indexes.ContainsKey(odb.TableName + "|" + odb.ColumnName + "|" + snapshot_id))
                {
                    return new List<int>();
                }
            }

            var result_set = new List<int>();
            if (val.Type == LinqDbTypes.double_ && val.DoubleVal >= 0 || val.Type == LinqDbTypes.DateTime_ && val.DoubleVal >= 0 || val.Type == LinqDbTypes.int_ && val.IntVal >= 0)
            {
                return result_set;
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
                return LessThanOperator(odb, val, is_equal, cache, ro, snapshot_id, id_snapshot_id);
            }
        }

        List<int> GreaterThanOperatorWithIndex(Oper odb, EncodedValue val, bool is_equal, ReadOptions ro, string snapshot_id, string id_snapshot_id, double? stop_val = null, bool stop_equal = false)
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
                        throw new LinqDbException("Linqdb: can't evaluate < null");
                    }
                    else
                    {
                        int ival = val.IntVal;
                        if (!is_equal && stop_val == null)
                        {
                            int icount = index.Parts.Count();
                            for (int i = 0; i < icount; i++)
                            {
                                var ids = ids_index.Parts[i].IntValues;
                                var iv = index.Parts[i].IntValues;
                                int jcount = iv.Count();
                                for (int j = 0; j < jcount; j++)
                                {
                                    if (ival < iv[j])
                                    {
                                        result.Add((int)ids[j]);
                                    }
                                }
                            }
                        }
                        else if (is_equal && stop_val == null)
                        {
                            int icount = index.Parts.Count();
                            for (int i = 0; i < icount; i++)
                            {
                                var ids = ids_index.Parts[i].IntValues;
                                var iv = index.Parts[i].IntValues;
                                int jcount = iv.Count();
                                for (int j = 0; j < jcount; j++)
                                {
                                    if (ival <= iv[j])
                                    {
                                        result.Add((int)ids[j]);
                                    }
                                }
                            }
                        }
                        else if (!is_equal && stop_val != null)
                        {
                            int stop_val_int = (int)stop_val;
                            if (!stop_equal)
                            {
                                int icount = index.Parts.Count();
                                for (int i = 0; i < icount; i++)
                                {
                                    var ids = ids_index.Parts[i].IntValues;
                                    var iv = index.Parts[i].IntValues;
                                    int jcount = iv.Count();
                                    for (int j = 0; j < jcount; j++)
                                    {
                                        if (ival < iv[j] && iv[j] < stop_val_int)
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
                                        if (ival < iv[j] && iv[j] <= stop_val_int)
                                        {
                                            result.Add((int)ids[j]);
                                        }
                                    }
                                }
                            }
                        }
                        else if (is_equal && stop_val != null)
                        {
                            int stop_val_int = (int)stop_val;
                            if (!stop_equal)
                            {
                                int icount = index.Parts.Count();
                                for (int i = 0; i < icount; i++)
                                {
                                    var ids = ids_index.Parts[i].IntValues;
                                    var iv = index.Parts[i].IntValues;
                                    int jcount = iv.Count();
                                    for (int j = 0; j < jcount; j++)
                                    {
                                        if (ival <= iv[j] && iv[j] < stop_val_int)
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
                                        if (ival <= iv[j] && iv[j] <= stop_val_int)
                                        {
                                            result.Add((int)ids[j]);
                                        }
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
                        double dval = val.DoubleVal;
                        if (!is_equal && stop_val == null)
                        {
                            int icount = index.Parts.Count();
                            for (int i = 0; i < icount; i++)
                            {
                                var ids = ids_index.Parts[i].IntValues;
                                var iv = index.Parts[i].DoubleValues;
                                int jcount = iv.Count();
                                for (int j = 0; j < jcount; j++)
                                {
                                    if (dval < iv[j])
                                    {
                                        result.Add((int)ids[j]);
                                    }
                                }
                            }
                        }
                        else if (is_equal && stop_val == null)
                        {
                            int icount = index.Parts.Count();
                            for (int i = 0; i < icount; i++)
                            {
                                var ids = ids_index.Parts[i].IntValues;
                                var iv = index.Parts[i].DoubleValues;
                                int jcount = iv.Count();
                                for (int j = 0; j < jcount; j++)
                                {
                                    if (dval <= iv[j])
                                    {
                                        result.Add((int)ids[j]);
                                    }
                                }
                            }
                        }
                        else if (!is_equal && stop_val != null)
                        {
                            double stop_val_double = (double)stop_val;
                            if (!stop_equal)
                            {
                                int icount = index.Parts.Count();
                                for (int i = 0; i < icount; i++)
                                {
                                    var ids = ids_index.Parts[i].IntValues;
                                    var iv = index.Parts[i].DoubleValues;
                                    int jcount = iv.Count();
                                    for (int j = 0; j < jcount; j++)
                                    {
                                        if (dval < iv[j] && iv[j] < stop_val_double)
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
                                        if (dval < iv[j] && iv[j] <= stop_val_double)
                                        {
                                            result.Add((int)ids[j]);
                                        }
                                    }
                                }
                            }
                        }
                        else if (is_equal && stop_val != null)
                        {
                            double stop_val_double = (double)stop_val;
                            if (!stop_equal)
                            {
                                int icount = index.Parts.Count();
                                for (int i = 0; i < icount; i++)
                                {
                                    var ids = ids_index.Parts[i].IntValues;
                                    var iv = index.Parts[i].DoubleValues;
                                    int jcount = iv.Count();
                                    for (int j = 0; j < jcount; j++)
                                    {
                                        if (dval <= iv[j] && iv[j] < stop_val_double)
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
                                        if (dval <= iv[j] && iv[j] <= stop_val_double)
                                        {
                                            result.Add((int)ids[j]);
                                        }
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

    public static class Ext
    {
        public static byte[] MyReverseNoCopy(this byte[] val)
        {
            if (val == null || val.Length == 0)
            {
                return val;
            }

            if (val.Length == 8 && val[7] == (byte)128)
            {
                if (val[0] == (byte)0 && val[1] == (byte)0 && val[2] == (byte)0 && val[3] == (byte)0 && val[4] == (byte)0 && val[5] == (byte)0 && val[6] == (byte)0)
                {
                    val[7] = 0; //IEEE 754 floating point numbers have two representations of 0, changing from -0 to +0.
                    return val;
                }
            }
            //double NaN is not supported
            if (val.Length == 8 && val[7] == (byte)255 && val[6] == (byte)248)
            {
                if (val[0] == (byte)0 && val[1] == (byte)0 && val[2] == (byte)0 && val[3] == (byte)0 && val[4] == (byte)0 && val[5] == (byte)0)
                {
                    throw new LinqDbException("Linqdb: double with value NaN is not supported.");
                }
            }
            //negative infinity
            if (val.Length == 8 && val[7] == (byte)255 && val[6] == (byte)240)
            {
                if (val[0] == (byte)0 && val[1] == (byte)0 && val[2] == (byte)0 && val[3] == (byte)0 && val[4] == (byte)0 && val[5] == (byte)0)
                {
                    throw new LinqDbException("Linqdb: double with value NegativeInfinity is not supported.");
                }
            }
            //positive infinity
            if (val.Length == 8 && val[7] == (byte)127 && val[6] == (byte)240)
            {
                if (val[0] == (byte)0 && val[1] == (byte)0 && val[2] == (byte)0 && val[3] == (byte)0 && val[4] == (byte)0 && val[5] == (byte)0)
                {
                    throw new LinqDbException("Linqdb: double with value PositiveInfinity is not supported.");
                }
            }

            byte tmp;
            for (int i = 0; i < val.Length / 2; i++)
            {
                tmp = val[i];
                val[i] = val[val.Length - 1 - i];
                val[val.Length - 1 - i] = tmp;
            }

            return val;
        }
        public static byte[] MyReverseWithCopy(this byte[] val)
        {
            if (val == null || val.Length == 0)
            {
                return val;
            }

            if (val.Length == 8 && val[7] == (byte)128)
            {
                if (val[0] == (byte)0 && val[1] == (byte)0 && val[2] == (byte)0 && val[3] == (byte)0 && val[4] == (byte)0 && val[5] == (byte)0 && val[6] == (byte)0)
                {
                    return new byte[8] { 0, 0, 0, 0, 0, 0, 0, 0 };
                }
            }
            //double NaN is not supported
            if (val.Length == 8 && val[7] == (byte)255 && val[6] == (byte)248)
            {
                if (val[0] == (byte)0 && val[1] == (byte)0 && val[2] == (byte)0 && val[3] == (byte)0 && val[4] == (byte)0 && val[5] == (byte)0)
                {
                    throw new LinqDbException("Linqdb: double with value NaN is not supported.");
                }
            }
            //negative infinity
            if (val.Length == 8 && val[7] == (byte)255 && val[6] == (byte)240)
            {
                if (val[0] == (byte)0 && val[1] == (byte)0 && val[2] == (byte)0 && val[3] == (byte)0 && val[4] == (byte)0 && val[5] == (byte)0)
                {
                    throw new LinqDbException("Linqdb: double with value NegativeInfinity is not supported.");
                }
            }
            //positive infinity
            if (val.Length == 8 && val[7] == (byte)127 && val[6] == (byte)240)
            {
                if (val[0] == (byte)0 && val[1] == (byte)0 && val[2] == (byte)0 && val[3] == (byte)0 && val[4] == (byte)0 && val[5] == (byte)0)
                {
                    throw new LinqDbException("Linqdb: double with value PositiveInfinity is not supported.");
                }
            }

            var res = new byte[val.Length];
            for (int i = 0; i < val.Length; i++)
            {
                res[i] = val[val.Length - i - 1];
            }

            return res;
        }
    }
}
