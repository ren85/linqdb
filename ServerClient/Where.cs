using ServerSharedData;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbClientInternal
{
    public partial class Ldb
    {
        public ClientResult Where<T>(Expression<Func<T, bool>> predicate)
        {
            var res = new ClientResult();
            res.Type = "where";

            Stack<Oper> wstack = new Stack<Oper>();
            ParseBinExpr(predicate.Body as BinaryExpression, predicate.Parameters.ToList(), wstack, typeof(T));
            
            List<SharedOper> opers = new List<SharedOper>();            
            res.Opers = opers;
            while (wstack.Any())
            {
                var op = wstack.Pop();
                byte[] ndb = null;

                if (!op.IsDb && !op.IsOperator)
                {
                    var column = wstack.Peek();
                    op.ColumnName = column.ColumnName;
                }
                if (!op.IsOperator && op.NonDbValue != null)
                {
                    if (op.ColumnName == null)
                    {
                        throw new LinqDbException("Linqdb: error in Where statement, probably field selector is used in the expression. That's not supported, i.e. .Where(f => f.Value % 2 == 0) won't work.");
                    }

                    var def = GetTableDefinition<T>();
                    var type = StringTypeToLinqType(def.Item1[op.ColumnName]);
                    if (type == LinqDbTypes.int_)
                    {
                        ndb = BitConverter.GetBytes((int)op.NonDbValue);
                    }
                    else if (type == LinqDbTypes.double_)
                    {
                        ndb = BitConverter.GetBytes(Convert.ToDouble(op.NonDbValue));
                    }
                    else if (type == LinqDbTypes.string_)
                    {
                        ndb = Encoding.UTF8.GetBytes((string)op.NonDbValue);
                    }
                    else if (type == LinqDbTypes.DateTime_)
                    {
                        var ms = ((DateTime)op.NonDbValue - new DateTime(0001, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
                        ndb = BitConverter.GetBytes(ms);
                    }
                    else if (type == LinqDbTypes.binary_)
                    {
                        ndb = (byte[])op.NonDbValue;
                    }
                }

                var sop = new SharedOper()
                {
                    ColumnName = op.ColumnName,
                    IsDb = op.IsDb,
                    IsOperator = op.IsOperator,
                    NonDbValue = ndb,
                    IsResult = op.IsResult,
                    Type = (short)op.Type
                };
                opers.Add(sop);
            }
            opers.Reverse();
            for(short i = 0; i < opers.Count; i++)
            {
                opers[i].Id = i;
            }

            return res;
        }

        public void ParseBinExpr(BinaryExpression expr, List<ParameterExpression> pars, Stack<Oper> stack, Type table_type)
        {
            var op = new Oper()
            {
                IsOperator = true,
                Type = expr.NodeType
            };
            stack.Push(op);

            var left = expr.Left;
            var right = expr.Right;
            if (left is BinaryExpression)
            {
                ParseBinExpr(left as BinaryExpression, pars, stack, table_type);
            }
            else
            {
                op = FillOpData(left, pars, table_type);
                stack.Push(op);
            }
            if (right is BinaryExpression)
            {
                ParseBinExpr(right as BinaryExpression, pars, stack, table_type);
            }
            else
            {
                try
                {
                    object tmp_val = null;
                    if (right is MemberExpression || right.NodeType == ExpressionType.Convert && (right as UnaryExpression).Operand is MemberExpression)
                    {
                        MemberExpression outerMember = null;
                        if (right is MemberExpression)
                        {
                            outerMember = (MemberExpression)right;
                        }
                        else
                        {
                            outerMember = (right as UnaryExpression).Operand as MemberExpression;
                        }
                        if (outerMember.Member is FieldInfo)
                        {
                            FieldInfo outerProp = (FieldInfo)outerMember.Member;
                            if (outerMember is MemberExpression && outerMember.Member is FieldInfo && (outerMember.Member as FieldInfo).IsStatic)
                            {
                                object source = null;
                                tmp_val = ((FieldInfo)outerMember.Member).GetValue(source);
                            }
                            else if (outerMember.Expression is MemberExpression)
                            {
                                MemberExpression innerMember = (MemberExpression)outerMember.Expression;
                                FieldInfo innerField = (FieldInfo)innerMember.Member;
                                ConstantExpression ce = (ConstantExpression)innerMember.Expression;
                                object innerObj = ce.Value;
                                object outerObj = innerField.GetValue(innerObj);
                                tmp_val = outerProp.GetValue(outerObj);
                            }
                            else if (outerMember.Expression is ConstantExpression)
                            {
                                var constantSelector = (ConstantExpression)outerMember.Expression;
                                tmp_val = ((FieldInfo)outerMember.Member).GetValue(constantSelector.Value);
                            }
                            else
                            {
                                throw new Exception("bad where");
                            }

                        }
                        else if (outerMember.Member is PropertyInfo)
                        {
                            PropertyInfo outerProp = (PropertyInfo)outerMember.Member;
                            if (outerMember.Expression is ConstantExpression)
                            {
                                tmp_val = (outerMember.Member as PropertyInfo).GetValue(((ConstantExpression)outerMember.Expression).Value);
                            }
                            else
                            {
                                MemberExpression innerMember = (MemberExpression)outerMember.Expression;
                                FieldInfo innerField = (FieldInfo)innerMember.Member;
                                ConstantExpression ce = (ConstantExpression)innerMember.Expression;
                                object innerObj = ce.Value;
                                object outerObj = innerField.GetValue(innerObj);
                                tmp_val = outerProp.GetValue(outerObj, null);
                            }
                        }
                        else
                        {
                            throw new Exception("bad where");
                        }
                    }
                    else if (right.NodeType == ExpressionType.Convert)
                    {
                        var operand = (right as UnaryExpression).Operand;
                        if (operand is ConstantExpression)
                        {
                            tmp_val = (operand as ConstantExpression).Value;
                        }
                        else
                        {
                            operand = (operand as UnaryExpression).Operand;
                            tmp_val = (operand as ConstantExpression).Value;
                        }
                    }
                    else if (right.NodeType == ExpressionType.Constant)
                    {
                        tmp_val = (right as ConstantExpression).Value;
                    }
                    else
                    {
                        throw new Exception("bad where");
                    }

                    if (right.NodeType == ExpressionType.MemberAccess || right.NodeType == ExpressionType.Constant || right.NodeType == ExpressionType.Convert)
                    {
                        op = new Oper()
                        {
                            IsDb = false,
                            IsOperator = false,
                            NonDbValue = tmp_val,
                        };
                        stack.Push(op);
                        return;
                    }
                    else
                    {
                        throw new Exception("bad where");
                    }

                    //op = FillOpData(right, pars, table_type);
                    //stack.Push(op);
                }
                catch (Exception ex)
                {
                    throw new LinqDbException("Linqdb: error in Where clause - on the right hand side of the operator there must be member access (first level) or variable or constant.");
                }
            }
        }

        Oper FillOpData(Expression expr, List<ParameterExpression> pars, Type table_type)
        {
            var pname = pars.First().Name;
            if (expr.NodeType == ExpressionType.Convert && !expr.ToString().StartsWith(pname))
            {
                throw new LinqDbException("Linqdb: Error in Where statement, probably type mismatch, for example int and int? are different types and both side of an expression must be of same type. Cast the right hand side's value.");
            }
            //if (!expr.ToString().StartsWith(pname) && !expr.ToString().StartsWith("Convert("+pname))
            //{
            //    throw new LinqDbException("Linqdb: Error in Where statement, probably type mismatch, for example int and int? are different types and both side of an expression must be of same type. Cast the right hand side's value.");
            //}
            var column_name = SharedUtils.GetPropertyName(expr.ToString());
            var op = new Oper()
            {
                Type = expr.NodeType,
                ColumnName = column_name,
                IsOperator = false,
                IsDb = true
            };
            return op;
        }

    }

    public class Oper
    {
        public bool IsOperator { get; set; }
        public ExpressionType Type { get; set; }
        public string ColumnName { get; set; }
        public object NonDbValue { get; set; }
        public bool IsDb { get; set; }
        public short TableNumber { get; set; }
        public short ColumnNumber { get; set; }
        public bool IsResult { get; set; }
        public OperResult Result { get; set; }
        public string StringResult { get; set; }
    }

    public class OperResult
    {
        public bool Skip { get; set; }
        public bool All { get; set; }
        public HashSet<int> ResIds { get; set; }
        public bool IsOrdered { get; set; }
        public Dictionary<int, int> OrderedIds { get; set; }
        public int Id { get; set; }
        public int? OrWith { get; set; }
    }

    public class WhereInfo : BaseInfo
    {
        public Stack<Oper> Opers { get; set; }
    }

    public class BaseInfo
    {
        public int Id { get; set; }
        public int? OrWith { get; set; }
    }
}
