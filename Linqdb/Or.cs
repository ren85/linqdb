using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public OperResult CombineData(List<OperResult> op_list)
        {
            if (!op_list.Any())
            {
                return new OperResult()
                {
                    All = true
                };
            }
            if (op_list.Count() == 1)
            {
                return op_list.First();
            }
            var res = new OperResult()
            {
                All = false,
                ResIds = null
            };

            while (op_list.Any(f => f.OrWith != null))
            {
                List<OperResult> tmp = new List<OperResult>();
                foreach (var orw in op_list)
                {
                    if (orw.Skip)
                    {
                        continue;
                    }
                    if (orw.OrWith != null)
                    {
                        var w = op_list.First(f => f.Id == (int)orw.OrWith);
                        w.Skip = true;
                        tmp.Add(OrOpers(orw, w));
                    }
                    else
                    {
                        tmp.Add(orw);
                    }
                }

                foreach (var t in tmp)
                {
                    t.Skip = false;
                }
                op_list = tmp;
            }

            //only ands left
            op_list = op_list.OrderBy(f => f.All ? Int32.MaxValue : f.ResIds.Count()).ToList();
            res = op_list[0];
            for (int i = 1; i < op_list.Count; i++)
            {
                res = AndOpers(res, op_list[i]);
            }

            return res;
        }
        public OperResult OrOpers(OperResult op1, OperResult op2)
        {
            if (op1.All)
            {
                return op2;
            }
            if (op2.All)
            {
                return op1;
            }

            op2.ResIds = op2.ResIds.MyUnion(op1.ResIds);
            op2.Id = op1.Id;
            return op2;
        }

        public OperResult AndOpers(OperResult op1, OperResult op2)
        {
            if (op1.All)
            {
                return op2;
            }
            if (op2.All)
            {
                return op1;
            }
            op2.Id = op1.Id;
            op2.ResIds = op2.ResIds.MyIntersect(op1.ResIds);

            return op2;

        }
    }
    public static class Ldb_ext
    {
        public static IDbQueryable<T> Or<T>(this IDbQueryable<T> source)
        {
            source.LDBTree.Prev.OrWith = source.LDBTree.Prev.Id + 1;
            return source;
        }
        public static List<int> MyUnion(this List<int> a, List<int> b)
        {
            if (!a.Any())
            {
                return b;
            }
            if (!b.Any())
            {
                return a;
            }

            var max1 = a.Max();
            var max2 = b.Max();

            var min1 = a.Min();
            var min2 = b.Min();

            var small_max = (max1 - min1) > (max2 - min2) ? (max2 - min2) : (max1 - min1);
            var small_min = (max1 - min1) > (max2 - min2) ? min2 : min1;

            List<int> res = new List<int>();
            if (small_max > 100000000)
            {
                a.AddRange(b);
                return a.Distinct().ToList();
            }
            else
            {

                var smaller = (max1 - min1) > (max2 - min2) ? b : a;
                var bigger = (max1 - min1) > (max2 - min2) ? a : b;

                var acount = a.Count();
                var bcount = b.Count();
                var total_el = acount + bcount;

                var check = new List<bool>(small_max + 1);
                for (int i = 0; i < small_max + 1; i++)
                {
                    check.Add(false);
                }

                foreach (var i in smaller)
                {
                    check[i - small_min] = true;
                }

                res = smaller;
                foreach (var big in bigger)
                {
                    var bg = big - small_min;
                    if (bg < 0 || bg > small_max || !check[bg])
                    {
                        res.Add(big);
                    }
                }
                return res;
            }
        }

        public static List<int> MyIntersect(this List<int> a, List<int> b)
        {
            if (!a.Any())
            {
                return new List<int>();
            }
            if (!b.Any())
            {
                return new List<int>();
            }

            var max1 = a.Max();
            var max2 = b.Max();

            var min1 = a.Min();
            var min2 = b.Min();

            var small_max = (max1 - min1) > (max2 - min2) ? (max2 - min2) : (max1 - min1);
            var small_min = (max1 - min1) > (max2 - min2) ? min2 : min1;

            List<int> res = new List<int>();
            if (small_max > 100000000)
            {
                res = a.Intersect(b).ToList();
            }
            else
            {

                var smaller = (max1 - min1) > (max2 - min2) ? b : a;
                var bigger = (max1 - min1) > (max2 - min2) ? a : b;

                var acount = a.Count();
                var bcount = b.Count();
                var less_el = acount > bcount ? bcount : acount;

                var check = new List<bool>(small_max + 1);
                for (int i = 0; i < small_max + 1; i++)
                {
                    check.Add(false);
                }

                foreach (var i in smaller)
                {
                    check[i - small_min] = true;
                }

                res = new List<int>(less_el);
                foreach (var big in bigger)
                {
                    var bg = big - small_min;
                    if (bg >= 0 && bg <= small_max && check[bg])
                    {
                        res.Add(big);
                    }
                }
            }

            return res;
        }
    }
}

