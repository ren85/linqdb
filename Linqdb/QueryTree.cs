using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public class QueryTree
    {
        public BaseInfo Prev { get; set; }
        public int Counter { get; set; }
        public List<WhereInfo> WhereInfo { get; set; }
        public OrderingInfo OrderingInfo { get; set; }
        public List<BetweenInfo> BetweenInfo { get; set; }
        public List<IntersectInfo> IntersectInfo { get; set; }
        public Dictionary<long, byte[]> QueryCache = new Dictionary<long, byte[]>();
        public List<SearchInfo> SearchInfo { get; set; }
        public GroupByInfo GroupingInfo { get; set; }
    }
}
