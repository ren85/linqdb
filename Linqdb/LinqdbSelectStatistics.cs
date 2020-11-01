using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDb
{
    public class LinqdbSelectStatistics
    {
        /// <summary>
        ///  Total number of records satisfying criterias.
        /// </summary>
        public int? Total { get; set; }
        /// <summary>
        ///  In case when time limited search is used shows what percentage of data was searched.
        /// </summary>
        public double? SearchedPercentile { get; set; }
    }
}

namespace LinqDbInternal
{
    public class LinqdbSelectStatisticsInternal
    {
        public int? Total { get; set; }
        public double? SearchedPercentile { get; set; }
    }
}
