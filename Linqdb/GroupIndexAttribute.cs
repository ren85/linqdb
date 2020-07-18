//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;

//namespace LinqDbInternal
//{
//    /// <summary>
//    ///  Attribute used to create group-by index with a specific aggregation property.
//    /// </summary>
//    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
//    public class LinqdbGroupIndexAddInternalAttribute : Attribute
//    {
//        /// <summary>
//        ///  Property which is aggregated. 
//        /// </summary>
//        public string AggregationPropertyName { get; set; }
//        /// <summary>
//        ///  Date of index creation, used to determine whether an index should be created.
//        /// </summary>
//        public DateTime CreationDate { get; set; }
//    }

//    /// <summary>
//    ///  Attribute used to create group-by index with a specific aggregation property.
//    /// </summary>
//    public class LinqdbGroupIndexRemoveAttribute : Attribute
//    {
//        /// <summary>
//        ///  Property which is aggregated. 
//        /// </summary>
//        public string AggregationPropertyName { get; set; }
//        /// <summary>
//        ///  Date of index removal, used to determine whether an index should be removed.
//        /// </summary>
//        public DateTime RemovalDate { get; set; }
//    }
//}

//namespace LinqDb
//{
//    using LinqDbInternal;
//    /// <summary>
//    ///  Attribute used to create group-by index with a specific aggregation property.
//    /// </summary>
//    public class LinqdbGroupIndexAddAttribute : LinqdbGroupIndexAddInternalAttribute
//    { }
//}