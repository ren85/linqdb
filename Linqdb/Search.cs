using RocksDbSharp;
using ServerSharedData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbInternal
{
    public partial class Ldb
    {
        public IDbQueryable<T> Search<T, TKey>(IDbQueryable<T> source, Expression<Func<T, TKey>> keySelector, string search_query, bool partial, int? start_step, int? steps)
        {
            if (start_step != null && steps == null || start_step == null && steps != null)
            {
                throw new LinqDbException("Linqdb: start_step and steps parameters must be used together.");
            }

            CheckTableInfo<T>();
            if (string.IsNullOrEmpty(search_query))
            {
                return source;
            }
            if (source.LDBTree == null)
            {
                source.LDBTree = new QueryTree();
            }
            var tree = source.LDBTree;
            if (tree.SearchInfo == null)
            {
                tree.SearchInfo = new List<SearchInfo>();
            }
            var info = new SearchInfo();
            tree.SearchInfo.Add(info);
            info.SearchQuery = search_query;
            info.Partial = partial;
            info.Start_step = start_step;
            info.Steps = steps;

            var par = keySelector.Parameters.First();
            var name = SharedUtils.GetPropertyName(keySelector.Body.ToString());
            var table_info = GetTableInfo(typeof(T).Name);
            info.TableInfo = table_info;
            info.Name = name;

            source.LDBTree.Prev = info;
            source.LDBTree.Prev.Id = source.LDBTree.Counter + 1;
            source.LDBTree.Counter++;

            return source;
        }

        public List<OperResult> Search(QueryTree tree, List<OperResult> oper_list, ReadOptions ro)
        {
            if (tree.SearchInfo == null)
            {
                return oper_list;
            }
            
            foreach (var s in tree.SearchInfo)
            {
                var res = SearchOne(s, ro, s.Partial, s.Steps, s.Start_step);
                var oper_res = new OperResult()
                {
                    All = false,
                    ResIds = res
                };
                oper_res.Id = s.Id;
                oper_res.OrWith = s.OrWith;
                oper_list.Add(oper_res);
            }

            return oper_list;
        }

        public List<int> SearchOne(SearchInfo info, ReadOptions ro, bool partial, int? steps = null, int? start_step = null)
        {
            if (!info.Name.ToLower().EndsWith("search"))
            {
                throw new LinqDbException("Linqdb: only string properties named ...Search are indexed and can be searched.");
            }
            return MakeSearch(info.SearchQuery, info.TableInfo, info.Name, ro, partial, steps, start_step);
        }
    }


    public class SearchInfo : BaseInfo
    {
        public string SearchQuery { get; set; }
        public bool Partial { get; set; }
        public TableInfo TableInfo { get; set; }
        public string Name { get; set; }
        public int? Start_step { get; set; }
        public int? Steps { get; set; }
    }
}
