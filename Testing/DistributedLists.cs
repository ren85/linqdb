using LinqdbClient;
using LinqDbClientInternal;
using ServerLogic;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Testing.basic_tests;
using Testing.tables;


namespace Testing
{
    public class DistributedLists
    {
        public static void Do()
        {
            var sw = new Stopwatch();
            sw.Start();

            ITestDistributed current = null;
            try
            {
                DistributedDb db = null;

                var tests = new List<ITestDistributed>()
                {
                    new DistributedSimpleSave(),
                    new DistributedBatchSave(),
                    new DistributedSimpleWhere(),
                    new DistributedLessThan(),
                    new DistributedLogicalChain(),
                    new DistributedNoId(),
                    new DistributedSelectIncorrectly(),
                    new DistributedOrderBy(),
                    new DistributedOrderByDescending(),
                    new DistributedTake(),
                    new DistributedBetween(),
                    new DistributedIntersect(),
                    new DistributedUpdate(),
                    new DistributedDelete(),
                    new DistributedMultipleWhere(),
                    new DistributedUpdateBinary(),
                    new DistributedSelectEntity(),
                    new DistributedSearch(),
                    new DistributedCount(),
                    new DistributedOrOperator(),
                    new DistributedSelectOnly(),
                    new DistributedBoolType(),
                    new DistributedParallelWorkload(),
                    new DistributedParallelWorkload2(),
                    new DistributedParallelWorkloadManySelects(),
                    new DistributedParallelWorkload3(),
                    new DistributedTotal(),

                    new DistributedStringIndexOddBatchSize(),
                    new DistributedSelectBadExpression(),
                    new DistributedOrderByEdgeCases(),
                    new DistributedSearchSlices(),

                    new DistributedBadWhere(),
                    new DistributedGetIds(),
                    new DistributedLastStep(),

                    new DistributedOr2(),
                    new DistributedParallelSave(),
                    new DistributedParallelWorkloadAll(),

                    new DistributedBasicGroup(),
                    new DistributedGroupWhere(),

                    new DistributedGenericType()

                };

                foreach (var t in tests)
                {
                    current = t;
                    Console.WriteLine(current.GetName());
                    t.Do(db);
                }

               
                var times = new List<int>();
                for (int i = 0; i < 3; i++)
                {
                    Console.Clear();
                    Console.WriteLine("SHUFFLE: " + i + "\n");
                    var sw1 = new Stopwatch();
                    sw1.Start();
                    foreach (var t in tests.OrderBy(f => Guid.NewGuid()).Where(f => !f.GetName().ToLower().Contains("group")).ToList())
                    {
                        current = t;
                        Console.WriteLine(current.GetName());
                        t.Do(db);
                    }
                    sw1.Stop();
                    times.Add((int)(sw1.ElapsedMilliseconds / 1000));
                }

                if (db != null)
                {
                    db.Dispose();
                }


                Console.WriteLine("THE (HAPPY) END");
                sw.Stop();
                Console.WriteLine("Elapsed: {0} sec", sw.ElapsedMilliseconds / 1000);
                foreach (var t in times)
                {
                    Console.WriteLine("{0} sec", t);
                }
                Console.ReadLine();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Oops (" + current.GetName() + "): " + ex.Message);
                if (ex is LinqDbException && (ex as LinqDbException).errors != null && (ex as LinqDbException).errors.Any())
                {
                    foreach (var error in (ex as LinqDbException).errors)
                    {
                        Console.WriteLine($"\t{error.Key} - {error.Value?.FirstOrDefault()?.Message}");
                    }
                }
                Console.ReadLine();
            }
        }
    }
}
