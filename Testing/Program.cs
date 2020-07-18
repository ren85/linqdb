//to test server go to Properties -> Build -> Conditional compilation symbols -> type SERVER
#if (SERVER || SOCKETS)
using LinqdbClient;
using ServerLogic;
#else
using LinqDb;
#endif
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


/*For complete testing tests with these symbols must be run:
(empty)
SAMEDB
SAMEDB;INDEXES
SERVER
SERVER;INDEXES
SOCKETS (for this server must be running at localhost:2055)
SOCKETS;INDEXES
DATA
DATA;INDEXES
DATA;SERVER
DATA;SERVER;INDEXES
 */

namespace Testing
{
    class Program
    {
        static void Main(string[] args)
        {
            var sw = new Stopwatch();
            sw.Start();

            ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA");

            ITest current = null;
            try
            {
                Db db = null;
#if (SAMEDB || SERVER || INDEXES)
                db = new Db("DATA");
#endif
                var tests = new List<ITest>()
                {
                    #if ((SAMEDB || SERVER || SOCKETS) && INDEXES)
                        new IndexesPrepare(),
                    #endif
                    #if (!SAMEDB || !INDEXES)
                        new OpenClose(),
                    #endif                     
                    new SimpleSave(),
                    new BatchSave(),
                    new SimpleWhere(),
                    new LessThan(),
                    new LessThanOrEqual(),
                    new GreaterThan(),
                    new GreaterThanOrEqual(),
                    new Equal(),
                    new NotEqual(),
                    new NullEqual(),
                    new NullNotEqual(),
                    new LessThanNegative(),
                    new LessThanOrEqualNegative(),
                    new GreaterThanNegative(),
                    new GreaterThanOrEqualNegative(),
                    new EqualNegative(),
                    new NotEqualNegative(),
                    new LessThanDate(),
                    new And(),
                    new Or(),
                    new LogicalChain(),
                    new NoId(),
                    new SelectIncorrectly(),
                    new OrderBy(),
                    new OrderByDescending(),
                    new OrderByNegative(),
                    new OrderByDescendingNegative(),
                    new SkipTake(),
                    new SkipTakeNegative(),
                    new OrderByMixed(),
                    new OrderByDescendingMixed(),
                    new SkipTakeMixed(),
                    new Between(),
                    new BetweenNegative(),
                    new Intersect(),
                    new IntersectNegative(),
                    new IntersectDates(),
                    new Update(),
                    new UpdateDouble(),
                    new UpdateNegative(),
                    new Delete(),
                    new MultipleWhere(),
                    new OrderMultipleOps(),
                    new MultipleBetween(),
                    new MultipleIntersect(),
                    new Binary(),
                    new UpdateBinary(),
                    new Id(),
                    new BinaryNull(),
                    new BinaryUpdateNull(),
                    new UpdateIntNull(),
                    new BinaryMultipleOps(),
                    new StringMultipleOps(),
                    new DoubleMultipleOps(),
                    new SelectEntity(),
                    new StringTest(),
                    new Search(),
                    new SearchAfterUpdate(),
                    new SearchAfterUpdate2(),
                    new SearchAfterDelete(),
                    new StringIndex(),
                    new OrderByString(),
                    new Count(),
                    new CountMultipleOps(),
                    new TwoTables(),
                    new OrOperator(),
                    new SelectOnly(),
                    new BoolType(),
                    new WhereTypes(),
                    new WhereExpression(),
                    new Betweens(),
                    new UpdateStringIndex(),
                    new UpdateBinaryIndex(),
                    new UpdateDateTimeIndex(),
                    new UpdateDoubleIndex(),
                    new UpdateStringIndexNull(),
                    #if (!SOCKETS && !SERVER && !SAMEDB && !INDEXES)
                        new LowLevelDelete(),
                    #endif
                    new ParallelWorkload(),
                    new ParallelWorkload2(),
                    new ParallelWorkloadManySelects(),
                    new ParallelWorkload3(),
                    new Total(),
                    new StringIndexOddBatchSize(),
                    new SelectBadExpression(),
                    new OrderByEdgeCases(),
                    new SearchSlices(),
                    new BadWhere(),
                    new Transaction(),
                    new TransactionsParallel(),
                    new GetIds(),
                    new SelectTwice(),
                    new DeleteAsFirst(),
                    new LastStep(),
                    new AtomicIncrement(),
                    new AtomicIncrementTransaction(),
                    new TransactionString(),
                    new TransactionIds(),
                    new ManyWhere(),
                    new GetTables(),
                    new GetTableDefinition(),
                    #if (SERVER && !INDEXES)
                        new Config(),
                    #endif 
                    #if (!SOCKETS) //server/client architecture may differ
                        new SelectTooMuch(),
                    #endif 
                    new EmptyString(),
                    new BadDouble(),
                    new BadWhere2(),
                    new SelectAllAnonymous(),
                    new Or2(),
                    new ParallelSave(),
                    new ParallelBatch(),
                    new ParallelWorkloadAll(),
                    new TransactionWriteDelete(),
                    new TransactionCount(),
                    new TransactionEditSameIndex(),
                    new TransactionWriteEdit(),
                    new TransactionCount2(),
                    new TransactionWriteDelete2(),
                    new TransactionEditSameIndex2(),
                    new TransactionEditingSameEntities(),
                    new TransactionEditingSameField(),
                    new BasicGroup(),
                    new GroupWhere(),
                    new DistinctCountGroup(),
                    #if (!SAMEDB && !SOCKETS && !SERVER) 
                        new IndexRemovedField(),
                    #endif
                    new BadColumnIndexes(),
                    new GroupAggregation(),
                    new GroupByWrongColumn(),
                    #if (SERVER && !INDEXES)
                        new IndexOnStart(),
                        new GetIndexes(),
                    #endif
                    new SelectStringOnly(),
                    new IntersectString(),
                    #if (SERVER && !INDEXES)
                        new IndexRemove(),
                    #endif
                    new ParallelSaveSameIds(),
                    new NewIdTooBig(),
                    new AtomicIncrementOverflow(),
                    new ParallelGroupBy(),
                    new TransParallelWorkload(),
                    new TransParallelWorkload2(),
                    new TransParallelWorkloadManySelect(),
                    new TransParallelWorkload3(),
                    new TransactionEmpty(),
                    new EmptyModify(),
                    new UpdateDoublesWithInts(),
                    new ErrorInABatch(),
                    new BadWhere3(),
                    new GenericType(),
                    new BatchTooBig(),
                    new SearchPartial(),
                    new NotSearchableProperty(),
                    new CaseInsensitive(),
                    new MaxBinaryAndString(),
                    new NonAtomicModifications(),
                    new SelectNonAtomically()
                };

                var tests2 = new List<ITest>()
                {
#if (!SOCKETS)
                    new IdAssignment(),
                    new ManyIdAssignments(),
                    new ChangeType(),
                    new TypeChanges(),
                    new NewField(),
                    new ReplicateTest(),
                    new TypeChangesDelete(),
                    new ChangeType2(),
                    new NewColumn2(),
                    new DeleteAfterNewColumn(),
                    new DeletedColumnThenAdded(),
                    new UpdateWithZero(),
                    new ChangeType3(),
                    new ChangeType4(),
#endif
                };

                //while(true)
                foreach (var t in tests)
                {
                    current = t;
                    Console.WriteLine(current.GetName());
                    t.Do(db);
                }

                foreach (var t in tests2)
                {
                    current = t;
                    Console.WriteLine(current.GetName());
                    t.Do(db);
                }

#if (!SOCKETS && !SERVER)
                if (db != null)
                {
                    db.Dispose();
                    db = new Db("DATA");
                }
#endif

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
                Console.ReadLine();
            }
        }
    }
}
