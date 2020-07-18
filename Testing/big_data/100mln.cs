//using LinqDb;
//using System;
//using System.Collections.Generic;
//using System.IO;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;
//using Testing.tables;

//namespace Testing.big_data
//{
//    //public class LogEntry
//    //{
//    //    public int Id { get; set; }
//    //    public string Data { get; set; }
//    //    public string Result { get; set; }
//    //    public string Input { get; set; }
//    //    public string Compiler_args { get; set; }
//    //    public int Lang { get; set; }
//    //    public int Is_api { get; set; }
//    //    public DateTime Time { get; set; }

//    //}
//    public class _100mln : ITest
//    {
//        public void Do(Db db)
//        {

//            //Db.Init(@"C:\Data");
//            //var res = db.Table<LogEntry>()
//            //    //.Between(f => f.Time, Convert.ToDateTime("2016-07-31 19:41"), Convert.ToDateTime("2016-07-31 19:46"))
//            //    //.Where(f => f.Lang == 4)
//            //    //.Search<LogEntry, string>(f => f.Data, "os.setsid()")
//            //    //.Where(f => f.Lang != 1

//            //            .Where(f => f.Is_api == 1 && f.Lang == 4)
//            //            .OrderByDescending(f => f.Data)
//            //            .Take(10)
//            //            .SelectEntity();
//            //Console.ReadLine();





//            string path = @"C:\Data";
//            DateTime qstart = DateTime.Now;
//            if (!Directory.Exists(path))
//            {
//                db = new Db(path);
//                DateTime start = DateTime.Now;
//                var list = new List<SomeData>();
//                var rg = new Random();
//                for (int i = 0; i < 100000000; i++)
//                {
//                    var d = new SomeData()
//                    {
//                        Id = i,
//                        Normalized = rg.NextDouble(),
//                        Value = rg.Next(1, 1000),
//                        ObjectId = rg.Next(1, Int32.MaxValue),
//                        PeriodId = rg.Next(1, Int32.MaxValue),
//                        PersonId = rg.Next(1, Int32.MaxValue),
//                        Name = RandomString(20, i) + " " + RandomString(10, i)
//                    };

//                    //var d = new Answer()
//                    //{
//                    //    Id = i,
//                    //    SomeDouble = i,
//                    //    SomeId = i,
//                    //    SomeBin = BitConverter.GetBytes(i).ToArray(),
//                    //    Anwser = i.ToString() + ": " + RandomString(10) + " " + RandomString(10) + " " + RandomString(10) + " " + RandomString(10) + " " + RandomString(10) + " " + RandomString(10) + " " + RandomString(10) + " " + RandomString(10) + " " + RandomString(10) + " " + RandomString(10),
//                    //    Title = i.ToString() + ": " + RandomString(50)
//                    //};
//                    list.Add(d);
//                    if (i > 0 && i % 100000 == 0)
//                    {
//                        db.Table<SomeData>().SaveBatch(list);
//                        list = new List<SomeData>();
//                        Console.WriteLine("After {0} sec {1}", Math.Round((DateTime.Now - start).TotalSeconds), i);
//                    }
//                }
//                db.Table<SomeData>().SaveBatch(list);
//                DateTime end = DateTime.Now;
//                var loading_time_sec = (end - start).TotalSeconds;
//            }
//            else
//            {
//                db = new Db(path);
//            }
//            Console.WriteLine("Creation time in sec: {0}", (DateTime.Now - qstart).TotalSeconds);
//            Console.ReadLine();
//            qstart = DateTime.Now;

//            //int? person_id = 5;
//            //var tmp = db.Table<SomeData>()
//            //            .Where(f => f.Normalized == 5);
//            //if (person_id != null)
//            //{
//            //    tmp.Where(f => f.PersonId == person_id); //no need to assign to tmp
//            //}

//            //var res = tmp.SelectEntity(); //goes to disk

//            //int max_period = db.Table<SomeData>()
//            //                   .OrderByDescending(f => f.PeriodId)
//            //                   .Take(1)
//            //                   .Select(f => new
//            //                   {
//            //                       PeriodId = f.PeriodId
//            //                   })
//            //                   .First()
//            //                   .PeriodId;
//            //var res_list = new List<KeyValuePair<int, int>>();
//            //int last_period = 0;
//            //while (last_period <= max_period)
//            //{
//            //    var res = db.Table<SomeData>()
//            //                .Between(f => f.PeriodId, last_period, last_period + 1000000, BetweenBoundaries.FromInclusiveToExclusive)
//            //                .Select(f => new
//            //                {
//            //                    Id = f.Id,
//            //                    Period = f.PeriodId
//            //                })
//            //                .Where(f => f.Period % 2 == 0)
//            //                .ToList();
//            //    res_list.AddRange(res.Select(f => new KeyValuePair<int, int>(f.Id, f.Period)));
//            //    last_period += 1000000;
//            //}

//            //var dic = new Dictionary<int, string>();
//            //for (int i = 2000000; i < 4000000; i++)
//            //{
//            //    dic[i] = "ąėęįšųėįęųčė čėęįšė ųįūšėūįšįė";
//            //}
//            //db.Table<SomeData>().Update(f => f.Name, dic);

//            var res = db.Table<SomeData>()
//                //.Where(f => f.Id > 999999)
//                .Between(f => f.Normalized, 0.33, 0.34)
//                //.Where(f => f.Normalized > 0.99 /*&& f.Normalized > 0.899*/ /*f.Value == 42 &&*/)
//                //.Where(f => f.PeriodId % 2 == 0)
//                //.OrderBy(f => f.Normalized)
//                //.Skip(10000000)
//                //.OrderBy(f => f.Id)
//                //.Take(10)
//                //.Where(f => f.Name == null)
//                //.Search(f => f.Name, "čėęįšė ųįūšėūįšįė")
//                //.OrderBy(f => f.Id)
//                //.Take(10)
//                //.SelectEntity();
//                //.Search(f => f.Name, "čėęįšė ųįūšėūįšįė")
//                //.Where(f => f.Name == null)
//                // .SelectEntity();
//            .Select(f => new
//            {
//                //Id = f.Id,
//                Normalized = f.Normalized,
//                //Value = f.Value,
//                //ObjectId = f.ObjectId,
//                //PeriodId = f.PeriodId,
//                //PersonId = f.PersonId
//                //Name = f.Name
//            });


//            //var res = db.Table<Answer>()
//            //    .OrderBy(f => f.Anwser)
//            //    .Take(1000)
//            //    //.Between(f => f.SomeDouble, 10000, 20000)
//            //    //.Where(f => f.SomeDouble == 1000)
//            //    //.Search(f => f.Title, "1000: JV11N5GCJ2VDHNNTW4EAYHMX4SH8C1YLEDXHO4A0LLQFRMTJ2K")
//            //    .SelectEntity();

//            //var count = db.Table<SomeData>().Count();

//            //var res_list = new List<SomeData>();
//            //int start_period = 1932600;
//            //while (res_list.Count() < 10)
//            //{
//            //    var res = db.Table<SomeData>()
//            //        //.Between(f => f.PeriodId, start_period, start_period)
//            //                .Where(f => f.PeriodId == start_period)
//            //                .Select(f => new
//            //                {
//            //                    //Id = f.Id,
//            //                    Normalized = f.Normalized,
//            //                    //Value = f.Value,
//            //                    //ObjectId = f.ObjectId,
//            //                    PeriodId = f.PeriodId,
//            //                    //PersonId = f.PersonId
//            //                })
//            //                .Select(f => new SomeData()
//            //                {
//            //                    Normalized = f.Normalized,
//            //                    PeriodId = f.PeriodId
//            //                });

//            //    start_period--;
//            //    res_list.AddRange(res);
//            //}



//            //Console.WriteLine(db.Table<SomeData>().Count());
//            //res = res.OrderByDescending(f => f.Normalized).ToList();

//            //var list_ob = new HashSet<int?>();
//            //for (int i = 0; i < 100000; i++)
//            //{
//            //    list_ob.Add(i);
//            //}
//            //var res = db.Table<SomeData>()
//            //            .Intersect(f => f.ObjectId, list_ob)
//            //            .Select(f => new
//            //            {
//            //                ObjectId = f.ObjectId
//            //            });

//            Console.WriteLine("Count:  " + res.Count);
//            Console.WriteLine("res[0]: " + res[0].Normalized);
//            var query_time_sec = (DateTime.Now - qstart).TotalSeconds;

//            var a = query_time_sec;
//        }

//        public string GetName()
//        {
//            return "100mln";
//        }

//        public static string RandomString(int length, int seed)
//        {
//            var rg = new Random(/*seed*/);
//            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
//            return new string(Enumerable.Repeat(chars, length)
//              .Select(s => s[rg.Next(s.Length)]).ToArray());
//        }

//    }
//}
