#if (SERVER || SOCKETS)
using LinqdbClient;
using ServerLogic;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Testing.Queues;
using Testing.tables;

namespace Testing.basic_tests
{
    public class QueuesPutGetLarge : ITest
    {
        public void Do(Db db)
        {
            bool dispose = false; if (db == null) { db = new Db("DATA"); dispose = true; }
#if (SERVER)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif
#if (SOCKETS)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
#endif

            db.Queue<QueueA>().GetAllFromQueue();

            int total = 30000;
            int read = 0;
            object _read_lock = new object();

            Parallel.ForEach(Enumerable.Range(0, 25).ToList(), f => {
                Task.Run(() =>
                {
                    while (read < total)
                    {
                        try
                        {
                            var res = db.Queue<QueueA>().GetAllFromQueue();
                            if (res.Any() && res.First().SomeArray.Count() != 250000)
                            {
                                throw new Exception("not match");
                            }
                            lock (_read_lock)
                            {
                                read += res.Count();
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("A " + ex.Message);
                        }
                    }
                });
            });

            Parallel.ForEach(Enumerable.Range(0, total).ToList(), new ParallelOptions() { MaxDegreeOfParallelism = 10 }, f =>
            {
                try
                {
                    var a = new QueueA()
                    {
                        SomeString = "test123",
                        SomeArray = Enumerable.Range(0, 250000).ToList()
                    };

                    db.Queue<QueueA>().PutToQueue(a);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            });


            Thread.Sleep(5000);


            if (read != total)
            {
                throw new Exception("Assert failure");
            }

#if (SERVER || SOCKETS)
            if (dispose) { Logic.Dispose(); }
#else
            if (dispose) { db.Dispose(); }
#endif
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
#endif