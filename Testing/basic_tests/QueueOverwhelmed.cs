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
    public class QueueOverwhelmed : ITest
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
            int total = 500;
            int bad = 0;
            int others = 0;
            int good = 0;
            int started = 0;
            object _lock = new object();

            db.Queue<QueueA>().GetAllFromQueue();

            Parallel.ForEach(Enumerable.Range(0, total).ToList(), new ParallelOptions() { MaxDegreeOfParallelism = 40 }, f =>
            {
                try
                {
                    lock (_lock)
                    {
                        started++;
                    }
                    var a = new QueueA()
                    {
                        SomeString = "test123",
                        SomeArray = new List<int>() { 1, 2, 3 }
                    };

                    db.Queue<QueueA>().PutToQueue(a);
                    lock (_lock)
                    {
                        good++;
                    }
                }
                catch (Exception ex)
                {
                    if (ex.Message.Contains("more readers"))
                    {
                        lock (_lock)
                        {
                            bad++;
                        }
                    }
                    else
                    {
                        lock (_lock)
                        {
                            others++;
                        }
                    }
                }
            });


            if (bad != 201 || others != 0 || started != total || good != 299)
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