#if (SERVER || SOCKETS)
using LinqdbClient;
using ServerLogic;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.Queues;
using Testing.tables;

namespace Testing.basic_tests
{
    public class SimpleQueue : ITest
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

            var a = new QueueA()
            {
                SomeString = "test123",
                SomeArray = new List<int>() { 1,2,3}
            };

            db.Queue<QueueA>().PutToQueue(a);
            var res = db.Queue<QueueA>().GetAllFromQueue();

            if (res.Count() != 1 || res.Single().SomeString != "test123" || res.Single().SomeArray.Count() != 3 || res.Single().SomeArray[0] != 1)
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