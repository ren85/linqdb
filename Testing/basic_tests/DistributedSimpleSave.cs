
using LinqdbClient;
using LinqDbClientInternal;
using ServerLogic;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.distributedtables;

namespace Testing.basic_tests
{

    public class SocketTestingDistributed
    {
        public static byte[] CallServer(byte[] input, Db db, string ip, int port)
        {
            if (db._db_internal.Sock == null)
            {
                db._db_internal.Sock = new ClientSockets();
            }
            string Hostname = ip;
            int Port = port;
            string error = null;
            var res = db._db_internal.Sock.CallServer(input, Hostname, Port, out error);
            if (res.Count() == 4 && BitConverter.ToInt32(res, 0) == -1)
            {
                throw new LinqDbException("Linqdb: socket error: " + error);
            }
            return res;
        }
        public static DistributedDb GetTestDb()
        {
            var db1 = new Db("127.0.0.1:2055");
            db1._db_internal.CallServer = (byte[] f) => { return CallServer(f, db1, "127.0.0.1", 2055); };
            var db2 = new Db("127.0.0.1:2056");
            db2._db_internal.CallServer = (byte[] f) => { return CallServer(f, db2, "127.0.0.1", 2056); };
            var db3 = new Db("127.0.0.1:2057");
            db3._db_internal.CallServer = (byte[] f) => { return CallServer(f, db3, "127.0.0.1", 2057); };
            var db4 = new Db("127.0.0.1:2058");
            db4._db_internal.CallServer = (byte[] f) => { return CallServer(f, db4, "127.0.0.1", 2058); };
            var db5 = new Db("127.0.0.1:2059");
            db5._db_internal.CallServer = (byte[] f) => { return CallServer(f, db5, "127.0.0.1", 2059); };
            var db6 = new Db("127.0.0.1:2060");
            db6._db_internal.CallServer = (byte[] f) => { return CallServer(f, db6, "127.0.0.1", 2060); };

            var db = new DistributedDb(new Dictionary<int, Db>()
                {
                    { 1, db1 },
                    { 2, db2 },
                    { 3, db3 },
                    { 4, db4 },
                    { 5, db5 },
                    { 6, db6 },
                });

            return db;
        }
    }

    public class DistributedSimpleSave : ITestDistributed
    {
        public void Do(DistributedDb db)
        {
            bool dispose = false;
            if (db == null)
            {
                db = SocketTestingDistributed.GetTestDb();
                dispose = true;
            }

            var sids = db.DistributedTable<SomeDataDistributed>().Select(f => new { f.Sid, f.Id }).Select(f => new DistributedId(f.Sid, f.Id)).ToList();
            //sids = sids.Select(f => new DistributedId(1, f.Id)).ToList();
            db.DistributedTable<SomeDataDistributed>().Delete(sids);

            var d = new SomeDataDistributed()
            {
                Id = 1,
                Sid = 1,
                Normalized = 1.2,
                PeriodId = 5
            };
            db.DistributedTable<SomeDataDistributed>().Save(d);

            var res = db.DistributedTable<SomeDataDistributed>().Select(f => new
            {
                PeriodId = f.PeriodId
            });
            if (res[0].PeriodId != 5)
            {
                throw new Exception("Assert failure");
            }
            if (dispose)
            {
                if (dispose) { Logic.Dispose(); }
            }
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
