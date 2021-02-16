#if (SERVER || SOCKETS)
using LinqdbClient;
using LinqDbClientInternal;
using ServerLogic;
#else
using LinqDb;
#endif

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
#if (SOCKETS)
    public class SocketTesting
    {
        public static byte[] CallServer(byte[] input, Db db)
        {
            if (db._db_internal.Sock == null)
            {
                db._db_internal.Sock = new ClientSockets();
            }
            string Hostname = "127.0.0.1";
            int Port = 2055;
            string error = null;
            var res = db._db_internal.Sock.CallServer(input, Hostname, Port, out error);
            if (res.Count() == 4 && BitConverter.ToInt32(res, 0) == -1)
            {
                throw new LinqDbException("Linqdb: socket error: " + error);
            }
            return res;
        }
    }
#endif
#if (SERVER)
    public class SocketTesting
    {
        static bool first = true;
        static object _lock = new object();
        public static byte[] CallServer(byte[] input)
        {
            if (first)
            {
                lock (_lock)
                {
                    if (first)
                    {
                        first = false;
                        System.IO.File.WriteAllText(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "config.txt"),
                        @"
user.admin.pass=admin
user.admin.role=admin

user.reader_user.pass=re@der123
user.reader_user.role=reader

user.writer_user.pass=wr1ter123
user.writer_user.role=writer

user.admin_user.pass=@admin123
user.admin_user.role=admin

servername=5.5.5.5:2055

");
                        string str;
                        int por;
                        ServerSharedData.CommandHelper.ReadConfig(out str, out por);
                        Logic.ServerBuildIndexesOnStart();
                    }
                }
            }
            return Logic.Execute(input.Skip(4).ToArray()).Skip(4).ToArray();
        }
        public static void TestDispose()
        {
            first = true;
        }
    }
#endif

    public class SimpleSave : ITest
    {
        public void Do(Db db)
        {
            bool dispose = false;
            if (db == null)
            {
                db = new Db("DATA");
                dispose = true;
            }
#if (SERVER)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif
#if (SOCKETS)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
#endif
#if (SOCKETS || SAMEDB || INDEXES || SERVER)
            db.Table<SomeData>().Delete(new HashSet<int>(db.Table<SomeData>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif

            var d = new SomeData()
            {
                Id = 1,
                Normalized = 1.2,
                PeriodId = 5
            };
            db.Table<SomeData>().Save(d);

            var res = db.Table<SomeData>().Select(f => new
            {
                PeriodId = f.PeriodId
            });
            if (res[0].PeriodId != 5)
            {
                throw new Exception("Assert failure");
            }
#if (SERVER || SOCKETS)
            if(dispose)
            {
                if(dispose) { Logic.Dispose(); }
            }
#else
            if (dispose)
            {
                if (dispose) { db.Dispose(); }
            }
#endif


#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
            if(dispose)
            {
                if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
            }
#endif

        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
