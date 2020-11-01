#if (SERVER)
using LinqdbClient;
using ServerLogic;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.tables;

namespace Testing.basic_tests
{
    public class ServerStatus : ITest
    {
        public void Do(Db db_)
        {
            bool dispose = true;

            var db = new Db("DATA2", "reader_user", "re@der123");
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
            var status = db.GetServerStatus(5000);
            if (status.ServerName != "5.5.5.5:2055" || !status.IsUp)
            {
                throw new Exception("Assert failure");
            }

            if (dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA2"); }
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
#endif