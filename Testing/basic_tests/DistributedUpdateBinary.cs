using LinqdbClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testing.distributedtables;
using Testing.tables;

namespace Testing.basic_tests
{
    public class DistributedUpdateBinary : ITestDistributed
    {
        public void Do(DistributedDb db)
        {
            bool dispose = false;
            if (db == null)
            {
                db = SocketTestingDistributed.GetTestDb();
                dispose = true;
            }

            var sids = db.DistributedTable<BinaryDataDistributed>().Select(f => new { f.Sid, f.Id }).Select(f => new DistributedId(f.Sid, f.Id)).ToList();
            db.DistributedTable<BinaryDataDistributed>().Delete(sids);

            var d = new BinaryDataDistributed()
            {
                Id = 2,
                Sid = 2,
                Data = new List<byte>() { 4, 5, 6 }.ToArray()
            };
            db.DistributedTable<BinaryDataDistributed>().Save(d);
            var res = db.DistributedTable<BinaryDataDistributed>()
                        .Select(f => new
                        {
                            f.Id,
                            f.Sid,
                            MyData = f.Data
                        });
            if (res.Count() != 1 || res[0].MyData.Length != 3 || res[0].MyData[0] != (byte)4)
            {
                throw new Exception("Assert failure");
            }

            db.DistributedTable<BinaryDataDistributed>().Update(f => f.Data, new Dictionary<DistributedId, byte[]>() { { new DistributedId(2,2), (new List<byte>() { 1, 2, 3 }).ToArray() } });

            res = db.DistributedTable<BinaryDataDistributed>()
                        .Select(f => new
                        {
                            f.Id,
                            f.Sid,
                            MyData = f.Data
                        });
            if (res.Count() != 1 || res[0].MyData.Length != 3 || res[0].MyData[0] != (byte)1)
            {
                throw new Exception("Assert failure");
            }

            if (dispose)
            {
                if (dispose) { db.Dispose(); }
            }
        }

        public string GetName()
        {
            return this.GetType().Name;
        }
    }
}
