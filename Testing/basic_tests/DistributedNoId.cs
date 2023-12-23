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
    public class DistributedNoId : ITestDistributed
    {
        public void Do(DistributedDb db)
        {
            bool dispose = false;
            if (db == null)
            {
                db = SocketTestingDistributed.GetTestDb();
                dispose = true;
            }

            var d = new NoIdType()
            {
                A = 5
            };

            string ex_msg = "";
            try
            {
                db.DistributedTable<NoIdType>().Save(d);
            }
            catch (Exception ex)
            {
                ex_msg = ex.Message;
            }

            if (!ex_msg.Contains("Linqdb: type must have integer Id property"))
            {
                throw new Exception("Assert failure");
            }

            var d2 = new NoSidType()
            {
                A = 5
            };

            ex_msg = "";
            try
            {
                db.DistributedTable<NoSidType>().Save(d2);
            }
            catch (Exception ex)
            {
                ex_msg = ex.Message;
            }

            if (!ex_msg.Contains("Linqdb: type must have integer Sid property"))
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

