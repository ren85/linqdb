using LinqdbClient;
using ServerLogic;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Testing
{
    interface ITestDistributed
    {
        void Do(DistributedDb db);
        string GetName();
    }
}
