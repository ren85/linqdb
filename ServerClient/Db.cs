using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqDbClientInternal
{
    public class LinqDbException : Exception
    {
        public LinqDbException() : base()
        {
        }

        public LinqDbException(string message) : base(message)
        {
        }
    }
}
