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
        public Dictionary<string, List<Exception>> errors { get; set; }
        public LinqDbException() : base()
        {
        }

        public LinqDbException(string message, Dictionary<string, List<Exception>> errors = null) : 
            base(message + " " + (errors != null && errors.Any() ? errors.First().Key +": "+ errors.First().Value.FirstOrDefault()?.Message : ""))
        {
            this.errors = errors;
        }
    }
}
