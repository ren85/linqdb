using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqdbClient
{
    public class LinqdbServerStatus
    {
        /// <summary>
        ///  Returns 'servername' parameter's value from config file.
        /// </summary>
        public string ServerName { get; set; }
        /// <summary>
        ///  True if server replied in less than 1 second, false otherwise.
        /// </summary>
        public bool IsUp { get; set; }
    }
}
