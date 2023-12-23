using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Testing.distributedtables
{
    public class UsersItemDistributed
    {
        public int Id { get; set; }
        public int Sid { get; set; }
        public int Gid { get; set; }
        public string ID { get; set; }
        public int UserId { get; set; }
        public string TitleSearch { get; set; }
        public string CodeSearch { get; set; }
        public string LangSearch { get; set; }
        public string GuidSearch { get; set; }
        public string RegexSearch { get; set; }
        public string ReplaceSearch { get; set; }
        public string TextSearch { get; set; }
        public DateTime Date { get; set; }
        public int? IsLive { get; set; }
    }
}
