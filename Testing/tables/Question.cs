using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Testing.tables
{
    public class Question
    {
        public int Id { get; set; }
        public string Title { get; set; }
        public byte[] Body { get; set; }
        public DateTime CreationDate { get; set; }
        public int Score { get; set; }
        public int ViewCount { get; set; }
        public int? OwnerUserId { get; set; }
        public int? LastEditorUserId { get; set; }
        public string LastEditorDisplayName { get; set; }
        public DateTime? LastEditDate { get; set; }
        public DateTime? LastActivityDate { get; set; }
        public string Tags { get; set; }
        public int AnswerCount { get; set; }
        public int CommentCount { get; set; }
        public int? FavoriteCount { get; set; }
        public DateTime? CommunityOwnedDate { get; set; }
        public int? AcceptedAnswerId { get; set; }
    }
}
