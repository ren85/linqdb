#if (SERVER || SOCKETS)
using LinqdbClient;
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
    class GenericType2 : ITest
    {
        public void Do(Db db)
        {
            bool dispose = false; if (db == null) { db = new Db("DATA"); dispose = true; }
#if (SERVER)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f); };
#endif
#if (SOCKETS)
            db._db_internal.CallServer = (byte[] f) => { return SocketTesting.CallServer(f, db); };
#endif
#if (SOCKETS || SAMEDB || INDEXES || SERVER)
            db.Table<TableWithDate>().Delete(new HashSet<int>(db.Table<TableWithDate>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
            db.Table<TableWithString>().Delete(new HashSet<int>(db.Table<TableWithString>().Select(f => new { f.Id }).Select(f => f.Id).ToList()));
#endif


            SaveData(
                new TableWithDate()
                {
                    CommonIntValue = 1,
                    CommonStringValue = "a",
                    Date = Convert.ToDateTime("1970-01-01")
                }, db);
            SaveData(
                new TableWithString()
                {
                    CommonIntValue = 2,
                    CommonStringValue = "b",
                    NameSearch = "c"
                }, db);

            var r1 = GetData<TableWithDate>("a", db);
            if (r1.Count() != 1 || (r1.First() as TableWithDate).Date != Convert.ToDateTime("1970-01-01"))
            {
                throw new Exception("Assert failure");
            }
            var r2 = GetData<TableWithString>("b", db);
            if (r2.Count() != 1 || (r2.First() as TableWithString).NameSearch != "c")
            {
                throw new Exception("Assert failure");
            }



#if (SERVER || SOCKETS)
            if (dispose) { Logic.Dispose(); }
#else
            if (dispose) { db.Dispose(); }
#endif
#if (!SOCKETS && !SAMEDB && !INDEXES && !SERVER)
            if(dispose) { ServerSharedData.SharedUtils.DeleteFilesAndFoldersRecursively("DATA"); }
#endif
        }

        public string GetName()
        {
            return this.GetType().Name;
        }

        static void SaveData<T>(T data, Db db) where T : ITable, new()
        {
            db.Table<T>().Save(data);
        }
        static List<T> GetData<T>(string value, Db db) where T : ITable, new()
        {
            return db.Table<T>().Where(f => f.CommonStringValue == value).SelectEntity();
        }
    }
}
