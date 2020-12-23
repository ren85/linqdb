using LinqDb;
using ServerSharedData;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace ServerLogic
{
    public class Logic
    {
        public static Db db { get; set; }
        public static object _db_lock = new object();
        public static void Init(string db_path)
        {
            if (db == null)
            {
                lock (_db_lock)
                {
                    if (db == null)
                    {
                        if (string.IsNullOrEmpty(db_path))
                        {
                            db_path = "DATA";
                        }
                        
                        db = new Db(db_path, false);
                        
                    }
                }
            }
        }
        public static void Dispose()
        {
            lock (_db_lock)
            {
                if (db != null)
                {
                    db.Dispose();
                    db = null;
                }
            }
        }
        public static void ServerBuildIndexesOnStart(string db_path = null)
        {
            try
            {
                Init(db_path);
                db._InternalBuildIndexesOnStart();
            }
            catch (Exception ex)
            {
                try
                {
                    File.WriteAllText("ServerStartupError.txt", "While building indexes: " + DateTime.Now + Environment.NewLine + ex.Message + Environment.NewLine + ex.StackTrace);
                }
                catch (Exception) { }
            }
        }
        public static byte[] Execute(byte[] input, string db_path = null)
        {
            try
            {
                Init(db_path);
                var result = db._Internal_server_execute(input);
                return ServerResultHelper.GetBytes(result);
            }
            catch (Exception ex)
            {
                //try
                //{
                //    if (!ex.Message.Contains("Linqdb:"))
                //    {
                //        var rg = new Random();
                //        File.WriteAllText("error_" + rg.Next() + ".txt", ex.Message + " " + ex.StackTrace + (ex.InnerException != null ? (" " + ex.InnerException.Message + " " + ex.InnerException.StackTrace) : ""));
                //    }
                //}
                //catch (Exception) { }
                var cm = new ServerResult()
                {
                    ServerError = ex.Message + ex.StackTrace
                };
                return ServerResultHelper.GetBytes(cm);
            }
        }
    }
}
