using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServerSharedData
{
    public class SharedUtils
    {
        public static string GetPropertyName(string name)
        {
            return name.Split(".".ToCharArray(), StringSplitOptions.RemoveEmptyEntries)[1].Replace(")", "");
        }

        public static void DeleteFilesAndFoldersRecursively(string target_dir)
        {
            var path = Path.Combine(Directory.GetCurrentDirectory(), target_dir);
            if (!Directory.Exists(path))
            {
                return;
            }
            foreach (string file in Directory.GetFiles(path))
            {
                File.Delete(file);
            }

            foreach (string subDir in Directory.GetDirectories(path))
            {
                DeleteFilesAndFoldersRecursively(subDir);
            }

            Thread.Sleep(1); // This makes the difference between whether it works or not. Sleep(0) is not enough.
            Directory.Delete(path);
        }
    }
}
