using ProtoBuf;
using ProtoBuf.Meta;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
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

        public static string Decompress(string input)
        {
            byte[] compressed = Convert.FromBase64String(input);
            byte[] decompressed = Decompress(compressed);
            return Encoding.UTF8.GetString(decompressed);
        }

        public static string Compress(string input)
        {
            byte[] encoded = Encoding.UTF8.GetBytes(input);
            byte[] compressed = Compress(encoded);
            return Convert.ToBase64String(compressed);
        }

        public static byte[] Decompress(byte[] input)
        {
            using (var source = new MemoryStream(input))
            {
                byte[] lengthBytes = new byte[4];
                source.Read(lengthBytes, 0, 4);

                var length = BitConverter.ToInt32(lengthBytes, 0);
                using (var decompressionStream = new GZipStream(source,
                    CompressionMode.Decompress))
                {
                    var result = new byte[length];
                    decompressionStream.Read(result, 0, length);
                    return result;
                }
            }
        }

        public static byte[] Compress(byte[] input)
        {
            using (var result = new MemoryStream())
            {
                var lengthBytes = BitConverter.GetBytes(input.Length);
                result.Write(lengthBytes, 0, 4);

                using (var compressionStream = new GZipStream(result,
                    CompressionMode.Compress))
                {
                    compressionStream.Write(input, 0, input.Length);
                    compressionStream.Flush();

                }
                return result.ToArray();
            }
        }



        public static TData DeserializeFromBytes<TData>(byte[] b)
        {
            using (var memoryStream = new MemoryStream(b))
            {
                var info = Serializer.Deserialize<TData>(memoryStream);
                return info;
            }
        }

        public static byte[] SerializeToBytes<TData>(TData settings)
        {

            using (var memoryStream = new MemoryStream())
            {
                Serializer.Serialize(memoryStream, settings);
                return memoryStream.ToArray();
            }
        }
        //public static TData DeserializeFromBytes<TData>(byte[] b)
        //{
        //    using (var stream = new MemoryStream(b))
        //    {
        //        var formatter = new BinaryFormatter();
        //        stream.Seek(0, SeekOrigin.Begin);
        //        return (TData)formatter.Deserialize(stream);
        //    }
        //}

        //public static byte[] SerializeToBytes<TData>(TData settings)
        //{
        //    using (var stream = new MemoryStream())
        //    {
        //        var formatter = new BinaryFormatter();
        //        formatter.Serialize(stream, settings);
        //        stream.Flush();
        //        stream.Position = 0;
        //        return stream.ToArray();
        //    }
        //}
    }
}
