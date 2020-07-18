//using ICSharpCode.SharpZipLib.BZip2;
//using ICSharpCode.SharpZipLib.Tar;
//using LinqDb;
//using System;
//using System.Collections.Generic;
//using System.Diagnostics;
//using System.IO;
//using System.Linq;
//using System.Text;
//using System.Text.RegularExpressions;
//using System.Threading.Tasks;

//namespace Testing.big_data
//{
//    class Freedb : ITest
//    {

//        private const string db_path = @"C:\freedb\";
//        private static List<Disk> disks = new List<Disk>();

//        public void Do(Db db)
//        {
//            bool dispose = false;
//            var sw = ParseDisks(disk => disks.Add(disk));

//            Console.WriteLine("Elapsed: " + sw.Elapsed);

//            if (dispose) { db.Dispose(); }    
//        }

//        static Db db = new Db(db_path);
//        public static void TestEntityOne()
//        {
//            bool dispose = false;
//            while (true)
//            {
//                var sw = Stopwatch.StartNew();
//                Console.WriteLine("What to search: ");
//                var search = Console.ReadLine();
//                int i = 0;
//                while (true)
//                {
//                    var res = db.Table<DiskEntityOne>().Search(f => f.Title, search).Or().Search(f => f.Artist, search).Or().Search(f => f.Tracks, search).OrderBy(f => f.Id).Skip(i).Take(10).SelectEntity();
//                    foreach (var r in res)
//                    {
//                        Console.WriteLine("{0} {1}", r.Artist, r.Title);
//                        foreach (var t in (r.Tracks + "").Split("|".ToArray(), StringSplitOptions.RemoveEmptyEntries))
//                        {
//                            Console.WriteLine("\t{0}", t);
//                        }
//                    }
//                    i++;
//                    Console.WriteLine("Press for more...");
//                    var line = Console.ReadLine();
//                    if (line == "0")
//                    {
//                        break;
//                    }
//                }
//            }
//            if (dispose) { db.Dispose(); }
//        }

//        private static Stopwatch ParseDisks(Action<Disk> addToBatch)
//        {
//            int i = 0;
//            var parser = new Parser();
//            var buffer = new byte[1024 * 1024];// more than big enough for all files

//            var sp = Stopwatch.StartNew();
//            int total = 0;
//            using (var bz2 = new BZip2InputStream(File.Open(@"C:\freedb-complete-20141001.tar.bz2", FileMode.Open)))
//            using (var tar = new TarInputStream(bz2))
//            {
//                TarEntry entry;
//                while ((entry = tar.GetNextEntry()) != null)
//                {
//                    if (entry.Size == 0 || entry.Name == "README" || entry.Name == "COPYING")
//                        continue;
//                    var readSoFar = 0;
//                    while (true)
//                    {
//                        var read = tar.Read(buffer, readSoFar, ((int)entry.Size) - readSoFar);
//                        if (read == 0)
//                            break;

//                        readSoFar += read;
//                    }
//                    // we do it in this fashion to have the stream reader detect the BOM / unicode / other stuff
//                    // so we can read the values properly
//                    var fileText = new StreamReader(new MemoryStream(buffer, 0, readSoFar)).ReadToEnd();
//                    try
//                    {
//                        var disk = parser.Parse(fileText);
//                        addToBatch(disk);
//                        total++;
//                        if (i++ % 1000 == 0)
//                            Console.Write("\r{0} {1:#,#}  {2}         ", entry.Name, i, sp.Elapsed);

//                        if (i % 100000 == 0)
//                        {
//                            Disk.SaveBulk(disks, db);
//                            //Disk.SaveBulkOne(disks);
//                            disks = new List<Disk>();
//                            Console.Write("{0}|||", total);
//                        }
//                    }
//                    catch (Exception e)
//                    {
//                        Console.WriteLine();
//                        Console.WriteLine(entry.Name);
//                        Console.WriteLine(e);
//                        return sp;
//                    }
//                }

//                Disk.SaveBulk(disks, db);
//                //Disk.SaveBulkOne(disks);
//                disks = new List<Disk>();
//            }
//            return sp;
//        }



//        public string GetName()
//        {
//            return this.GetType().Name;
//        }
//    }


//    public class Disk
//    {
//        public string Title { get; set; }
//        public string Artist { get; set; }
//        public int DiskLength { get; set; }
//        public string Genre { get; set; }
//        public int Year { get; set; }
//        public List<string> DiskIds { get; set; }

//        public List<int> TrackFramesOffsets { get; set; }
//        public List<string> Tracks { get; set; }
//        public Dictionary<string, string> Attributes { get; set; }
//        public Disk()
//        {
//            TrackFramesOffsets = new List<int>();
//            Tracks = new List<string>();
//            DiskIds = new List<string>();
//            Attributes = new Dictionary<string, string>();
//        }
//        static int disk_id = 1;
//        public static void SaveBulk(List<Disk> disks, Db db)
//        {
//            var diskent = new List<DiskEntity>();
//            var attributes = new List<Attribute>();
//            var diskids = new List<DiskId>();
//            var offsets = new List<Offset>();
//            var tracks = new List<Track>();
//            foreach (var d in disks)
//            {
//                var combined = DiskToEntity(d);
//                combined.Disk.Id = disk_id;

//                diskent.Add(combined.Disk);
//                foreach (var a in combined.Attributes)
//                {
//                    a.DiskEntityId = disk_id;
//                }
//                attributes.AddRange(combined.Attributes);
//                foreach (var di in combined.DiskIds)
//                {
//                    di.DiskEntityId = disk_id;
//                }
//                diskids.AddRange(combined.DiskIds);
//                foreach (var o in combined.Offsets)
//                {
//                    o.DiskEntityId = disk_id;
//                }
//                offsets.AddRange(combined.Offsets);
//                foreach (var t in combined.Tracks)
//                {
//                    t.DiskEntityId = disk_id;
//                }
//                tracks.AddRange(combined.Tracks);

//                disk_id++;
//            }

//            db.Table<DiskEntity>().SaveBatch(diskent);
//            db.Table<Attribute>().SaveBatch(attributes);
//            db.Table<DiskId>().SaveBatch(diskids);
//            db.Table<Offset>().SaveBatch(offsets);
//            db.Table<Track>().SaveBatch(tracks);
//        }
//        public static void SaveBulkOne(List<Disk> disks, Db db)
//        {
//            var list = new List<DiskEntityOne>();
//            foreach (var d in disks)
//            {
//                var entity = DiskToEntityOne(d);
//                list.Add(entity);
//            }
//            db.Table<DiskEntityOne>().SaveBatch(list);
//        }
//        public static Combined DiskToEntity(Disk disk)
//        {
//            var res = new Combined();
//            res.Disk = new DiskEntity();
//            res.Disk.Artist = disk.Artist;
//            res.Disk.DiskLength = disk.DiskLength;
//            res.Disk.Genre = disk.Genre;
//            res.Disk.Title = disk.Title;
//            res.Disk.Year = disk.Year;
//            res.DiskIds = disk.DiskIds != null ? disk.DiskIds.ToList().Select(f => new DiskId()
//            {
//                DiskIdValue = f
//            }).ToList() : new List<DiskId>();
//            res.Attributes = disk.Attributes != null ? disk.Attributes.ToList().Select(f => new Attribute()
//            {
//                Name = f.Key,
//                Value = f.Value
//            }).ToList() : new List<Attribute>();
//            res.Offsets = disk.TrackFramesOffsets != null ? disk.TrackFramesOffsets.ToList().Select(f => new Offset()
//            {
//                OffsetValue = f
//            }).ToList() : new List<Offset>();
//            res.Tracks = disk.Tracks != null ? disk.Tracks.ToList().Select(f => new Track()
//            {
//                Title = f
//            }).ToList() : new List<Track>();
//            return res;
//        }
//        public static DiskEntityOne DiskToEntityOne(Disk disk)
//        {
//            var res = new DiskEntityOne();
//            res.Artist = disk.Artist;
//            res.Title = disk.Title;
//            res.DiskLength = disk.DiskLength;
//            res.Genre = disk.Genre;
//            res.Year = disk.Year;
//            res.Attributes = disk.Attributes != null && disk.Attributes.Any() ? disk.Attributes.Select(f => f.Key + "|" + f.Value).Aggregate((a, b) => a + ":" + b) : null;
//            res.Tracks = disk.Tracks != null && disk.Tracks.Any() ? disk.Tracks.Aggregate((a, b) => a + "|" + b) : null;

//            var offsets = new List<byte>();
//            foreach (var o in disk.TrackFramesOffsets)
//            {
//                offsets.AddRange(BitConverter.GetBytes(o));
//            }
//            res.Offsets = offsets.ToArray();
//            return res;
//        }
//    }

//    public class DiskEntityOne
//    {
//        public int Id { get; set; }
//        public string Title { get; set; }
//        public string Artist { get; set; }
//        public int DiskLength { get; set; }
//        public string Genre { get; set; }
//        public int Year { get; set; }
//        public string Attributes { get; set; }
//        public string Tracks { get; set; }
//        public byte[] Offsets { get; set; }
//        public byte[] DiskIds { get; set; }

//    }



//    public class Combined
//    {
//        public DiskEntity Disk { get; set; }
//        public List<Attribute> Attributes { get; set; }
//        public List<Track> Tracks { get; set; }
//        public List<Offset> Offsets { get; set; }
//        public List<DiskId> DiskIds { get; set; }
//    }
//    public class DiskEntity
//    {
//        public int Id { get; set; }
//        public string Title { get; set; }
//        public string Artist { get; set; }
//        public int DiskLength { get; set; }
//        public string Genre { get; set; }
//        public int Year { get; set; }
//    }
//    public class Attribute
//    {
//        public int Id { get; set; }
//        public int DiskEntityId { get; set; }
//        public string Name { get; set; }
//        public string Value { get; set; }
//    }
//    public class Track
//    {
//        public int Id { get; set; }
//        public int DiskEntityId { get; set; }
//        public string Title { get; set; }
//    }
//    public class Offset
//    {
//        public int Id { get; set; }
//        public int DiskEntityId { get; set; }
//        public int OffsetValue { get; set; }
//    }
//    public class DiskId
//    {
//        public int Id { get; set; }
//        public int DiskEntityId { get; set; }
//        public string DiskIdValue { get; set; }
//    }

//    public class Parser
//    {
//        readonly List<Tuple<Regex, Action<Disk, MatchCollection>>> actions = new List<Tuple<Regex, Action<Disk, MatchCollection>>>();

//        public Parser()
//        {
//            Add(@"^\#\s+xmcd", (disk, collection) =>
//            {
//                if (collection.Count == 0)
//                    throw new InvalidDataException("Not an XMCD file");
//            });

//            Add(@"^\# \s* Track \s+ frame \s+ offsets \s*: \s* \n (^\# \s* (\d+) \s* \n)+", (disk, collection) =>
//            {
//                foreach (Capture capture in collection[0].Groups[2].Captures)
//                {
//                    disk.TrackFramesOffsets.Add(int.Parse(capture.Value));
//                }
//            });

//            Add(@"Disc \s+ length \s*: \s* (\d+)", (disk, collection) =>
//                                                              disk.DiskLength = int.Parse(collection[0].Groups[1].Value)
//                );

//            Add("DISCID=(.+)", (disk, collection) =>
//            {
//                var strings = collection[0].Groups[1].Value.Split(new[] { "," },
//                                                                  StringSplitOptions.RemoveEmptyEntries);
//                disk.DiskIds.AddRange(strings.Select(x => x.Trim()));
//            });

//            Add("DTITLE=(.+)", (disk, collection) =>
//            {
//                var parts = collection[0].Groups[1].Value.Split(new[] { "/" }, 2, StringSplitOptions.RemoveEmptyEntries);
//                if (parts.Length == 2)
//                {
//                    disk.Artist = parts[0].Trim();
//                    disk.Title = parts[1].Trim();
//                }
//                else
//                {
//                    disk.Title = parts[0].Trim();
//                }
//            });

//            Add(@"DYEAR=(\d+)", (disk, collection) =>
//            {
//                if (collection.Count == 0)
//                    return;
//                var value = collection[0].Groups[1].Value;
//                if (value.Length > 4) // there is data like this
//                {
//                    value = value.Substring(value.Length - 4);
//                }
//                disk.Year = int.Parse(value);
//            }
//            );

//            Add(@"DGENRE=(.+)", (disk, collection) =>
//            {
//                if (collection.Count == 0)
//                    return;
//                disk.Genre = collection[0].Groups[1].Value.Trim();
//            }
//            );

//            Add(@"TTITLE\d+=(.+)", (disk, collection) =>
//            {
//                foreach (Match match in collection)
//                {
//                    disk.Tracks.Add(match.Groups[1].Value.Trim());
//                }
//            });

//            Add(@"(EXTD\d*)=(.+)", (disk, collection) =>
//            {
//                foreach (Match match in collection)
//                {
//                    var key = match.Groups[1].Value;
//                    string value;
//                    if (disk.Attributes.TryGetValue(key, out value))
//                    {
//                        disk.Attributes[key] = value + match.Groups[2].Value.Trim();
//                    }
//                    else
//                    {
//                        disk.Attributes[key] = match.Groups[2].Value.Trim();
//                    }
//                }
//            });
//        }

//        private void Add(string regex, Action<Disk, MatchCollection> action)
//        {
//            var key = new Regex(regex, RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace | RegexOptions.Multiline);
//            actions.Add(Tuple.Create(key, action));
//        }

//        public Disk Parse(string text)
//        {
//            var disk = new Disk();
//            foreach (var action in actions)
//            {
//                var collection = action.Item1.Matches(text);
//                try
//                {
//                    action.Item2(disk, collection);
//                }
//                catch (Exception e)
//                {
//                    Console.WriteLine();
//                    Console.WriteLine(text);
//                    Console.WriteLine(action.Item1);
//                    Console.WriteLine(e);
//                    throw;
//                }
//            }

//            return disk;
//        }
//    }

//}
