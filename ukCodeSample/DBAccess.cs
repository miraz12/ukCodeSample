using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Net;
using System.Threading;
using Newtonsoft.Json;

namespace ukCodeSample
{
    public class DBAccess
    {
        private static readonly List<string> entries = new List<string>();

        //Reload CSV file into SQLite database
        public static void ReloadLoadCsv()
        {
            try
            {
                entries.Clear();
                using (IDbConnection cnn = new SQLiteConnection(LoadConnectionString()))
                {
                    cnn.Open();
                    using (IDbCommand cmd = cnn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = "DELETE FROM Location;" +
                                          "DELETE FROM Email;" +
                                          "DELETE FROM CacheInfo;" +
                                          "VACUUM";
                        cmd.ExecuteNonQuery();


                        using (IDbTransaction trans = cnn.BeginTransaction())
                        {
                            cmd.Transaction = trans;
                            cmd.CommandText = "INSERT INTO Location (ID, Postal) values (@ID, @Postal);" +
                                              "INSERT INTO Email (ID, Email) values (@ID, @Email);";

                            using (var reader = new StreamReader(@"uk-500.csv"))
                            {
                                reader.ReadLine();
                                while (!reader.EndOfStream)
                                {
                                    var line = reader.ReadLine();
                                    var values = line.Split(';');
                                    string entry = values[0] + ":" + values[1];
                                    entries.Add(entry);
                                    cmd.Parameters.Add(new SQLiteParameter("ID", entry));
                                    cmd.Parameters.Add(new SQLiteParameter("Postal", values[6]));
                                    cmd.Parameters.Add(new SQLiteParameter("Email", values[9]));
                                    cmd.ExecuteNonQuery();
                                }
                            }

                            cmd.CommandText = "INSERT INTO CacheInfo (CsvCacheDate) values (@CsvCacheDate);";
                            cmd.Parameters.Add(new SQLiteParameter("CsvCacheDate", DateTime.Now));
                            cmd.ExecuteNonQuery();
                            trans.Commit();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error reloading CSV: " + e);
            }
        }

        //Load location data from DB. Used when CSV is already loaded in the DB.
        public static void LoadFromDB()
        {
            try
            {
                using (IDbConnection cnn = new SQLiteConnection(LoadConnectionString()))
                {
                    cnn.Open();
                    using (IDbCommand cmd = cnn.CreateCommand())
                    {
                        cmd.CommandText = "SELECT * FROM Location";
                        cmd.CommandType = CommandType.Text;
                        IDataReader r = cmd.ExecuteReader();
                        while (r.Read())
                        {
                            string entry = Convert.ToString(r["ID"]);
                            entries.Add(entry);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error loading from DB: " + e);
            }
        }

        //Get email entries
        public static void GetEmails(Dictionary<string, int> domains)
        {
            try
            {
                using (IDbConnection cnn = new SQLiteConnection(LoadConnectionString()))
                {
                    cnn.Open();
                    using (IDbCommand cmd = cnn.CreateCommand())
                    {
                        cmd.CommandText = "SELECT * FROM Email";
                        cmd.CommandType = CommandType.Text;
                        IDataReader r = cmd.ExecuteReader();
                        while (r.Read())
                        {
                            string entry = Convert.ToString(r["email"]).Split('@')[1];
                            if (domains.ContainsKey(entry))
                                domains[entry]++;
                            else
                                domains.Add(entry, 1);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error getting Emails: " + e);
            }
        }

        //Get postal codes and return them in List<String> Postals
        public static void GetPostals(List<string> Postals)
        {
            try
            {
                using (IDbConnection cnn = new SQLiteConnection(LoadConnectionString()))
                {
                    cnn.Open();
                    using (IDbCommand cmd = cnn.CreateCommand())
                    {
                        int numFetches = entries.Count / 100;
                        for (int i = 0; i < numFetches; i++)
                        {
                            cmd.CommandText = "SELECT * FROM Location LIMIT " + 100 + " OFFSET " + 100 * i + ";";
                            cmd.CommandType = CommandType.Text;
                            IDataReader r = cmd.ExecuteReader();

                            string splitString = "[";
                            int iter = 0;
                            while (r.Read())
                            {
                                iter++;
                                string entry = Convert.ToString(r["Postal"]);
                                splitString += "\"" + entry + "\",";
                            }

                            splitString = splitString.Remove(splitString.Length - 1);
                            splitString += "]";
                            Postals.Add(splitString);
                            r.Close();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error getting postals: " + e);
            }
        }

        //Cache the location data into the DB
        public static void CacheLocation(List<string> postals)
        {
            try
            {
                using (IDbConnection cnn = new SQLiteConnection(LoadConnectionString()))
                {
                    cnn.Open();
                    using (IDbCommand cmd = cnn.CreateCommand())
                    {
                        //Empty table
                        using (IDbTransaction trans = cnn.BeginTransaction())
                        {
                            cmd.Transaction = trans;
                            cmd.CommandText =
                                "UPDATE Location  SET Longitude = @Longitude, Latitude = @Latitude, Region = @Region WHERE Postal = @Key;";
                            List<Location> locations = new List<Location>();
                            locations.Capacity = 500;
                            Thread[] objThread = new Thread[5];
                            for (int i = 0; i < postals.Count; i++)
                            {
                                int iCopy = i;
                                objThread[iCopy] = new Thread(() => GetLocationFromPostal(postals[iCopy], locations));
                                objThread[iCopy].Start();
                            }

                            for (int i = 0; i < objThread.Length; i++)
                            {
                                // Wait until thread is finished.
                                objThread[i].Join();
                            }
                            //Add long lat data to database so that we don't have to query API every time. 
                            for (int i = 0; i < locations.Count; i++)
                            {
                                cmd.Parameters.Add(new SQLiteParameter("Key", locations[i].postcode));
                                cmd.Parameters.Add(new SQLiteParameter("Longitude", locations[i].longitude));
                                cmd.Parameters.Add(new SQLiteParameter("Latitude", locations[i].latitude));
                                cmd.Parameters.Add(new SQLiteParameter("Region", locations[i].region));
                                cmd.ExecuteNonQuery();
                            }

                            //Update cache entry in cache table.
                            cmd.CommandText = "UPDATE CacheInfo SET LocationCacheDate = @LocationCacheDate;";
                            cmd.Parameters.Add(new SQLiteParameter("LocationCacheDate", DateTime.Now));
                            cmd.ExecuteNonQuery();

                            trans.Commit();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error caching locations: " + e);
            }
        }

        private static void GetLocationFromPostal(string postal, List<Location> locations)
        {
            try
            {
                HttpWebRequest WebReq = (HttpWebRequest) WebRequest.Create("https://api.postcodes.io/postcodes/");
                WebReq.Method = "POST";
                WebReq.ContentType = "application/json";

                using (var streamWriter = new StreamWriter(WebReq.GetRequestStream()))
                {
                    string json = "{ \"postcodes\":" + postal + "}";
                    streamWriter.Write(json);
                    streamWriter.Flush();
                }

                var httpResponse = (HttpWebResponse) WebReq.GetResponse();
                using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
                {
                    var responseText = streamReader.ReadToEnd();
                    PostcodesBulk bulk = JsonConvert.DeserializeObject<PostcodesBulk>(responseText);
                    //Loop through the json string fetching long and lat. 
                    for (int i = 0; i < bulk.result.Count; i++)
                    {
                        Location loc = new Location();
                        if (bulk.result[i].result != null)
                        {
                            loc.postcode = bulk.result[i].result.postcode;
                            loc.longitude = bulk.result[i].result.longitude;
                            loc.latitude = bulk.result[i].result.latitude;
                            loc.region = bulk.result[i].result.european_electoral_region;

                            locations.Add(loc);
                        }

                        //Missing data, some postal addresses have been terminated. Get coordinates by checking terminated postals. Sadly doesn't contain region so it will become invalid.
                        else
                        {
                            string postalCode = bulk.result[i].query;
                            WebReq = (HttpWebRequest) WebRequest.Create(
                                string.Format("https://api.postcodes.io/terminated_postcodes/" + postalCode));
                            WebReq.Method = "GET";

                            httpResponse = (HttpWebResponse) WebReq.GetResponse();
                            using (var streamReaderTerm = new StreamReader(httpResponse.GetResponseStream()))
                            {
                                responseText = streamReaderTerm.ReadToEnd();
                                PostcodesEntry entry = JsonConvert.DeserializeObject<PostcodesEntry>(responseText);

                                loc.postcode = entry.result.postcode;
                                loc.longitude = entry.result.longitude;
                                loc.latitude = entry.result.latitude;
                                loc.region =
                                    "invalid"; //Could fetch this from closest postal code from long lat but trying to keep things simple.
                                locations.Add(loc);
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error fetching postals: " + e);
            }
        }

        private static string LoadConnectionString(string id = "Default")
        {
            return ConfigurationManager.ConnectionStrings[id].ConnectionString;
        }

        //Maybe split this into two, one for fetching long lat and one for region.
        public static List<Location> GetLocations()
        {
            List<Location> outLocations = new List<Location>();
            try
            {
                using (IDbConnection cnn = new SQLiteConnection(LoadConnectionString()))
                {
                    cnn.Open();
                    using (IDbCommand cmd = cnn.CreateCommand())
                    {
                        cmd.CommandText = "SELECT * FROM Location";
                        cmd.CommandType = CommandType.Text;
                        IDataReader r = cmd.ExecuteReader();
                        while (r.Read())
                        {
                            Location l = new Location();
                            l.longitude = Convert.ToDouble(r["Longitude"]);
                            l.latitude = Convert.ToDouble(r["Latitude"]);
                            l.region = Convert.ToString(r["Region"]);
                            outLocations.Add(l);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error getting Locations: " + e);
            }

            return outLocations;
        }

        //Fetch regions while counting occurrences and saving them in dict
        public static void GetRegions(Dictionary<string, int> dict)
        {
            try
            {
                using (IDbConnection cnn = new SQLiteConnection(LoadConnectionString()))
                {
                    cnn.Open();
                    using (IDbCommand cmd = cnn.CreateCommand())
                    {
                        cmd.CommandText = "SELECT * FROM Location";
                        cmd.CommandType = CommandType.Text;
                        IDataReader r = cmd.ExecuteReader();
                        while (r.Read())
                        {
                            string entry = Convert.ToString(r["Region"]);
                            if (dict.ContainsKey(entry))
                                dict[entry]++;
                            else
                                dict.Add(entry, 1);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error getting regions: " + e);
            }
        }

        //Check if csv data should be reloaded
        public static bool CheckCacheCsv()
        {
            try
            {
                using (IDbConnection cnn = new SQLiteConnection(LoadConnectionString()))
                {
                    cnn.Open();
                    using (IDbCommand cmd = cnn.CreateCommand())
                    {
                        cmd.CommandText = "SELECT * FROM CacheInfo";
                        cmd.CommandType = CommandType.Text;
                        IDataReader r = cmd.ExecuteReader();
                        if (r.Read())
                        {
                            string entry = Convert.ToString(r["CsvCacheDate"]);
                            if (entry == "-1")
                                return true;

                            DateTime timeEntry = Convert.ToDateTime(entry);
                            if ((timeEntry - DateTime.Now).TotalDays > 1) return true;
                            return false;
                        }

                        return true;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error checking for cache CSV: " + e);
            }

            return false;
        }

        //Check if location data should be cached
        public static bool CheckCacheLocation()
        {
            try
            {
                using (IDbConnection cnn = new SQLiteConnection(LoadConnectionString()))
                {
                    cnn.Open();
                    using (IDbCommand cmd = cnn.CreateCommand())
                    {
                        cmd.CommandText = "SELECT * FROM CacheInfo";
                        cmd.CommandType = CommandType.Text;
                        IDataReader r = cmd.ExecuteReader();
                        if (r.Read())
                        {
                            string entry = Convert.ToString(r["LocationCacheDate"]);
                            if (entry == "-1")
                                return true;

                            DateTime timeEntry = Convert.ToDateTime(entry);
                            if ((timeEntry - DateTime.Now).TotalDays > 1) return true;

                            return false;
                        }

                        return true;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error checking for cache Location: " + e);
            }

            return false;
        }

        public struct Location
        {
            public string postcode;
            public double longitude;
            public double latitude;
            public string region;
        }

        //Json classes for postcode data
        private class PostcodesBulk
        {
            public int status { get; set; }
            public List<PostcodesEntry> result { get; set; }
        }

        private class PostcodesEntry
        {
            public string query { get; set; }
            public PostcodeResult result { get; set; }
        }

        private class PostcodeResult
        {
            public string postcode { get; set; }
            public double longitude { get; set; }
            public double latitude { get; set; }
            public string european_electoral_region { get; set; }
        }
    }
}