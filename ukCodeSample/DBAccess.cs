using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace ukCodeSample
{
    public class DBAccess
    {
        private static List<String> entries = new List<string>();

        public struct Location 
        {
            public double longitude;
            public double latitude;
            public String region;
        }

        //Json classes for postcode data
        private class PostcodesBulk
        {
            public int status{ get; set; }
            public List<PostcodesEntry> result{ get; set; }
        }

        private class PostcodesEntry
        {
            public String query{ get; set; }
            public PostcodeResult result { get; set; }
        }
        private class PostcodeResult
        {
            public string postcode { get; set; }
            public double longitude { get; set; }
            public double latitude { get; set; }
            public string european_electoral_region { get; set; }
        }

        //Reload CSV file into SQLite database
        public static void ReloadLoadCsv()
        {
            entries.Clear();
            using (IDbConnection cnn = new SQLiteConnection(LoadConnectionString()))
            {
                cnn.Open();
                using (IDbCommand cmd = cnn.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = "DELETE FROM Location;"+
                                      "DELETE FROM Email;"+
                                      "DELETE FROM CacheInfo;" +
                                      "VACUUM";
                    cmd.ExecuteNonQuery();


                    using (IDbTransaction trans = cnn.BeginTransaction())
                    {
                        cmd.Transaction = trans;
                        cmd.CommandText = "INSERT INTO Location (ID, Postal) values (@ID, @Postal);" +
                                          "INSERT INTO Email (ID, Email) values (@ID, @Email);";

                        using(var reader = new StreamReader(@"uk-500.csv"))
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

        //Load location data from DB. Used when CSV is already loaded in the DB.
        public static void LoadFromDB()
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
                        String entry = Convert.ToString(r["ID"]);
                        entries.Add(entry);
                    }
                }
            }
        }

        //Get email entries
        public static void GetEmails(Dictionary<String, int> domains)
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
                        String entry = Convert.ToString(r["email"]).Split('@')[1];
                        if (domains.ContainsKey(entry))
                        {
                            domains[entry]++;
                        }
                        else
                        {
                            domains.Add(entry, 1);
                        }
                    }
                }
            }
        }

        //Get postal codes and return them in List<String> Postals
        public static void GetPostals(List<String> Postals)
        {
            using (IDbConnection cnn = new SQLiteConnection(LoadConnectionString()))
            {
                cnn.Open();
                using (IDbCommand cmd = cnn.CreateCommand())
                {
                    cmd.CommandText = "SELECT * FROM Location";
                    cmd.CommandType = CommandType.Text;
                    IDataReader r = cmd.ExecuteReader();

                    int iter = 1; //Split the string into sets of 100 so that we can use bulk lookup later. Pretty slow even with bulk fetch, use multiple threads? 
                    String splitString = "[";
                    while (r.Read())
                    {
                        String entry = Convert.ToString(r["Postal"]);
                        splitString += "\"" + entry + "\"";
                        iter++;
                        if (iter == 101)
                        {
                            iter = 1;
                            Postals.Add(splitString+"]");
                            splitString = "[";
                        }
                        else
                        {
                            splitString += ",";
                        }
                    }
                }
            }
        }

        //Cache the location data into the DB
        public static void CacheLocation(List<String> postals)
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
                        cmd.CommandText = "UPDATE Location  SET Longitude = @Longitude, Latitude = @Latitude, Region = @Region WHERE ID = @Key;";
                        List<Location> locations = new List<Location>();
                        for (int i = 0; i < postals.Count; i++)
                        {
                            GetLocationFromPostal(postals[i], locations);
                        }
                        for (int i = 0; i < entries.Count; i++) //Add long lat data to database so that we don't have to query API every time. 
                        {
                            String entry = entries[i];
                            cmd.Parameters.Add(new SQLiteParameter("Key", entry));
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

        private static void GetLocationFromPostal(String postal, List<Location> locations)
        {
            HttpWebRequest WebReq = (HttpWebRequest)WebRequest.Create(string.Format("https://api.postcodes.io/postcodes/"));
            WebReq.Method = "POST";
            WebReq.ContentType = "application/json";

            using (var streamWriter = new StreamWriter(WebReq.GetRequestStream()))
            {
                string json = "{ \"postcodes\":" + postal + "}";
                streamWriter.Write(json);
                streamWriter.Flush();
            }
            
            var httpResponse = (HttpWebResponse)WebReq.GetResponse();
            using (var streamReader = new StreamReader(httpResponse.GetResponseStream()))
            {
                var responseText = streamReader.ReadToEnd();
                PostcodesBulk bulk = JsonConvert.DeserializeObject<PostcodesBulk>(responseText);
                for (int i = 0; i < bulk.result.Count; i++) //Loop through the json string fetching long and lat. 
                {
                    Location loc = new Location();
                    if (bulk.result[i].result != null)
                    {
                        loc.longitude = bulk.result[i].result.longitude;
                        loc.latitude = bulk.result[i].result.latitude;
                        loc.region = bulk.result[i].result.european_electoral_region;

                        locations.Add(loc);
                    }
                    else //Missing data, some postal addresses have been terminated. Get coordinates by checking terminated postals. Sadly doesn't contain region so it will become invalid.
                    {
                        string postalCode = bulk.result[i].query;
                        WebReq = (HttpWebRequest)WebRequest.Create(string.Format("https://api.postcodes.io/terminated_postcodes/" + postalCode));
                        WebReq.Method = "GET";

                        httpResponse = (HttpWebResponse)WebReq.GetResponse();
                        using (var streamReaderTerm = new StreamReader(httpResponse.GetResponseStream()))
                        {
                            responseText = streamReaderTerm.ReadToEnd();
                            PostcodesEntry entry = JsonConvert.DeserializeObject<PostcodesEntry>(responseText);

                            loc.longitude = entry.result.longitude;
                            loc.latitude = entry.result.latitude;
                            loc.region = "invalid"; //Could fetch this from closest postal code from long lat but trying to keep things simple.
                            locations.Add(loc);
                        }
                    }
                }
            }
        }

        private static string LoadConnectionString(string id = "Default")
        {
            return ConfigurationManager.ConnectionStrings[id].ConnectionString;
        }

        public static List<Location> GetLocations() //Maybe split this into two, one for fetching long lat and one for region.
        {
            List<Location> outLocations = new List<Location>();
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

            return outLocations;
        }

        //Fetch regions while counting occurrences and saving them in dict
        public static void GetRegions(Dictionary<string, int> dict)
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
                        String entry = Convert.ToString(r["Region"]);
                        if (dict.ContainsKey(entry))
                        {
                            dict[entry]++;
                        }
                        else
                        {
                            dict.Add(entry, 1);
                        }
                    }
                }
            }
        }

        //Check if csv data should be reloaded
        public static bool CheckCacheCsv()
        {
            using (IDbConnection cnn = new SQLiteConnection(LoadConnectionString()))
            {
                cnn.Open();
                using (IDbCommand cmd = cnn.CreateCommand())
                {
                    cmd.CommandText = "SELECT * FROM CacheInfo";
                    cmd.CommandType = CommandType.Text;
                    IDataReader r = cmd.ExecuteReader();
                    if(r.Read())
                    {
                        String entry = Convert.ToString(r["CsvCacheDate"]);
                        if (entry == "-1")
                            return true;

                        DateTime timeEntry = Convert.ToDateTime(entry);
                        if ((timeEntry - DateTime.Now).TotalDays > 1)
                        {
                            return true;
                        }
                        return false;
                    }
                    else
                    {
                        return true;
                    }
                }
            }
        }

        //Check if location data should be cached
        public static bool CheckCacheLocation()
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
                        String entry = Convert.ToString(r["LocationCacheDate"]);
                        if (entry == "-1")
                            return true;

                        DateTime timeEntry = Convert.ToDateTime(entry);
                        if ((timeEntry - DateTime.Now).TotalDays > 1)
                        {
                            return true;
                        }

                        return false;
                    }
                    else
                    {
                        return true;
                    }
                }
            }
        }


    }
}
