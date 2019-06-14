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

        private struct Location 
        {
            public double longitude;
            public double latitude;
        }

        private struct PostcodesBulk
        {
            public int status{ get; set; }
            public List<PostcodesEntry> result{ get; set; }
        }

        private struct PostcodesEntry
        {
            public String query{ get; set; }
            public PostcodeResult result { get; set; }
        }
        private class PostcodeResult
        {
            public string postcode { get; set; }
            public double longitude { get; set; }
            public double latitude { get; set; }
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
                        trans.Commit();
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

                    int iter = 1; //Split the string into sets of 100 so that we can use bulk lookup later.
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

        public static void CacheLocation(List<String> postals)
        {
            using (IDbConnection cnn = new SQLiteConnection(LoadConnectionString()))
            {
                cnn.Open();
                using (IDbCommand cmd = cnn.CreateCommand())
                {
                    using (IDbTransaction trans = cnn.BeginTransaction())
                    {
                        cmd.Transaction = trans;
                        cmd.CommandText = "UPDATE Location  SET Longitude = @Longitude, Latitude = @Latitude WHERE ID = @Key";
                        List<Location> locations = new List<Location>();
                        for (int i = 0; i < postals.Count; i++)
                        {
                            GetLocationFromPostal(postals[i], locations);
                        }
                        for (int i = 0; i < entries.Count; i++)
                        {
                            String entry = entries[i];
                            cmd.Parameters.Add(new SQLiteParameter("Key", entry));
                            cmd.Parameters.Add(new SQLiteParameter("Longitude", locations[i].longitude));
                            cmd.Parameters.Add(new SQLiteParameter("Latitude", locations[i].latitude));
                            cmd.ExecuteNonQuery();
                        }
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
                for (int i = 0; i < bulk.result.Count; i++) //Loop through the json string fetching easting and northing. 
                {
                    Location loc = new Location();
                    if (bulk.result[i].result != null)
                    {
                        loc.longitude = bulk.result[i].result.longitude;
                        loc.latitude = bulk.result[i].result.latitude;
                        locations.Add(loc);
                    }
                    else //Missing data, some postal adresses have been terminated. Get coordinates by checking terminated postals.
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
    }
}
