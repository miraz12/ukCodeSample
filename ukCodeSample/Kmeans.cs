using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Win32;

namespace ukCodeSample
{
    class Kmeans
    {
        private class DataPoint
        {
            public double longitude { get; set; }
            public double latitude { get; set; }
            public int cluster { get; set; }
        }

        private List<DataPoint> locationData = new List<DataPoint>();
        private List<DataPoint> cluseter = new List<DataPoint>();
        private int numClusters = 9; //same as number of regions in the UK
        private double minLat;
        private double maxLat;
        private double minLong;
        private double maxLong;

        public Kmeans(List<DBAccess.Location> l)
        {
            maxLong = minLong = l[0].longitude;
            maxLat = minLat = l[0].latitude;
            for (int i = 0; i < l.Count; i++)
            {
                double longi = l[i].longitude;
                double latil = l[i].latitude;

                //Save min max so we can now between which coordinates we should place the initial centroids.
                if (longi > maxLong)
                    maxLong = longi;
                else if (longi < minLong)
                    minLong = longi;

                if (latil > maxLat)
                    maxLat = latil;
                else if (latil < minLat)
                    minLat = latil;

                DataPoint entry = new DataPoint();
                entry.longitude = l[i].longitude;
                entry.latitude= l[i].latitude;
                entry.cluster = -1;
                locationData.Add(entry);
            }

            InitClusters();
        }

        private void InitClusters()
        {
            Random rand = new Random(numClusters);
            for (int i = 0; i < numClusters; i++) //Make sure no cluster is empty by assigning one data point to each cluster
            {
                locationData[i].cluster = i;
                cluseter.Add(new DataPoint() { cluster = i });
            }

            for (int i = numClusters; i < locationData.Count; i++) //Randomly assign the rest of the data points
            {
                locationData[i].cluster = rand.Next(0, numClusters);
                
            }
        }

        public List<String> Cluster()
        {
            bool change = true;
            bool sucess = true;

            int maxIter = locationData.Count * 10;
            int iter = 0;

            while (sucess && change && iter < maxIter)
            {
                ++iter;
                sucess = RecalculateMeans();
                change = RecalculateClusters();
            }

            //Return number of entries in each cluster.
            List<String> outList = new List<string>();
            var group = locationData.GroupBy(s => s.cluster).OrderBy(s => s.Key);
            foreach (var g in group)
            {
                int entries = 0;
                foreach (var value in g)
                {
                    entries++;
                }
                outList.Add("Cluster " + g.Key + " entries: " + entries);
            }
            return outList;
        }

        private bool RecalculateMeans()
        {
            if (EmptyClusters())
                return false;

            //Update cluster mean values
            var clusterGroups = locationData.GroupBy(s => s.cluster).OrderBy(s => s.Key);
            int index = 0;
            double longitude = 0.0;
            double latitude = 0.0;
            foreach (var item in clusterGroups)
            {
                foreach (var value in item)
                {
                    longitude += value.longitude;
                    latitude += value.latitude;
                }

                cluseter[index].longitude = longitude / item.Count();
                cluseter[index].latitude = latitude / item.Count();
                index++;
                longitude = 0.0;
                latitude = 0.0;
            }
            return true;
        }

        private bool RecalculateClusters()
        {
            bool change = false;
            double[] distances = new double[numClusters];

            StringBuilder sb = new StringBuilder(); //TODO: REMOVE
            for (int i = 0; i < locationData.Count; i++)
            {
                for (int j = 0; j < numClusters; j++)
                {
                    distances[j] = Distance(locationData[i], cluseter[j]);

                    int newClusterId = GetMinIndex(distances);
                    if (newClusterId != locationData[i].cluster)
                    {
                        change = true;
                        locationData[i].cluster = newClusterId;
                    }
                 
                }
            }

            if (EmptyClusters())
                return false;

            return change;
        }

        private int GetMinIndex(double[] distances)
        {
            int returnIndex = 0;
            double minDist = distances[0];

            for (int i = 0; i < distances.Length; i++)
            {
                if (distances[i] < minDist)
                {
                    minDist = distances[i];
                    returnIndex = i;
                }
            }
            return returnIndex;
        }

        //Convert long lat to cartesian coordinates and get Euclidean distance. 
        private double Distance(DataPoint loc1, DataPoint loc2)
        {
            double radius = 6371; //km

            double x1 = radius * Math.Cos(loc1.latitude) * Math.Cos(loc1.longitude);
            double y1 = radius * Math.Cos(loc1.latitude) * Math.Sin(loc1.longitude);

            double x2 = radius * Math.Cos(loc2.latitude) * Math.Cos(loc2.longitude);
            double y2 = radius * Math.Cos(loc2.latitude) * Math.Sin(loc2.longitude);

            return (Math.Pow(x2-x1, 2) + Math.Pow(y2-y1, 2)); //squared distance to make it faster
        }

        private bool EmptyClusters() //Check for empty clusters
        {
            //Check for empty clusters
            var clusterGroupsEmpty = locationData.GroupBy(s => s.cluster).OrderBy(s => s.Key).Select(g => new { Cluster = g.Key, Count = g.Count() });
            foreach (var item in clusterGroupsEmpty)
            {
                if (item.Count == 0)
                    return true;
            }

            return false;
        }
    }
}
