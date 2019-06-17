using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ukCodeSample
{
    internal class Kmeans
    {
        private readonly List<DataPoint> cluseter = new List<DataPoint>();

        private readonly List<DataPoint> locationData = new List<DataPoint>();
        private readonly int numClusters = 9; //same as number of regions in the UK


        public Kmeans(List<DBAccess.Location> l)
        {
            //Populate data list
            for (int i = 0; i < l.Count; i++)
            {
                DataPoint entry = new DataPoint();
                entry.longitude = l[i].longitude;
                entry.latitude = l[i].latitude;
                entry.cluster = -1;
                locationData.Add(entry);
            }

            InitClusters();
        }

        private void InitClusters()
        {
            Random rand = new Random(numClusters);

            //Make sure no cluster is empty by assigning one data point to each cluster
            for (int i = 0; i < numClusters; i++)
            {
                locationData[i].cluster = i;
                cluseter.Add(new DataPoint {cluster = i});
            }

            for (int i = numClusters; i < locationData.Count; i++) //Randomly assign the rest of the data points
            {
                locationData[i].cluster = rand.Next(0, numClusters);
                
            }
        }

        public List<string> Cluster()
        {
            bool change = true;
            bool sucess = true;

            int maxIter = locationData.Count * 10;
            int iter = 0;

            //Loop until fail, there is no change or maximum iterations have been reached
            while (sucess && change && iter < maxIter)
            {
                ++iter;
                sucess = RecalculateMeans();
                change = RecalculateClusters();
            }

            //Return number of entries in each cluster.
            List<string> outList = new List<string>();
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

        //Recalculate the clusters centroid positions.
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

        //Calculate distance between each entry and each centroid and group accordingly.
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

        //Return cluster with minimum distance
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

        //Get distance betewen long lat with Haversine function
        private double Distance(DataPoint loc1, DataPoint loc2)
        {
            double radius = 6378.137; //km

            double rad1Y = loc1.latitude * Math.PI / 180.0;
            double rad2Y = loc2.latitude * Math.PI / 180.0;

            double latDistance = (loc2.latitude - loc1.latitude) * Math.PI / 180.0;
            double lonDistance = (loc2.longitude - loc1.longitude) * Math.PI / 180.0;

            double a = Math.Sin(latDistance / 2)
                       * Math.Sin(latDistance / 2)
                       + Math.Cos(rad1Y)
                       * Math.Cos(rad2Y)
                       * Math.Sin(lonDistance / 2) * Math.Sin(lonDistance / 2);

            double c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
            double distance = radius * c * 1000;

            return distance; 
        }

        //Check for empty clusters
        private bool EmptyClusters() 
        {
            //Check for empty clusters
            var clusterGroupsEmpty = locationData.GroupBy(s => s.cluster).OrderBy(s => s.Key)
                .Select(g => new {Cluster = g.Key, Count = g.Count()});
            foreach (var item in clusterGroupsEmpty)
            {
                if (item.Count == 0)
                    return true;
            }

            return false;
        }

        private class DataPoint
        {
            public double longitude { get; set; }
            public double latitude { get; set; }
            public int cluster { get; set; }
        }
    }
}