using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Windows;

namespace ukCodeSample
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
            //Reload CSV if not cached in DB
            if (DBAccess.CheckCacheCsv())
            {
                DBAccess.ReloadLoadCsv();
            }
            else
            {
                DBAccess.LoadFromDB();
            }
        }

        //Road and parse CSV inte database
        private void ButtonParse_Click(object sender, RoutedEventArgs e)
        {
            Thread thread = new Thread(DBAccess.ReloadLoadCsv);
            thread.Start();
        }

        //List Most popular domains.
        private void Domains_Click(object sender, RoutedEventArgs e)
        {
            Dictionary<String, int> dict = new Dictionary<string, int>(); 
            DBAccess.GetEmails(dict);

            var sortedDict = from entry in dict orderby entry.Value descending select entry;
            domainBox.ItemsSource = sortedDict;
        }

        //Ability to cache data in database to refrain from overloading the api and speeding data fetching time.
        private void Cache_Click(object sender, RoutedEventArgs e)
        {
            Thread t = new Thread(Cache);
            t.Start();
        }

        private void Cache()
        {
            this.Dispatcher.Invoke(new Action(() =>
            {
                textBlockLoading.Text = "Loading...";
            }));
            List<String> postals = new List<string>();
            DBAccess.GetPostals(postals);
            DBAccess.CacheLocation(postals);
            this.Dispatcher.Invoke(new Action(() =>
            {
                textBlockLoading.Text = "";
            }));
        }

        /*
         * Realized that this is overkill for sure.
         * But will leave it in as extra data, since it works well with the concept of a localization application.
         * The app could be used to find most densely populated regions as well as the most densely populated geographical areas.
         * To really make use of this data though an appropriate visualization of the data would be needed, like plotting the different clusters
         * with different colors on a map over the UK.
         * Also handles all postcodes since terminated postcodes still provide valid long lat positions.
         */
        private void ClusterKmeans_Click(object sender, RoutedEventArgs e)
        {
            Thread t = new Thread(RunKmeans);
            t.Start();
        }

        private void RunKmeans()
        {
            List<String> load = new List<string>();
            load.Add("Loading...");
            this.Dispatcher.Invoke(new Action(() =>
            {
                kmeansTable.ItemsSource = load;
            }));
            if (DBAccess.CheckCacheLocation())
            {
                List<String> postals = new List<string>();
                DBAccess.GetPostals(postals);
                DBAccess.CacheLocation(postals);
            }
            List<DBAccess.Location> l = DBAccess.GetLocations();
            Kmeans cluster = new Kmeans(l);
            List<String> clusters = cluster.Cluster();

            this.Dispatcher.Invoke(new Action(() =>
            {
                kmeansTable.ItemsSource = clusters;
            }));
        }

        //Cluster entries by region
        private void ClusterRegion_Click(object sender, RoutedEventArgs e)
        {
            Thread t = new Thread(ClusterRegion);
            t.Start();
        }

        private void ClusterRegion()
        {
            List<String> load = new List<string>();
            load.Add("Loading...");
            if (DBAccess.CheckCacheLocation())
            {
                this.Dispatcher.Invoke(new Action(() =>
                {
                    RegionTable.ItemsSource = load;
                }));
                List<String> postals = new List<string>();
                DBAccess.GetPostals(postals);
                DBAccess.CacheLocation(postals);
            }

            Dictionary<String, int> dict = new Dictionary<string, int>();
            DBAccess.GetRegions(dict);
            var sortedDict = from entry in dict orderby entry.Value descending select entry;
            this.Dispatcher.Invoke(new Action(() =>
            {
                RegionTable.ItemsSource = sortedDict;
            }));
        }
    }
}
