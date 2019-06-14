using System;
using System.Collections.Generic;
using System.Linq;
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
            DBAccess.ReloadLoadCsv();
        }

        //Road and parse CSV inte database
        private void ButtonParse_Click(object sender, RoutedEventArgs e)
        {
            DBAccess.ReloadLoadCsv();
        }

        //List Most popular domains.
        private void Domains_Click(object sender, RoutedEventArgs e)
        {
            Dictionary<String, int> dict = new Dictionary<string, int>(); 
            DBAccess.GetEmails(dict);

            var sortedDict = from entry in dict orderby entry.Value descending select entry;
            Console.WriteLine(sortedDict.ElementAt(0));
            Console.WriteLine(sortedDict.ElementAt(1));
            Console.WriteLine(sortedDict.ElementAt(2));
            Console.WriteLine(sortedDict.ElementAt(3));
            Console.WriteLine(sortedDict.ElementAt(4));
        }

        //Ability to cache data in database to refrain from overloading the api and speeding data fetching time.
        private void Cache_Click(object sender, RoutedEventArgs e)
        {
            List<String> postals = new List<string>();
            DBAccess.GetPostals(postals);
            DBAccess.CacheLocation(postals);
            int a = 1;


        }
    }
}
