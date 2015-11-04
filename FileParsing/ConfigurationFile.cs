using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SESDAD {
    namespace FileParsing {
        public enum LoggingLevel { Full, Light }
        public enum RoutingPolicy { Flooding, Filter }
        public enum Ordering { No, Fifo, Total }
        public enum ProcessType { Subscriber, Publisher, Broker }

        public class Site {
            public string name { get; set; }
            public Site parent = null;
            public Process broker;
            public List<Process> subscribers = new List<Process>();
            public List<Process> publishers = new List<Process>();

            public Site( string name, Site parent = null ) {
                this.name = name;
                this.parent = parent;
            }
        }

        public class Process {
            public string name { get; }

            public string url { get; }
            public string ip { get; }
            public string port { get; }
            public string serviceName { get; }

            Site site = null;
            public ProcessType type { get; }

            public Process( string name, string url, Site site, ProcessType type ) {
                this.name = name;
                this.url = url;
                this.site = site;
                this.type = type;

                Regex pattern = new Regex( @"tcp://(?<ip>.*):(?<port>\d+)/(?<servicename>\w+)" );
                Match match = pattern.Match( url );
                if ( match.Success ) {
                    ip = match.Groups[ "ip" ].Value;
                    port = match.Groups[ "port" ].Value;
                    serviceName = match.Groups[ "servicename" ].Value;
                }
            }

            public Site GetSite() { return site; }
        }

        public class ConfigurationData {
            LoggingLevel logging;
            RoutingPolicy routing;
            Ordering ordering;
            public List<Site> sites = new List<Site>();
            public List<Process> processes = new List<Process>();

            public ConfigurationData( LoggingLevel logging = LoggingLevel.Light, RoutingPolicy routing = RoutingPolicy.Flooding, Ordering ordering = Ordering.Fifo ) {
                this.logging = logging;
                this.routing = routing;
                this.ordering = ordering;
            }

            public LoggingLevel GetLoggingLevel() { return logging; }
            public RoutingPolicy GetRoutingPolicy() { return routing; }
            public Ordering GetOrdering() { return ordering; }

            // Add site
            public void AddSite( Site site ) { sites.Add( site ); }
            public void AddProcess( Process process ) { processes.Add( process ); }
            // Add process
        }

        public static class ConfigurationFile {
            public static ConfigurationData ReadConfigurationFile( string filename ) {
                string[] lines = null;

                // Read file
                try {
                    lines = System.IO.File.ReadAllLines( filename );
                }
                catch ( FileNotFoundException fnfe ) {
                    Console.WriteLine( "Configuration file not found." );
                    return null;
                }
                catch ( Exception e ) {
                    Console.WriteLine( "Error while reading from configuration file." );
                    return null;
                }
                
                // Parse content

                // Logging Level
                LoggingLevel level = LoggingLevel.Light;

                // Routing policy
                RoutingPolicy routing = RoutingPolicy.Flooding;

                // Ordering
                Ordering ordering = Ordering.Fifo;

                List<Site> sites = new List<Site>();
                List<Process> processes = new List<Process>();

                foreach ( string line in lines ) {
                    if ( line.StartsWith( "LoggingLevel" ) ) {
                        Regex pattern = new Regex( @"LoggingLevel (?<type>(full|light))" );
                        Match match = pattern.Match( line );
                        if ( match.Success ) {
                            string type = match.Groups[ "type" ].Value;
                            level = (type == "full" ? LoggingLevel.Full : LoggingLevel.Light);
                        }
                    }
                    else if ( line.StartsWith( "RoutingPolicy" ) ) {
                        Regex pattern = new Regex( @"RoutingPolicy (?<type>(flooding|filter))" );
                        Match match = pattern.Match( line );
                        if ( match.Success ) {
                            string type = match.Groups[ "type" ].Value;
                            routing = (type == "flooding" ? RoutingPolicy.Flooding : RoutingPolicy.Filter);
                        }
                    }
                    else if ( line.StartsWith( "Ordering" ) ) {
                        Regex pattern = new Regex( @"Ordering (?<type>(NO|FIFO|TOTAL))" );
                        Match match = pattern.Match( line );
                        if ( match.Success ) {
                            string type = match.Groups[ "type" ].Value;
                            if ( type == "fifo" ) { ordering = Ordering.Fifo; }
                            else if ( type == "total" ) { ordering = Ordering.Total; }
                            else if ( type == "no" ) { ordering = Ordering.No; }
                        }
                    }
                    else if ( line.StartsWith( "Site" ) ) {
                        Regex pattern = new Regex( @"Site (?<name>\w+) Parent (?<parent>(none|\w+))" );
                        Match match = pattern.Match( line );
                        if ( match.Success ) {
                            string name = match.Groups[ "name" ].Value;
                            string parent = match.Groups[ "parent" ].Value;

                            Site parentSite = null;
                            if ( parent != "none" ) {
                                parentSite = sites.Find( n => n.name == parent );
                            }
                            sites.Add( new Site( name, parentSite ) );
                        }
                    }
                    else if ( line.StartsWith( "Process" ) ) {
                        Regex pattern = new Regex( @"Process (?<name>\w+) [Ii][Ss] (?<type>(broker|publisher|subscriber)) On (?<site>\w+) URL (?<url>[\w.:/-]+)" );
                        Match match = pattern.Match( line );

                        if ( match.Success ) {
                            string name = match.Groups[ "name" ].Value;
                            string type = match.Groups[ "type" ].Value;
                            string sitename = match.Groups[ "site" ].Value;
                            string url = match.Groups[ "url" ].Value;

                            ProcessType pType = ProcessType.Broker;
                            if ( type == "broker" ) { pType = ProcessType.Broker; }
                            else if ( type == "publisher" ) { pType = ProcessType.Publisher; }
                            else if ( type == "subscriber" ) { pType = ProcessType.Subscriber; }

                            Site site = sites.Find( n => n.name == sitename );
                            if ( site == null ) { return null; }

                            Process process = new Process(name, url, site, pType);
                            processes.Add( process);

                            if (type == "broker") { site.broker = process; }
                            else if (type == "publisher") { site.publishers.Add(process); }
                            else if (type == "subscriber") { site.subscribers.Add(process);  }
                        }
                    }
                }

                ConfigurationData data = new ConfigurationData( level, routing, ordering );

                foreach ( Site site in sites ) {
                    data.AddSite( site );
                }

                foreach ( Process process in processes ) {
                    data.AddProcess( process );
                }

                return data;
            }
        }

    }
}
