using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.RegularExpressions;

namespace SESDAD {
    namespace UnitTesting {
        class ConfigFileTest {
            static void Main( string[] args ) {
                //TestConfigurationFile();

                TestScriptFile();
            }

            static void TestConfigurationFile() {
                FileParsing.ConfigurationData config = null;
                try {
                    config = FileParsing.ConfigurationFile.ReadConfigurationFile( "Config.txt" );
                }
                catch ( Exception e ) {
                    Console.WriteLine( "Something went wrong." );
                }

                if ( config != null ) {
                    Console.WriteLine( "Success reading config file." );

                    Console.WriteLine( "Logging: " + config.GetLoggingLevel().ToString() );
                    Console.WriteLine( "Routing: " + config.GetRoutingPolicy().ToString() );
                    Console.WriteLine( "Ordering: " + config.GetOrdering().ToString() );

                    foreach ( FileParsing.Site site in config.sites ) {
                        Console.WriteLine( "Site: " + site.name + " Parent: " + (site.parent == null ? "NONE" : site.parent.name) );
                    }

                    foreach ( FileParsing.Process process in config.processes ) {
                        Console.WriteLine( "Process: " + process.name + " URL: " + process.url + " Site: " + process.GetSite().name + " Type: " + process.type.ToString() );
                        Console.WriteLine( "    url: " + process.ip + "  " + process.serviceName + "  " + process.port );
                    }

                }
                else {
                    Console.WriteLine( "Failed to read config file." );
                }

                Console.WriteLine( "End of Configuration file test." );
                Console.ReadLine();
            }

            static void TestScriptFile() {
                FileParsing.ScriptEventQueue commands = null;
                try {
                    commands = FileParsing.PuppetScript.ReadScriptFile( "Script.txt" );
                }
                catch ( Exception e ) {
                    Console.WriteLine( "Something went wrong." );
                }

                if ( commands != null ) {
                    Console.WriteLine( "Success reading script file." );

                    while ( !commands.Empty() ) {
                        var command = commands.GetNextCommand();
                        Console.Write( "Command: " + command.type.ToString() );
                        Console.Write( " Properties: " );
                        if ( command.properties != null ) {
                            foreach ( string prop in command.properties ) {
                                Console.Write( prop + " " );
                            }
                        }
                        Console.WriteLine();
                    }
                }
                else {
                    Console.WriteLine( "Failed to read script file." );
                }

                Console.WriteLine( "End of Script file test." );
                Console.ReadLine();
            }
        }
    }
}
