using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SESDAD {
    namespace FileParsing {

        public enum CommandType { Invalid, Subscribe, Unsubscribe, Publish, Status, Crash, Freeze, Unfreeze, Wait, Script }

        public struct ScriptCommandDescription {
            public CommandType type;
            public string[] properties;
        }

        public class ScriptEventQueue {
            Queue<ScriptCommandDescription> commands = new Queue<ScriptCommandDescription>();

            public bool Empty() { return commands.Count == 0; }

            public int Count() { return commands.Count; }

            public ScriptCommandDescription GetNextCommand() { return commands.Dequeue(); }

            public void Concatenate( ScriptEventQueue se ) {
                foreach ( ScriptCommandDescription command in se.commands ) {
                    AddCommand( command );
                }
            }
            public void AddCommand( ScriptCommandDescription scd ) {
                commands.Enqueue( scd );
            }
        }

        public static class PuppetScript {


            public static ScriptEventQueue ReadCommand( string command ) {
                ScriptEventQueue queue = new ScriptEventQueue();

                ScriptCommandDescription description = new ScriptCommandDescription();
                description.type = CommandType.Invalid;

                if ( command.StartsWith( "Subscriber" ) ) {
                    Regex pattern = new Regex( @"Subscriber (?<pname>\w+) (?<type>Subscribe|Unsubscribe) (?<topic>\w+)" );
                    Match match = pattern.Match( command );
                    if ( match.Success ) {
                        if ( match.Groups[ "type" ].Value == "Subscribe" ) {
                            description.type = CommandType.Subscribe;
                        }
                        else {
                            description.type = CommandType.Unsubscribe;
                        }

                        description.properties = new string[ 2 ];
                        description.properties[ 0 ] = match.Groups[ "pname" ].Value;
                        description.properties[ 1 ] = match.Groups[ "topic" ].Value;

                        queue.AddCommand( description );
                    }
                }
                else if ( command.StartsWith( "Publisher" ) ) {
                    Regex pattern = new Regex( @"Publisher (?<pname>\w+) Publish (?<numevents>\d+) Ontopic (?<topic>\w+) Interval (?<interval>\d+)" );
                    Match match = pattern.Match( command );
                    if ( match.Success ) {
                        description.type = CommandType.Publish;

                        description.properties = new string[ 4 ];
                        description.properties[ 0 ] = match.Groups[ "pname" ].Value;
                        description.properties[ 1 ] = match.Groups[ "numevents" ].Value;
                        description.properties[ 2 ] = match.Groups[ "topic" ].Value;
                        description.properties[ 3 ] = match.Groups[ "interval" ].Value;

                        queue.AddCommand( description );
                    }
                }
                else if ( command.StartsWith( "Status" ) ) {
                    /*Status for all nodes - no parameters*/
                    description.type = CommandType.Status;
                    description.properties = null;
                    queue.AddCommand( description );
                }
                else if ( command.StartsWith( "Crash" ) ) {
                    Regex pattern = new Regex( @"Crash (?<pname>.*)" );
                    Match match = pattern.Match( command );
                    if ( match.Success ) {
                        description.type = CommandType.Crash;

                        description.properties = new string[ 1 ];
                        description.properties[0 ] = match.Groups[ "pname" ].Value;
                        queue.AddCommand( description );
                    }
                }
                else if ( command.StartsWith( "Freeze" ) ) {
                    Regex pattern = new Regex( @"Freeze (?<pname>.*)" );
                    Match match = pattern.Match( command );
                    if ( match.Success ) {
                        description.type = CommandType.Freeze;

                        description.properties = new string[ 1 ];
                        description.properties[ 0 ] = match.Groups[ "pname" ].Value;
                        queue.AddCommand( description );
                    }
                }
                else if ( command.StartsWith( "Unfreeze" ) ) {
                    Regex pattern = new Regex( @"Unfreeze (?<pname>.*)" );
                    Match match = pattern.Match( command );
                    if ( match.Success ) {
                        description.type = CommandType.Unfreeze;

                        description.properties = new string[ 1 ];
                        description.properties[ 0 ] = match.Groups[ "pname" ].Value;
                        queue.AddCommand( description );
                    }
                }
                else if ( command.StartsWith( "Wait" ) ) {
                    Regex pattern = new Regex( @"Wait (?<time>\d+)" );
                    Match match = pattern.Match( command );
                    if ( match.Success ) {
                        description.type = CommandType.Wait;

                        description.properties = new string[ 1 ];
                        description.properties[ 0 ] = match.Groups[ "time" ].Value;
                        queue.AddCommand( description );
                    }
                }
                else if ( command.StartsWith( "Script" ) ) {
                    Regex pattern = new Regex( @"Script (?<filename>.*)" );
                    Match match = pattern.Match( command );
                    if ( match.Success ) {
                        /*description.type = CommandType.Script;

                        description.properties = new string[ 1 ];
                        description.properties[ 0 ] = match.Groups[ "filename" ].Value;*/
                        ScriptEventQueue fileEvents = null;
                        try {
                            fileEvents = ReadScriptFile( match.Groups[ "filename" ].Value );
                        }
                        catch ( Exception e ) {
                            /* Do nothing */
                        }

                        if ( fileEvents != null ) {
                            queue.Concatenate( fileEvents );
                        }
                    }
                }

                return queue;
            }
            
            public static ScriptEventQueue ReadScriptFile( string filename ) {
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

                ScriptEventQueue commands = new ScriptEventQueue();
                foreach ( string line in lines ) {
                    try {
                        var command = ReadCommand( line );
                        if ( command != null ) {
                            commands.Concatenate( command );
                        }
                    }
                    catch ( Exception e ) {
                        Console.Write( "Exception from reading command in file." );
                        /*DO NOTHING*/
                    }
                }

                return commands;
            }
        }
    }
}
