﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Diagnostics;

namespace SESDAD {

    public class RemotePuppetMaster : MarshalByRefObject, IPuppetMaster
    {
        public void Log(string message)
        {
            Console.WriteLine(message);
        }
    }


    class PuppetMaster {

        static Dictionary<String, IPuppetSubscriber> subscribers = new Dictionary<String, IPuppetSubscriber>();
        static Dictionary<String, IPuppetPublisher> publishers = new Dictionary<String, IPuppetPublisher>();
        static Dictionary<String, IPuppetBroker> brokers = new Dictionary<String, IPuppetBroker>();

        static void Main( string[] args ) {
            TcpChannel channel = new TcpChannel( 8080 );
            ChannelServices.RegisterChannel( channel, true );

            FileParsing.ConfigurationData config = null;
            try {
                config = FileParsing.ConfigurationFile.ReadConfigurationFile( @"config.txt" );
            }
            catch ( Exception e ) {
                Console.WriteLine( "Error reading configuration file." );
                Console.ReadLine();
                return;
            }

            if ( config == null ) {
                Console.WriteLine( "Error." );
                Console.ReadLine();
                return;
            }
            Console.WriteLine( "Creating processes.." );

            foreach ( FileParsing.Process processData in config.processes ) {
                Process newProcess = new Process();

                if ( processData.type == FileParsing.ProcessType.Broker ) {
                    // Configure the process using the StartInfo properties.
                    newProcess.StartInfo.FileName = @"..\..\..\Broker\bin\Debug\Broker.exe";

                    // Como vamos ter que criar o canal de TCP com o porto, passamos aqui o endereco
                    newProcess.StartInfo.Arguments = processData.port + " " + processData.serviceName;
                    newProcess.Start();

                    IPuppetBroker obj = (IPuppetBroker)Activator.GetObject(
                                              typeof(IPuppetBroker),
                                              processData.url);

                    PuppetMaster.brokers.Add(processData.name, obj);

                }
                else if ( processData.type == FileParsing.ProcessType.Publisher ) {
                    // Configure the process using the StartInfo properties.
                    newProcess.StartInfo.FileName = @"..\..\..\Publisher\bin\Debug\Publisher.exe";

                    // Como vamos ter que criar o canal de TCP com o porto, passamos aqui o endereco
                    newProcess.StartInfo.Arguments = processData.port + " " + processData.serviceName;
                    newProcess.Start();

                    IPuppetPublisher obj = (IPuppetPublisher)Activator.GetObject(
                                              typeof(IPuppetPublisher),
                                              processData.url);

                    PuppetMaster.publishers.Add(processData.name, obj);

                }
                else if ( processData.type == FileParsing.ProcessType.Subscriber ) {
                    // Configure the process using the StartInfo properties.
                    newProcess.StartInfo.FileName = @"..\..\..\Subscriber\bin\Debug\Subscriber.exe";

                    // Como vamos ter que criar o canal de TCP com o porto, passamos aqui o endereco
                    newProcess.StartInfo.Arguments = processData.port + " " + processData.serviceName;
                    newProcess.Start();

                    IPuppetSubscriber obj = (IPuppetSubscriber)Activator.GetObject(
                                               typeof(IPuppetSubscriber),
                                               processData.url);

                    PuppetMaster.subscribers.Add(processData.name, obj);
                }

            }

            foreach (FileParsing.Process processData in config.processes)
            {
                if (processData.type == FileParsing.ProcessType.Broker)
                {
                    FileParsing.Site site;
                    site = processData.GetSite();

                    IPuppetBroker obj;

                    brokers.TryGetValue(processData.name, out obj);
                    if (obj != null)
                    {
                        if(site.parent != null)
                        {

                                FileParsing.Process parentData = site.parent.broker;
                                //Regista pai no filho
                                String parentUrl = parentData.url;

                                obj.RegisterParent(parentUrl);

                                //Regista filho no pai

                                String parentName = parentData.name;

                                IPuppetBroker objParent;

                                brokers.TryGetValue(parentName, out objParent);
                                if (objParent != null)
                                {
                                    objParent.RegisterChild(processData.url);
                                }
                        }

                        foreach (FileParsing.Process subProcess in site.subscribers)
                        {
                            obj.RegisterSubscriber(subProcess.url);
                        }

                        foreach (FileParsing.Process pubProcess in site.publishers)
                        {
                            obj.RegisterPublisher(pubProcess.url);
                        }
                    }

                }

                if (processData.type == FileParsing.ProcessType.Subscriber)
                {
                    FileParsing.Site site;
                    site = processData.GetSite();

                    IPuppetSubscriber obj;

                    subscribers.TryGetValue(processData.name, out obj);
                    if (obj != null)
                    {
                        obj.RegisterBroker(site.broker.url);
                    }

                }

                if (processData.type == FileParsing.ProcessType.Publisher)
                {
                    FileParsing.Site site;
                    site = processData.GetSite();

                    IPuppetPublisher obj;

                    publishers.TryGetValue(processData.name, out obj);
                    if (obj != null)
                    {
                        obj.RegisterBroker(site.broker.url);
                    }

                }

            }


            System.Console.WriteLine( "Hi, I'm a puppet master..." );

            IPuppetPublisher pub;
            publishers.TryGetValue("publisher0", out pub);
            pub.ForcePublish(0,"asdf",0);

            Console.WriteLine( "Enter commands: " );
            string command = "";
            while ( command != "exit" ) {
                Console.Write( "> " );
                command = System.Console.ReadLine();
                ProcessCommand( command );
            }
        }

        static void ProcessCommand( string commandLine ) {
            FileParsing.ScriptEventQueue commands = null;

            try {
                commands = FileParsing.PuppetScript.ReadCommand( commandLine );
            }
            catch ( Exception e ) {
                Console.WriteLine( "Error parsing comand" );
            }

            if ( commands != null ) {

                if ( commands.Count() == 0 ) {
                    Console.WriteLine( "Unknown command." );
                }
                while ( !commands.Empty() ) {
                    var command = commands.GetNextCommand();

                    // TODO: Implement
                    if ( command.type == FileParsing.CommandType.Subscribe ) {
                    }
                    else if ( command.type == FileParsing.CommandType.Unsubscribe ) {
                    }
                    else if ( command.type == FileParsing.CommandType.Publish ) {
                    }
                    else if ( command.type == FileParsing.CommandType.Status ) {
                    }
                    else if ( command.type == FileParsing.CommandType.Crash ) {
                    }
                    else if ( command.type == FileParsing.CommandType.Freeze ) {
                    }
                    else if ( command.type == FileParsing.CommandType.Unfreeze ) {
                    }
                    else if ( command.type == FileParsing.CommandType.Wait ) {
                    }

                    // Test prints TODO: Remove
                    /*Console.Write( "Command: " + command.type.ToString() );
                    Console.Write( " Properties: " );
                    if ( command.properties != null ) {
                        foreach ( string prop in command.properties ) {
                            Console.Write( prop + " " );
                        }
                    }
                    Console.WriteLine();*/
                } // End of while
            } // End of 'if null'
            else {
                Console.WriteLine( "Unrecognized command." );
            }
        } // End of function
    } // End of class
} // End of namespace

