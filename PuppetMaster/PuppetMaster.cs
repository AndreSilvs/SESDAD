using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Messaging;
using System.Diagnostics;

namespace SESDAD {

    public delegate void PuppetPublishDelegate( int numEvents, string topicname, int interval );

    public delegate void PuppetSubscribeDelegate( string topic );

    public class RemotePuppetMaster : MarshalByRefObject, IPuppetMaster
    {
        public void Log(string message)
        {
            PuppetMaster.LogMessage( message );
        }

        public void CreateBroker(string args)
        {
            Process newProcess = new Process();

            // Configure the process using the StartInfo properties.
            newProcess.StartInfo.FileName = @"..\..\..\Broker\bin\Debug\Broker.exe";

            // Como vamos ter que criar o canal de TCP com o porto, passamos aqui o endereco
            newProcess.StartInfo.Arguments = args;
            newProcess.Start();
        }

        public void CreatePublisher(string args)
        {
            Process newProcess = new Process();

            // Configure the process using the StartInfo properties.
            newProcess.StartInfo.FileName = @"..\..\..\Publisher\bin\Debug\Publisher.exe";

            // Como vamos ter que criar o canal de TCP com o porto, passamos aqui o endereco
            newProcess.StartInfo.Arguments = args;
            newProcess.Start();
        }

        public void CreateSubscriber(string args)
        {
            Process newProcess = new Process();

            // Configure the process using the StartInfo properties.
            newProcess.StartInfo.FileName = @"..\..\..\Subscriber\bin\Debug\Subscriber.exe";

            // Como vamos ter que criar o canal de TCP com o porto, passamos aqui o endereco
            newProcess.StartInfo.Arguments = args;
            newProcess.Start();
        }
    }


    class PuppetMaster {

        private static object mutex = new object();
        public static void LogMessage( string message ) {
            lock ( mutex ) {
                string fullMessage = message + "\r\n";
                System.IO.File.AppendAllText( @"log.txt", fullMessage );
            }
            //Console.WriteLine( fullMessage );
        }

        public static void PuppetPublishCallback( IAsyncResult ar ) {
            PuppetPublishDelegate del = (PuppetPublishDelegate)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke( ar );
            return;
        }

        public static void PuppetSubscribeCallback( IAsyncResult ar ) {
            PuppetSubscribeDelegate del = (PuppetSubscribeDelegate)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke( ar );
            return;
        }

        static string pmAddress = "tcp://localhost:30000/puppet";

        static Dictionary<String, IPuppetSubscriber> subscribers = new Dictionary<String, IPuppetSubscriber>();
        static Dictionary<String, IPuppetPublisher> publishers = new Dictionary<String, IPuppetPublisher>();
        static Dictionary<String, IPuppetBroker> brokers = new Dictionary<String, IPuppetBroker>();
        static Dictionary<String, IPuppetProcess> processes = new Dictionary<String, IPuppetProcess>();

        static void Main( string[] args ) {
            System.IO.File.WriteAllText( @"log.txt", string.Empty );

            TcpChannel channel = new TcpChannel( 30000 );
            ChannelServices.RegisterChannel( channel, true );

            RemotingConfiguration.RegisterWellKnownServiceType(
              typeof(RemotePuppetMaster),
              "puppet",
              WellKnownObjectMode.Singleton);

            FileParsing.ConfigurationData config = null;
            try {
                config = FileParsing.ConfigurationFile.ReadConfigurationFile( @"../../config.txt" );
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
                //Process newProcess = new Process();

                if ( processData.type == FileParsing.ProcessType.Broker ) {
                    // Configure the process using the StartInfo properties.
                    //newProcess.StartInfo.FileName = @"..\..\..\Broker\bin\Debug\Broker.exe";

                    // Como vamos ter que criar o canal de TCP com o porto, passamos aqui o endereco
                    //newProcess.StartInfo.Arguments = processData.port + " " + processData.serviceName + " " + processData.name;
                    //newProcess.Start();

                    string arguments = processData.port + " " + processData.serviceName + " " + processData.name + " " + config.GetOrdering() + " " + config.GetRoutingPolicy() + " " + config.GetLoggingLevel();

                    //Puppetmaster com o ip do processo
                    IPuppetMaster pup = (IPuppetMaster)Activator.GetObject(
                                                typeof(IPuppetMaster),
                                                "tcp://" + processData.ip + ":30000/puppet");

                    pup.CreateBroker(arguments);

                    IPuppetBroker obj = (IPuppetBroker)Activator.GetObject(
                                              typeof(IPuppetBroker),
                                              processData.url);

                    PuppetMaster.brokers.Add(processData.name, obj);

                }
                else if ( processData.type == FileParsing.ProcessType.Publisher ) {
                    // Configure the process using the StartInfo properties.
                    //newProcess.StartInfo.FileName = @"..\..\..\Publisher\bin\Debug\Publisher.exe";

                    // Como vamos ter que criar o canal de TCP com o porto, passamos aqui o endereco
                    //newProcess.StartInfo.Arguments = processData.port + " " + processData.serviceName + " " + processData.name;
                    //newProcess.Start();

                    string arguments = processData.port + " " + processData.serviceName + " " + processData.name;

                    //Puppetmaster com o ip do processo
                    IPuppetMaster pup = (IPuppetMaster)Activator.GetObject(
                                                typeof(IPuppetMaster),
                                                "tcp://" + processData.ip + ":30000/puppet");

                    pup.CreatePublisher(arguments);

                    IPuppetPublisher obj = (IPuppetPublisher)Activator.GetObject(
                                              typeof(IPuppetPublisher),
                                              processData.url);

                    PuppetMaster.publishers.Add(processData.name, obj);

                }
                else if ( processData.type == FileParsing.ProcessType.Subscriber ) {
                    // Configure the process using the StartInfo properties.
                    //newProcess.StartInfo.FileName = @"..\..\..\Subscriber\bin\Debug\Subscriber.exe";

                    // Como vamos ter que criar o canal de TCP com o porto, passamos aqui o endereco
                    //newProcess.StartInfo.Arguments = processData.port + " " + processData.serviceName + " " + processData.name;
                    //newProcess.Start();

                    string arguments = processData.port + " " + processData.serviceName + " " + processData.name;

                    //Puppetmaster com o ip do processo
                    IPuppetMaster pup = (IPuppetMaster)Activator.GetObject(
                                                typeof(IPuppetMaster),
                                                "tcp://" + processData.ip + ":30000/puppet");

                    pup.CreateSubscriber(arguments);

                    IPuppetSubscriber obj = (IPuppetSubscriber)Activator.GetObject(
                                               typeof(IPuppetSubscriber),
                                               processData.url);

                    PuppetMaster.subscribers.Add(processData.name, obj);
                }

                IPuppetProcess obj2 = (IPuppetProcess)Activator.GetObject(
                                              typeof(IPuppetProcess),
                                              processData.url);

                PuppetMaster.processes.Add(processData.name, obj2);

            }

            foreach (FileParsing.Process processData in config.processes)
            {
                IPuppetProcess objProcess;

                processes.TryGetValue(processData.name, out objProcess);
                if (objProcess != null)
                {
                    objProcess.RegisterPuppetMaster(pmAddress);
                }

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
                                    objParent.RegisterChild(processData.url, processData.name);
                                }
                        }

                        foreach (FileParsing.Process subProcess in site.subscribers)
                        {
                            obj.RegisterSubscriber(subProcess.url, subProcess.name );
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

                    if ( command.type == FileParsing.CommandType.Invalid ) {
                        continue;
                    }

                    LogMessage( command.fullInput );

                    // TODO: Implement all commands
                    if ( command.type == FileParsing.CommandType.Subscribe ) {
                        IPuppetSubscriber sub;
                        subscribers.TryGetValue( command.properties[ 0 ], out sub );
                        if ( sub != null ) {
                            //sub.ForceSubscribe( command.properties[ 1 ] );
                            PuppetSubscribeDelegate del = new PuppetSubscribeDelegate( sub.ForceSubscribe );
                            AsyncCallback remoteCallback = new AsyncCallback( PuppetSubscribeCallback );
                            IAsyncResult remAr = del.BeginInvoke( command.properties[ 1 ], remoteCallback, null );
                        }
                        else {
                            Console.WriteLine( "Invalid subscriber name: \"" + command.properties[ 0 ] + "\" Cannot process subscribe command." );
                        }
                    }
                    else if ( command.type == FileParsing.CommandType.Unsubscribe ) {
                        IPuppetSubscriber sub;
                        subscribers.TryGetValue( command.properties[ 0 ], out sub );
                        if ( sub != null ) {
                            //sub.ForceSubscribe( command.properties[ 1 ] );
                            PuppetSubscribeDelegate del = new PuppetSubscribeDelegate( sub.ForceUnsubscribe );
                            AsyncCallback remoteCallback = new AsyncCallback( PuppetSubscribeCallback );
                            IAsyncResult remAr = del.BeginInvoke( command.properties[ 1 ], remoteCallback, null );
                        }
                        else {
                            Console.WriteLine( "Invalid subscriber name: \"" + command.properties[ 0 ] + "\" Cannot process unsubscribe command." );
                        }
                    }
                    else if ( command.type == FileParsing.CommandType.Publish ) {
                        IPuppetPublisher pub;
                        publishers.TryGetValue(command.properties[0], out pub);

                        //pub.ForcePublish( Int32.Parse( command.properties[ 1 ] ), command.properties[ 2 ], Int32.Parse( command.properties[ 3 ] ) );
                        PuppetPublishDelegate del = new PuppetPublishDelegate( pub.ForcePublish );
                        AsyncCallback remoteCallback = new AsyncCallback( PuppetPublishCallback );
                        IAsyncResult remAr = del.BeginInvoke( Int32.Parse( command.properties[ 1 ] ), command.properties[ 2 ], Int32.Parse( command.properties[ 3 ] ), remoteCallback, null );
                    }
                    else if ( command.type == FileParsing.CommandType.Status ) {
                        foreach(var p in PuppetMaster.processes)
                        {
                            var obj = p.Value;
                            obj.Status();
                        }
                    }
                    else if ( command.type == FileParsing.CommandType.Crash ) {
                    }
                    else if ( command.type == FileParsing.CommandType.Freeze ) {
                    }
                    else if ( command.type == FileParsing.CommandType.Unfreeze ) {
                    }
                    else if ( command.type == FileParsing.CommandType.Wait ) {
                        int time = Int32.Parse( command.properties[ 0 ] );
                        Console.WriteLine( "Waiting: " + command.properties[ 0 ] + "ms" );
                        Thread.Sleep( time );
                        Console.WriteLine( "Waited." );
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

