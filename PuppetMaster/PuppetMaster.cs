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

    public delegate void PuppetCrashDelegate();

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

        //static string pmAddress = "tcp://localhost:30000/puppet";
        static string pmAddress;

        static Dictionary<String, IPuppetSubscriber> subscribers = new Dictionary<String, IPuppetSubscriber>();
        static Dictionary<String, IPuppetPublisher> publishers = new Dictionary<String, IPuppetPublisher>();
        static Dictionary<String, IPuppetBroker> brokers = new Dictionary<String, IPuppetBroker>();
        static Dictionary<String, IPuppetProcess> processes = new Dictionary<String, IPuppetProcess>();
        static Dictionary<String, BrokerNode> brokerNodes = new Dictionary<string, BrokerNode>();

        static void Main( string[] args ) {
            System.IO.File.WriteAllText( @"log.txt", string.Empty );

            TcpChannel channel = new TcpChannel( 30000 );
            ChannelServices.RegisterChannel( channel, false );

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
            pmAddress = "tcp://" + config.PuppetMasterIP + ":30000/puppet";

            // Check what type of puppet master this process should be
            Console.WriteLine( "Should I be the Main or a Slave Puppet Master?" );
            Console.WriteLine( "    1 - Main PM" );
            Console.WriteLine( "    2 - Slave PM" );

            ConsoleKeyInfo keyInfo;
            int type = 0;
            while ( true ) {
                keyInfo = Console.ReadKey();
                Console.WriteLine();

                if ( keyInfo.KeyChar == '1' ) {
                    type = 0;
                    break;
                }
                else if ( keyInfo.KeyChar == '2' ) {
                    type = 1;
                    break;
                }
                else {
                    Console.Write( "Enter 1 or 2: " );
                }
            }

            if ( type == 1 ) {
                Console.WriteLine( "     .. Waiting for commands .." );
                Console.WriteLine( "====== PRESS ANY KEY TO QUIT ======" );
                Console.ReadLine();
                return;
            }

            // Main process master
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

                    PuppetMaster.brokers.Add( processData.name, obj );

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

            // Obter port mais alto para criar os brokers extra
            int maxPort = 0;
            foreach ( FileParsing.Process processData in config.processes ) {
                int port;
                if ( Int32.TryParse( processData.port, out port ) ) {
                    maxPort = System.Math.Max( maxPort, port );
                }
            }

            // Criar Brokers de backup
            foreach ( FileParsing.Process processData in config.processes ) {
                if ( processData.type == FileParsing.ProcessType.Broker ) {

                    //Puppetmaster com o ip do processo
                    IPuppetMaster pup = (IPuppetMaster)Activator.GetObject(
                                                typeof( IPuppetMaster ),
                                                "tcp://" + processData.ip + ":30000/puppet" );

                    // Criar o segundo e terceiro brokers a partir da informacao do primeiro broker
                    int port1 = maxPort + 1;
                    int port2 = maxPort + 2;
                    string arguments = port1.ToString() + " " + processData.serviceName + " " + processData.name + "_1" + " " + config.GetOrdering() + " " + config.GetRoutingPolicy() + " " + config.GetLoggingLevel();
                    pup.CreateBroker( arguments );
                    arguments = port2.ToString() + " " + processData.serviceName + " " + processData.name + "_2" + " " + config.GetOrdering() + " " + config.GetRoutingPolicy() + " " + config.GetLoggingLevel();
                    pup.CreateBroker( arguments );

                    // Prepare port for next iteration
                    maxPort += 2;

                    string broker1Url = "tcp://" + processData.ip + ":" + port1.ToString() + "/" + processData.serviceName;
                    string broker2Url = "tcp://" + processData.ip + ":" + port2.ToString() + "/" + processData.serviceName;

                    // Obter interfaces dos processos para adicionar aos dicionarios
                    IPuppetBroker broker_1 = (IPuppetBroker)Activator.GetObject(
                                              typeof( IPuppetBroker ),
                                              broker1Url  );
                    IPuppetBroker broker_2 = (IPuppetBroker)Activator.GetObject(
                                              typeof( IPuppetBroker ),
                                              broker2Url );

                    PuppetMaster.brokers.Add( processData.name + "_1", broker_1 );
                    PuppetMaster.brokers.Add( processData.name + "_2", broker_2 );

                    IPuppetProcess brokerP_1 = (IPuppetProcess)Activator.GetObject(
                                              typeof( IPuppetBroker ),
                                              broker1Url );
                    IPuppetProcess brokerP_2 = (IPuppetProcess)Activator.GetObject(
                                              typeof( IPuppetBroker ),
                                              broker2Url );

                    PuppetMaster.processes.Add( processData.name + "_1", brokerP_1 );
                    PuppetMaster.processes.Add( processData.name + "_2", brokerP_2 );

                    // Relacionar brokers de backup com o site e os brokers principais
                    FileParsing.Process newbrokerProcess1 = new FileParsing.Process( processData.name + "_1", broker1Url, processData.GetSite(), processData.type );
                    FileParsing.Process newbrokerProcess2 = new FileParsing.Process( processData.name + "_2", broker2Url, processData.GetSite(), processData.type );

                    // Join these processes in a BrokerNode
                    BrokerNode node = new BrokerNode();

                    IPuppetBroker originalBroker = brokers[ processData.name ];
                    node.AddBroker( originalBroker, processData );
                    node.AddBroker( broker_1, newbrokerProcess1 );
                    node.AddBroker( broker_2, newbrokerProcess2 );

                    node.site = processData.GetSite();

                    // Add this node under the name of the original node
                    brokerNodes.Add( processData.name, node );
                }
            }

            // Make each process known to its neighbours
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
                            String parentUrl = parentData.url;
                            String parentName = parentData.name;

                            // No replication
                                //Regista pai no filho
                                /*obj.RegisterChild( parentUrl, parentName );

                                //Regista filho no pai
                                IPuppetBroker objParent;
                                brokers.TryGetValue( parentName, out objParent );
                                if ( objParent != null ) {
                                    objParent.RegisterChild( processData.url, processData.name );
                                }*/

                            // Replication
                                BrokerNode parentNode = brokerNodes[ parentName ];
                                if ( parentNode != null ) {
                                    // Regista pai no filho    
                                    obj.RegisterChildReplication( parentNode.GetListOfAddresses(), parentName );
                                    obj.RegisterParentReplication( parentName );
                                 }

                                BrokerNode childNode = null;
                                brokerNodes.TryGetValue( processData.name, out childNode );
                                if ( childNode != null ) {
                                    //Regista filho no pai
                                    parentNode.brokers[0].RegisterChildReplication( childNode.GetListOfAddresses(), processData.name );
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

                    // No replication
                    /*if ( obj != null)
                    {
                        obj.RegisterBroker(site.broker.url);
                    }*/

                    // Replication - send the url of all Brokers in the node along with the original's name
                    BrokerNode node = brokerNodes[ site.broker.name ];
                    obj.RegisterBrokers( node.GetListOfAddresses() );
                }

                if (processData.type == FileParsing.ProcessType.Publisher)
                {
                    FileParsing.Site site;
                    site = processData.GetSite();

                    IPuppetPublisher obj;
                    publishers.TryGetValue(processData.name, out obj);

                    // No replication
                    /*if ( obj != null ) {
                        obj.RegisterBroker( site.broker.url );
                    }*/

                    // Replication - send the url of all Brokers in the node along with the original's name
                    BrokerNode node = brokerNodes[ site.broker.name ];
                    obj.RegisterBrokers( node.GetListOfAddresses() );
                }
            }

            // Ligar os brokers replica aos subscribers do site e brokers de outros sites
            foreach ( var node in PuppetMaster.brokerNodes ) {
                FileParsing.Site site = node.Value.site;
                Console.WriteLine( "Processing site " + site.name );
                if ( site.parent != null ) {
                    FileParsing.Process parentData = site.parent.broker;
                    String parentNodeName = parentData.name;

                    // No replication
                    //Regista pai no filho
                    /*obj.RegisterChild( parentUrl, parentName );

                    //Regista filho no pai
                    IPuppetBroker objParent;
                    brokers.TryGetValue( parentName, out objParent );
                    if ( objParent != null ) {
                        objParent.RegisterChild( processData.url, processData.name );
                    }*/

                    BrokerNode parentNode = brokerNodes[ parentNodeName ];

                    for ( int i = 1; i < 3; ++i ) {
                        // Replication
                        // Regista pai no filho    
                        node.Value.brokers[ i ].RegisterChildReplication( parentNode.GetListOfAddresses(), parentNodeName );
                        node.Value.brokers[ i ].RegisterParentReplication( parentNodeName );
                    }

                    //Regista filhos no pai
                    for ( int j = 1; j < 3; ++j ) {
                        parentNode.brokers[ j ].RegisterChildReplication( node.Value.GetListOfAddresses(), node.Value.site.broker.name );
                    }
                }

                for ( int i = 1; i < 3; ++i ) {
                    ((IPuppetProcess)node.Value.brokers[ i ]).RegisterPuppetMaster( pmAddress );

                    foreach ( FileParsing.Process subProcess in site.subscribers ) {
                        node.Value.brokers[ i ].RegisterSubscriber( subProcess.url, subProcess.name );
                    }

                    foreach ( FileParsing.Process pubProcess in site.publishers ) {
                        node.Value.brokers[ i ].RegisterPublisher( pubProcess.url );
                    }
                }
            }

            // Make each replicated broker known to its neighbours
            foreach (FileParsing.Process processData in config.processes)
            {
                if (processData.type == FileParsing.ProcessType.Broker)
                {
                    FileParsing.Site site;
                    site = processData.GetSite();

                    BrokerNode broker = brokerNodes[processData.name];

                    for(int i = 0; i<3; i++)
                    {
                        string url = broker.brokersData[i].url;
                        IPuppetBroker obj = broker.brokers[i];

                        obj.RegisterReplicas(broker.GetListOfAddressesExcept(url), processData.name, i);
                    }
                }
            }

            // Criar sequencer se for ordem Total
            if ( config.GetOrdering() == FileParsing.Ordering.Total ) {
                // Create Sequencer process
                Process seqProcess = new Process();

                // Configure the process using the StartInfo properties.
                seqProcess.StartInfo.FileName = @"..\..\..\Sequencer\bin\Debug\Sequencer.exe";

                seqProcess.Start();

                string seqUrl = "tcp://localhost:8999/seq";

                // Send sequencer url to all processes
                foreach ( var broker in brokers ) {
                    IPuppetBroker bro = broker.Value;
                    bro.RegisterSequencer( seqUrl );
                }
            }

            // Puppet Master main loop
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
                        IPuppetProcess proc;
                        processes.TryGetValue( command.properties[0], out proc );
                        if (proc != null) {
                            //proc.Crash();
                            PuppetCrashDelegate del = new PuppetCrashDelegate( proc.Crash );
                            AsyncCallback remoteCallback = new AsyncCallback( PuppetPublishCallback );
                            IAsyncResult remAr = del.BeginInvoke( remoteCallback, null );
                            //remove process after crashing it
                            processes.Remove(command.properties[0]);
                            subscribers.Remove(command.properties[0]);
                            publishers.Remove(command.properties[0]);
                            brokers.Remove(command.properties[0]);
                        }
                        else
                            Console.WriteLine("Invalid process name: \"" + command.properties[0] + "\" Cannot process crash command.");
                    }
                    else if ( command.type == FileParsing.CommandType.Freeze ) {
                        IPuppetProcess proc;
                        processes.TryGetValue(command.properties[0], out proc);
                        if (proc != null)
                        {
                            proc.Freeze();
                        }
                        else
                            Console.WriteLine("Invalid process name: \"" + command.properties[0] + "\" Cannot process freeze command.");

                    }
                    else if ( command.type == FileParsing.CommandType.Unfreeze ) {

                        IPuppetProcess proc;
                        processes.TryGetValue(command.properties[0], out proc);
                        if (proc != null)
                        {
                            proc.Unfreeze();
                        }
                        else
                            Console.WriteLine("Invalid process name: \"" + command.properties[0] + "\" Cannot process unfreeze command.");
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

