using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Messaging;
using System.Net.Sockets;
using System.Threading;

namespace SESDAD
{
    public delegate void PublishPuppetLog( string message );
    public delegate void PublishTopicDelegate( Event ev, String name );


    struct EventCounter {
        public int topicCounter;
        public int globalCounter;
    }

    class RemotePublisher : MarshalByRefObject, IPuppetPublisher, IPuppetProcess, IPublisher {

        public void RegisterBroker( string address ) {
            Publisher.broker = (IBroker)Activator.GetObject(
               typeof( IBroker ),
               address );

            Console.WriteLine("I have a ~broker");
        }

        public void ForcePublish( int numberEvents, string topicname, int interval_ms ) {
            Console.WriteLine( "Publishing: " + numberEvents.ToString() + " " + topicname + " " + interval_ms.ToString() );
            new Task(() => { Publisher.PublishEvents( numberEvents, topicname, interval_ms ); } ).Start();
        }

        public void Status() {
            Console.WriteLine("I'm " + Publisher.name);
            Console.WriteLine("I'm alive");
        }

        public void Crash() {
            Environment.Exit(0);
        }

        public void Freeze() {
            Publisher.frozen = true;
        }

        public void Unfreeze() {
            lock (Publisher.monitorLock)
            {
                Publisher.frozen = false;
                Monitor.PulseAll(Publisher.monitorLock);
            }
        }

        public void RegisterPuppetMaster(string address)
        {
            Publisher.puppetMaster = (IPuppetMaster)Activator.GetObject(
                typeof(IPuppetMaster),
                address);

            //Console.WriteLine("I'm a puppet");
        }

        /*public void RegisterSequencer( string address ) {
            Publisher.sequencer = (ISequencer)Activator.GetObject(
                typeof( ISequencer ),
                address );
        }*/
    }

    class Publisher
    {
        static public IBroker broker;
        static public string name;

        static int count = 0;
        static object lockObject = new object();

        static public IPuppetMaster puppetMaster;
        //static public ISequencer sequencer;

        static Dictionary<string, int> topicCount = new Dictionary<string, int>();

        static public bool frozen = false;

        static public object monitorLock = new object();

        public static void PublishAsyncCallBack( IAsyncResult ar ) {
            PublishTopicDelegate del = (PublishTopicDelegate)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke( ar );
            return;
        }

        public static void PublishLogCallBack( IAsyncResult ar ) {
            PublishPuppetLog del = (PublishPuppetLog)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke( ar );
            return;
        }

        public static void PublishEvents( int numberEvents, string topic, int interval_ms ) {
            for ( int i = 0; i < numberEvents; ++i ) {
                //Publisher.broker.SendContent(new Event(topicname,"banana"));
                PublishTopicDelegate del = new PublishTopicDelegate( Publisher.broker.SendContentPub );
                AsyncCallback remoteCallback = new AsyncCallback( PublishAsyncCallBack );
                EventCounter eCounter = getCountAndIncrement( topic );
                IAsyncResult remAr = del.BeginInvoke( new Event( topic, Publisher.name + "_" + eCounter.topicCounter, Publisher.name, eCounter.topicCounter, eCounter.globalCounter  ), Publisher.name, remoteCallback, null );

                //Publisher.puppetMaster.Log( "PubEvent" );
                PublishPuppetLog logDel = new PublishPuppetLog( Publisher.puppetMaster.Log );
                AsyncCallback remoteCallbackLog = new AsyncCallback( PublishLogCallBack );
                IAsyncResult remArLog = logDel.BeginInvoke( "PubEvent " + Publisher.name + ", " + topic + ", " + eCounter.topicCounter, remoteCallbackLog, null );

                Thread.Sleep( interval_ms );
            }
        }

        static EventCounter getCountAndIncrement( string topic )
        {
            EventCounter ec = new EventCounter();
            if ( !topicCount.ContainsKey( topic ) ) {
                topicCount.Add( topic, 0 );
            }
            lock (lockObject)
            {
                ec.globalCounter = count++;
                ec.topicCounter = topicCount[ topic ]++;
            }
            return ec;
        }

        static void Main(string[] args)
        {
            if ( args.Length != 3 ) {
                return;
            }

            foreach ( string arg in args ) {
                Console.WriteLine( "Arg: " + arg );
            }

            int port; Int32.TryParse( args[ 0 ], out port );
            string serviceName = args[ 1 ];
            Publisher.name = args[2];

            BinaryServerFormatterSinkProvider provider = new BinaryServerFormatterSinkProvider();
            IDictionary props = new Hashtable();
            props[ "port" ] = port;
            props[ "timeout" ] = 3000; // 3 secs
            TcpChannel channel = new TcpChannel( props, null, provider );
            //TcpChannel channel = new TcpChannel(port);
            ChannelServices.RegisterChannel(channel, true);

            RemotingConfiguration.RegisterWellKnownServiceType(
             typeof(RemotePublisher),
             serviceName,
             WellKnownObjectMode.Singleton);


            /*Publisher.broker = (IBroker)Activator.GetObject(
                typeof(IBroker),
                "tcp://localhost:8086/broker");

            broker.SendContent("banana");*/

            //System.Console.WriteLine("Hi, I'm a publisher...");
            System.Console.ReadLine();
        }
    }
}
