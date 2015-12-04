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
using System.Threading;

namespace SESDAD
{


    class RemoteSubscriber : MarshalByRefObject, ISubscriber, IPuppetSubscriber, IPuppetProcess
    {
        public override object InitializeLifetimeService() {
            return null;
        }

        public void RegisterBroker(string address)
        {
            Subscriber.broker = (IBroker)Activator.GetObject(
               typeof( IBroker ),
               address );

            //Console.WriteLine("I have a ~broker");
        }
        public void RegisterBrokers( List<string> addresses ) {
            // Subscriber doesn't need to know the broker's name
            BrokerCircle brokerCircle = new BrokerCircle( "" );
            int id = 0;
            foreach ( string address in addresses ) {
                brokerCircle.AddBroker( (IBroker)Activator.GetObject( typeof( IBroker ), address ), id );
                id++;
            }

            Subscriber.brokerCircle = brokerCircle;
        }

        public void Crash()
        {
            Environment.Exit(0);
        }

        public void ForceSubscribe(string topicname)
        {
            Subscriber.Subscribe( topicname );
        }

        public void ForceUnsubscribe(string topicname)
        {
            Subscriber.Unsubscribe( topicname );
        }


        public void ReceiveContent(Event evt)
        {
            lock (Subscriber.monitorLock)
            {
                while (Subscriber.frozen)
                {
                    Monitor.Wait(Subscriber.monitorLock);
                }
            }

            if (Subscriber.topics.Contains(evt.Topic) || Subscriber.IsSubTopic( evt.Topic )){
                new Task(() => { Subscriber.puppetMaster.Log("SubEvent " + Subscriber.name + ", " + evt.PublisherName + ", " + evt.Topic + ", " + evt.TopicEventNum); }).Start();
                System.Console.WriteLine("Topic: " + evt.Topic + " Content: " + evt.Content + " " + evt.EventCounter);
                // Subscriber.puppetMaster.Log("SubEvent " + Subscriber.name + " thing.");
            }
        }

        public void InformNeighbourDeath( string circleName, int replicaId ) {
            Subscriber.brokerCircle.NewCircleLeader( replicaId );
        }


        public void Status()
        {
            Console.WriteLine("I'm " + Subscriber.name);
            if (Subscriber.frozen)
            {
                Console.WriteLine("Brrrr I'm freezing");

            }
            else
            {
                Console.WriteLine("I'm alive");
            }

            //Se os subscribers souberem as suas subscrições por aqui tambem
        }

        public void Freeze()
        {
            Subscriber.frozen = true;
        }

        public void Unfreeze()
        {
            lock (Subscriber.monitorLock)
            {
                Subscriber.frozen = false;
                Monitor.PulseAll(Subscriber.monitorLock);
            }
        }

        public void RegisterPuppetMaster(string address)
        {
            Subscriber.puppetMaster = (IPuppetMaster)Activator.GetObject(
                 typeof(IPuppetMaster),
                 address);

            //Console.WriteLine("I'm a puppet");
        }
        /*public void RegisterSequencer( string address ) {
            Subscriber.sequencer = (ISequencer)Activator.GetObject(
                typeof( ISequencer ),
                address );
        }*/
    }


    public delegate void SubscriberDelegate( string name, string topic );

    class Subscriber
    {
        static public IBroker broker;
        static public IPuppetMaster puppetMaster;
        //static public ISequencer sequencer;
        static public string name;

        // Replication
        static public BrokerCircle brokerCircle;

        static public bool frozen = false;

        static public object monitorLock = new object();

        static public List<string> topics = new List<string>();

        public static void SubscriberCallback( IAsyncResult ar ) {
            SubscriberDelegate del = (SubscriberDelegate)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke( ar );
            return;
        }

        public static bool IsSubTopic( string subTopic ) {
            foreach ( string topic in topics ) {
                if ( topic.EndsWith( "/*" ) ) {
                    string matchTopic = topic.Substring( 0, topic.Count() - 1 );
                    if ( subTopic.StartsWith( matchTopic ) ) {
                        return true;
                    }
                }
            }
            return false;
        }

        public static void Subscribe( string topic ) {
           // Console.WriteLine( "Subscribing to: " + topic );

            //broker.Subscribe( name, topic );

            // No replication
            //SubscriberDelegate del = new SubscriberDelegate( broker.Subscribe );

            // Replication
            SubscriberDelegate del = new SubscriberDelegate( brokerCircle.Subscribe );

            AsyncCallback remoteCallback = new AsyncCallback( SubscriberCallback );
            IAsyncResult remAr = del.BeginInvoke( name, topic, remoteCallback, null );

            Subscriber.topics.Add(topic);
        }

        public static void Unsubscribe( string topic ) {
            Console.WriteLine( "Unsubscribing from: " + topic );

            //broker.Unsubscribe( name, topic );
            //SubscriberDelegate del = new SubscriberDelegate( broker.Unsubscribe );
            SubscriberDelegate del = new SubscriberDelegate( brokerCircle.Unsubscribe );
            AsyncCallback remoteCallback = new AsyncCallback( SubscriberCallback );
            IAsyncResult remAr = del.BeginInvoke( name, topic, remoteCallback, null );

            Subscriber.topics.Remove(topic);
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
            Subscriber.name = args[2];

            BinaryServerFormatterSinkProvider provider = new BinaryServerFormatterSinkProvider();
            IDictionary props = new Hashtable();
            props[ "port" ] = port;
            props[ "timeout" ] = 10000; // 3 secs
            TcpChannel channel = new TcpChannel( props, null, provider );
            //TcpChannel channel = new TcpChannel(port);
            ChannelServices.RegisterChannel(channel, false);

            RemotingConfiguration.RegisterWellKnownServiceType(
              typeof(RemoteSubscriber),
              serviceName,
              WellKnownObjectMode.Singleton);

            /*IBroker obj = (IBroker)Activator.GetObject(
             typeof(IBroker),
             "tcp://localhost:8086/broker");*/


            //System.Console.WriteLine("Hi, I'm a subscriber...");
            System.Console.ReadLine();
        }
    }
}
