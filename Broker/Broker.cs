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
    class RemoteBroker : MarshalByRefObject, IBroker, IPuppetBroker, IPuppetProcess {

        public delegate void SendContentDelegate( Event ev );

        // Non-interface methods
        public static void PublishAsyncCallBack( IAsyncResult ar ) {
            SendContentDelegate del = (SendContentDelegate)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke( ar );
            return;
        }

        //PuppetMaster - register replica neighbours
        public void RegisterReplicas( List<string> addresses, string originalName, int id ) {
            Console.WriteLine( "Group name: " + originalName );
            Broker.groupName = originalName;
            Broker.replicationId = id;
            foreach ( string address in addresses ) {
                Broker.replicaBrokers.Add( (IBroker)Activator.GetObject( typeof( IBroker ), address ) );
            }
        }

        //PuppetMaster
        public void RegisterChild( string address, string name ) {
            Broker.children.Add( new NamedBroker( name, (IBroker)Activator.GetObject(
               typeof( IBroker ),
               address ) ) );

            //Console.WriteLine( "I have a kid" );
        }
        // Puppet Master
        public void RegisterChildReplication( List<string> addresses, string name ) {
            BrokerCircle brokerCircle = new BrokerCircle( name );
            foreach ( string address in addresses ) {
                brokerCircle.AddBroker( (IBroker)Activator.GetObject( typeof( IBroker ), address ) );
            }

            // Add circle to a list of neighbour circles
            Broker.neighbourBrokers.Add( brokerCircle );
        }

        public void RegisterParent( string address ) {

            /*Broker.parent = (IBroker)Activator.GetObject(
               typeof( IBroker ),
               address );*/

            //Console.WriteLine( "I have a parent" );
        }

        public void RegisterPublisher( string address ) {
            Broker.publishers.Add( (IPublisher)Activator.GetObject(
               typeof( IPublisher ),
               address ) );

           // Console.WriteLine( "I have a publisher" );
        }

        public void RegisterSubscriber( string address, string name ) {
            Broker.subscribers.Add( new NamedSubscriber( name, (ISubscriber)Activator.GetObject(
               typeof( ISubscriber ),
               address ) ) );

           // Console.WriteLine( "I have a subscriber" );
        }

        public void Status() {
            Console.WriteLine("I'm " + Broker.name);
            Console.WriteLine("I'm alive");
            Console.WriteLine("Subscriptions:");
            foreach (TopicSubscribers x in Broker.topicSubscribers.topicSubscribers)
            {
                Console.WriteLine(x.topic);
            }
            foreach (TopicBrokers x in Broker.topicBrokers.topicBrokers)
            {
                Console.WriteLine(x.topic);
            }
        }

        public void Crash() {
            Environment.Exit(0);
        }

        public void Freeze() {
            Broker.frozen = true;
        }

        public void Unfreeze() {
            lock (Broker.monitorLock) {
                Broker.frozen = false;
                Monitor.PulseAll(Broker.monitorLock);
            }
        }

        public void SendContentPub(Event evt, String name)
        {
            if (Broker.ordering == FileParsing.Ordering.Total)
            {
                evt.EventCounter = Broker.sequencer.GetNextSequenceNumber();
                //Console.WriteLine( "Sending event- " + "Seq: " + evt.EventCounter + " Topic: " + evt.TopicEventNum + " Pub: " + evt.PublisherName );
            }

            SendContent(evt, name);
        }

        //Broker
        public void SendContent(Event evt, String name)
        {
            new Task(() =>
            {
                lock (Broker.monitorLock)
                {
                    while (Broker.frozen)
                    {
                        Monitor.Wait(Broker.monitorLock);
                    }
                }

                if (Broker.ordering == FileParsing.Ordering.Fifo)
                {
                    lock (Broker.subscriptionMutex)
                    {
                        if (Broker.routing == FileParsing.RoutingPolicy.Filter)
                        {
                            // FILTERING: TOPIC EVENT COUNTER
                            Broker.publisherTopics.AddEvent(evt.PublisherName, evt);

                            PublisherTopicRegister pRegister = Broker.publisherTopics.GetPublisherTopic(evt.PublisherName);

                            foreach (Event orderedEvent in pRegister.GetLastOrderedEvents(evt.Topic))
                            {
                                //Console.WriteLine( "Send" );
                                Broker.SendContent(orderedEvent, name);

                                if (Broker.logging == FileParsing.LoggingLevel.Full)
                                {
                                    new Task(() => { Broker.puppetMaster.Log("BroEvent " + Broker.name + " " + orderedEvent.PublisherName + " " + orderedEvent.Topic + " " + orderedEvent.TopicEventNum); }).Start();
                                }
                            }
                        }
                        else
                        {
                            // FLOODING: EVENT COUNTER
                            Broker.publisherEvents.AddEvent(evt);

                            EventListFlooding eList = Broker.publisherEvents.GetEventList(evt.PublisherName);
                            //lock ( eList.mutex ) {
                            foreach (Event orderedEvent in eList.GetOrderedEventsUpToDate())
                            {
                                Broker.SendContent(orderedEvent, name);

                                // Console.WriteLine( "orderedEvent.EventCounter: " + orderedEvent.EventCounter );
                                if (Broker.logging == FileParsing.LoggingLevel.Full)
                                {
                                    new Task(() => { Broker.puppetMaster.Log("BroEvent " + Broker.name + ", " + orderedEvent.PublisherName + ", " + orderedEvent.Topic + ", " + orderedEvent.TopicEventNum); }).Start();
                                }
                            }
                        }

                    }
                }
                else if(Broker.ordering == FileParsing.Ordering.Total)
                {
                    lock (Broker.subscriptionMutex)
                    {
                        if (Broker.routing == FileParsing.RoutingPolicy.Filter)
                        {
                        }
                        //flooding 
                        else
                        {
                            //lock ( Broker.totalOrderEvents.mutex ) {
                                Broker.totalOrderEvents.AddEvent( evt );
                                foreach ( Event orderedEvent in Broker.totalOrderEvents.GetOrderedEventsUpToDate() ) {
                                    Broker.SendContent( orderedEvent, name );

                                    // Console.WriteLine( "orderedEvent.EventCounter: " + orderedEvent.EventCounter );
                                    if ( Broker.logging == FileParsing.LoggingLevel.Full ) {
                                        new Task( () => { Broker.puppetMaster.Log( "BroEvent " + Broker.name + ", " + orderedEvent.PublisherName + ", " + orderedEvent.Topic + ", " + orderedEvent.TopicEventNum ); } ).Start();
                                    }
                                }
                            //}
                        }
                    }
                }
                else
                {
                    Broker.SendContent(evt, name);
                    if (Broker.logging == FileParsing.LoggingLevel.Full)
                    {
                        new Task(() => { Broker.puppetMaster.Log("BroEvent " + Broker.name + ", " + evt.PublisherName + ", " + evt.Topic + ", " + evt.TopicEventNum); }).Start();
                    }
                }
            }).Start();
        }

        // No replication subscription
        /*public void Subscribe( string processname, string topic ) {

           // if (Broker.frozen){

                new Task(() =>
                {
                    lock (Broker.monitorLock)
                    {
                        while (Broker.frozen)
                        {
                            Monitor.Wait(Broker.monitorLock);
                        }
                    }

                    ISubscriber sub = Broker.subscribers.Find(n => n.name == processname).subcriber;
                    if (sub != null)
                    {
                        lock (Broker.subscriptionMutex)
                        {
                            // Console.WriteLine( "SUB: " + processname + " just subscribed to " + topic );
                            Broker.topicSubscribers.AddTopicSubscriber(topic, processname, sub);

                            foreach (NamedBroker broker in Broker.children)
                            {
                                if (broker.name != processname)
                                {
                                    broker.broker.SubscribeBroker(Broker.name, topic);
                                }
                                //coiso.broker.SendContent( evt );
                            }
                        }
                    }


                }).Start();

        }*/
        /*public void Unsubscribe( string processname, string topic ) {

            new Task(() =>
            {
                lock (Broker.monitorLock)
                {
                    while (Broker.frozen)
                    {
                        Monitor.Wait(Broker.monitorLock);
                    }
                }

                ISubscriber sub = Broker.subscribers.Find( n => n.name == processname ).subcriber;
            if ( sub != null ) {
                lock ( Broker.subscriptionMutex ) {
                   // Console.WriteLine( processname + " just unsubscribed from " + topic );
                    Broker.topicSubscribers.RemoveTopicSubscriber( topic, processname );

                    //if ( Broker.parent != null ) {
                        bool a = !Broker.topicSubscribers.HasTopic( topic );
                        bool b = !Broker.topicBrokers.HasTopic( topic );
                        if ( a && b ) {
                            Broker.EraseRelatedEvents( topic );
                            foreach (NamedBroker broker in Broker.children)
                            {
                                if (broker.name != processname)
                                {
                                    broker.broker.UnsubscribeBroker(Broker.name, topic);
                                }
                                //coiso.broker.SendContent( evt );
                            }
                        }

                    //}
                }
            }
            }).Start();
        }*/
        // No replication
        /*public void SubscribeBroker( string processname, string topic ) {

            new Task(() =>
            {
                lock (Broker.monitorLock)
                {
                    while (Broker.frozen)
                    {
                        Monitor.Wait(Broker.monitorLock);
                    }
                }

                IBroker bro = Broker.children.Find( n => n.name == processname ).broker;
            if ( bro != null ) {
                lock ( Broker.subscriptionMutex ) {
                    //Console.WriteLine( "BRO " + processname + " just subscribed to " + topic );
                    Broker.topicBrokers.AddTopicBroker( topic, processname, bro );

                        foreach (NamedBroker broker in Broker.children)
                        {
                            if (broker.name != processname)
                            {
                                broker.broker.SubscribeBroker(Broker.name, topic);
                            }
                            //coiso.broker.SendContent( evt );
                        }
                    }
            }
            }).Start();
        }*/
        // No replication
        /*public void UnsubscribeBroker( string processname, string topic ) {

            new Task(() =>
            {
                lock (Broker.monitorLock)
                {
                    while (Broker.frozen)
                    {
                        Monitor.Wait(Broker.monitorLock);
                    }
                }

                IBroker bro = Broker.children.Find( n => n.name == processname ).broker;
            if ( bro != null ) {
                lock ( Broker.subscriptionMutex ) {
                  //  Console.WriteLine( "BRO " + processname + " just unsubscribed from " + topic );
                    Broker.topicBrokers.RemoveTopicBroker( topic, processname );

                    if ( Broker.parent != null ) {
                        bool a = !Broker.topicSubscribers.HasTopic( topic );
                        bool b = !Broker.topicBrokers.HasTopic( topic );
                        if ( a && b ) {
                            Broker.EraseRelatedEvents( topic );
                            foreach (NamedBroker broker in Broker.children)
                            {
                                if (broker.name != processname)
                                {
                                    broker.broker.SubscribeBroker(Broker.name, topic);
                                }
                                //coiso.broker.SendContent( evt );
                            }
                            }
                    }
                }
            }
            }).Start();
        }*/

        public void Subscribe( string processname, string topic ) {

            // if (Broker.frozen){

            new Task( () =>
            {
                lock ( Broker.monitorLock ) {
                    while ( Broker.frozen ) {
                        Monitor.Wait( Broker.monitorLock );
                    }
                }

                ISubscriber sub = Broker.subscribers.Find( n => n.name == processname ).subcriber;
                if ( sub != null ) {
                    lock ( Broker.subscriptionMutex ) {
                        // Console.WriteLine( "SUB: " + processname + " just subscribed to " + topic );
                        Broker.topicSubscribers.AddTopicSubscriber( topic, processname, sub );

                        foreach ( BrokerCircle broker in Broker.neighbourBrokers ) {
                            if ( broker.name != processname ) {
                                broker.SubscribeBroker( Broker.groupName, topic );
                            }
                            //coiso.broker.SendContent( evt );
                        }
                    }
                }


            } ).Start();

        }
        public void Unsubscribe( string processname, string topic ) {

            new Task( () => {
                lock ( Broker.monitorLock ) {
                    while ( Broker.frozen ) {
                        Monitor.Wait( Broker.monitorLock );
                    }
                }

                ISubscriber sub = Broker.subscribers.Find( n => n.name == processname ).subcriber;
                if ( sub != null ) {
                    lock ( Broker.subscriptionMutex ) {
                        // Console.WriteLine( processname + " just unsubscribed from " + topic );
                        Broker.topicSubscribers.RemoveTopicSubscriber( topic, processname );

                        bool a = !Broker.topicSubscribers.HasTopic( topic );
                        bool b = !Broker.subscriptionCircles.HasTopic( topic );
                        if ( a && b ) {
                            Broker.EraseRelatedEventsReplication( topic );
                            foreach ( BrokerCircle broker in Broker.neighbourBrokers ) {
                                if ( broker.name != processname ) {
                                    broker.UnsubscribeBroker( Broker.groupName, topic );
                                }
                                //coiso.broker.SendContent( evt );
                            }
                        }
                    }
                }
            } ).Start();
        }
        public void SubscribeBroker( string processname, string topic ) {

            new Task( () => {
                lock ( Broker.monitorLock ) {
                    while ( Broker.frozen ) {
                        Monitor.Wait( Broker.monitorLock );
                    }
                }

                BrokerCircle bro = Broker.neighbourBrokers.Find( n => n.name == processname );
                if ( bro != null ) {
                    lock ( Broker.subscriptionMutex ) {
                        //Console.WriteLine( "BRO " + processname + " just subscribed to " + topic );
                        Broker.subscriptionCircles.AddTopicBroker( topic, bro );

                        foreach ( BrokerCircle broker in Broker.neighbourBrokers ) {
                            if ( broker.name != processname ) {
                                broker.SubscribeBroker( Broker.groupName, topic );
                            }
                            //coiso.broker.SendContent( evt );
                        }
                    }
                }
            } ).Start();
        }
        public void UnsubscribeBroker( string processname, string topic ) {

            new Task( () => {
                lock ( Broker.monitorLock ) {
                    while ( Broker.frozen ) {
                        Monitor.Wait( Broker.monitorLock );
                    }
                }

                BrokerCircle bro = Broker.neighbourBrokers.Find( n => n.name == processname );
                if ( bro != null ) {
                    lock ( Broker.subscriptionMutex ) {
                        //  Console.WriteLine( "BRO " + processname + " just unsubscribed from " + topic );
                        Broker.subscriptionCircles.RemoveTopicBroker( topic, processname );

                        //if ( Broker.parent != null ) {
                            bool a = !Broker.topicSubscribers.HasTopic( topic );
                            bool b = !Broker.subscriptionCircles.HasTopic( topic );
                            if ( a && b ) {
                                Broker.EraseRelatedEvents( topic );
                                foreach ( BrokerCircle broker in Broker.neighbourBrokers ) {
                                    if ( broker.name != processname ) {
                                        broker.SubscribeBroker( Broker.groupName, topic );
                                    }
                                    //coiso.broker.SendContent( evt );
                                }
                            }
                        //}
                    }
                }
            } ).Start();
        }

        public void RegisterPuppetMaster(string address)
        {
            Broker.puppetMaster = (IPuppetMaster)Activator.GetObject(
               typeof(IPuppetMaster),
               address);

            //Console.WriteLine("I'm a puppet");
        }
        public void RegisterSequencer( string address ) {
            Broker.sequencer = (ISequencer)Activator.GetObject(
                typeof( ISequencer ),
                address );

            /*if ( Broker.sequencer != null ) {
                Console.WriteLine( "Sequencer number test: " + Broker.sequencer.GetNextSequenceNumber() );
            }
            else {
                Console.WriteLine( "No sequencer found." );
            }*/
        }
    }
    class Broker
    {
        static public List<IPublisher> publishers = new List<IPublisher>();
        static public List<NamedSubscriber> subscribers = new List<NamedSubscriber>();
        static public List<NamedBroker> children = new List<NamedBroker>();

        static public IBroker parent;

        static public IPuppetMaster puppetMaster;
        static public ISequencer sequencer;

        static public TopicSubscriberList topicSubscribers = new TopicSubscriberList();
        static public TopicBrokerList topicBrokers = new TopicBrokerList();

        static public PublisherTopicDictionary publisherTopics = new PublisherTopicDictionary();
        static public EventQueueFlooding publisherEvents = new EventQueueFlooding();

        static public EventListFlooding totalOrderEvents = new EventListFlooding();

        static public FileParsing.Ordering ordering = FileParsing.Ordering.Fifo;
        static public FileParsing.RoutingPolicy routing = FileParsing.RoutingPolicy.Filter;
        static public FileParsing.LoggingLevel logging = FileParsing.LoggingLevel.Full;

        static public object subscriptionMutex = new object();

        static public object monitorLock = new object();

        // No replication
        static public string name;

        // For replication
        static public string groupName;
        // 0 = first broker, 1-N = replicas
        static public int replicationId = 0;
        static public List<IBroker> replicaBrokers = new List<IBroker>();
        static public List<BrokerCircle> neighbourBrokers = new List<BrokerCircle>();
        static public BrokerCircleSubscriptionTable subscriptionCircles = new BrokerCircleSubscriptionTable();

        static public bool frozen = false;


        static void Main(string[] args)
        {
            if ( args.Length != 6 ) {
                return;
            }

            foreach ( string arg in args ) {
                Console.WriteLine( "Arg: " + arg );
            }

            int port; Int32.TryParse( args[ 0 ], out port );
            string serviceName = args[ 1 ];
            Broker.name = args[2];
            Broker.groupName = args[ 2 ]; // Group name will be overwritten if replication is active
            ordering = (args[ 3 ].ToUpper() == "NO" ? FileParsing.Ordering.No :
                (args[ 3 ].ToUpper() == "FIFO" ? FileParsing.Ordering.Fifo : FileParsing.Ordering.Total));
            routing = (args[ 4 ].ToUpper() == "FLOODING" ? FileParsing.RoutingPolicy.Flooding :
                FileParsing.RoutingPolicy.Filter);
            logging = (args[ 5 ].ToUpper() == "LIGHT" ? FileParsing.LoggingLevel.Light :
                FileParsing.LoggingLevel.Full);

            BinaryServerFormatterSinkProvider provider = new BinaryServerFormatterSinkProvider();
            IDictionary props = new Hashtable();
            props[ "port" ] = port;
            props[ "timeout" ] = 3000; // 3 secs
            TcpChannel channel = new TcpChannel( props, null, provider );

            //TcpChannel channel = new TcpChannel(port);
            ChannelServices.RegisterChannel(channel, true);

            RemotingConfiguration.RegisterWellKnownServiceType(
              typeof(RemoteBroker),
              serviceName,
              WellKnownObjectMode.Singleton);


            //System.Console.WriteLine("Hi, I'm a broker...");

           // addSubscriberToList();
           // SendToSubscribers("banana");

            System.Console.ReadLine();
        }

        static public void SendContent(Event evt, String name)
        {
            //Console.WriteLine( lastSender + "  ->  " + Broker.groupName );

            if (Broker.routing == FileParsing.RoutingPolicy.Filter)
            {
                SendContentFiltering(evt, name);
            }

            else
            {
                string lastSender = evt.LastSenderName;
                evt.LastSenderName = Broker.groupName;

                // Flooding
                Console.WriteLine( "Sending " + evt.EventCounter );
                foreach (NamedSubscriber subscriber in Broker.subscribers)
                {
                    subscriber.subcriber.ReceiveContent(evt);
                }

                // No replication
                /*foreach ( NamedBroker broker in Broker.children)
                {
                    if ( broker.name != lastSender )
                    {
                        new Task(() => { broker.broker.SendContent(evt, Broker.name); }).Start();
                    }
                }*/

                // Replication
                foreach ( BrokerCircle broker in Broker.neighbourBrokers ) {
                    if ( broker.name != lastSender ) {
                        new Task( () => { broker.SendContent( evt, Broker.groupName ); } ).Start();
                    }
                }
            }
        }

        static public void SendContentFiltering( Event evt, String name ) {
            string lastSender = evt.LastSenderName;
            evt.LastSenderName = Broker.groupName;

            var subs = topicSubscribers.FindAllSubscribers( evt.Topic );
            foreach ( NamedSubscriber sub in subs ) {
                try {
                    sub.subcriber.ReceiveContent( evt );
                }
                catch ( Exception e ) {
                    Console.WriteLine( "Exception to susbcriber: " + e.Message );
                }
            }

            // No replication
            /*var bros = topicBrokers.FindAllBrokers( evt.Topic );
            foreach ( NamedBroker bro in bros ) {
                //bro.broker.SendContent( evt );
                new Task( () => { bro.broker.SendContent( evt, Broker.name ); } ).Start();
            }*/

            // Replication
            var bros = subscriptionCircles.FindAllBrokers( evt.Topic );
            foreach ( BrokerCircle bro in bros ) {
                if ( bro.name != lastSender ) {
                    new Task( () => { bro.SendContent( evt, Broker.groupName ); } ).Start();
                }
            }
        }

        static public void EraseRelatedEvents( string topic ) {
            if ( routing == FileParsing.RoutingPolicy.Filter ) {
                if ( topic.EndsWith( "/*" ) ) {
                    publisherTopics.EraseSubTopics( topic, topicSubscribers, topicBrokers );
                }
                else {
                    publisherTopics.EraseTopic( topic );
                }
            }
        }

        static public void EraseRelatedEventsReplication( string topic ) {
            if ( routing == FileParsing.RoutingPolicy.Filter ) {
                if ( topic.EndsWith( "/*" ) ) {
                    publisherTopics.EraseSubTopicsReplication( topic, topicSubscribers, subscriptionCircles );
                }
                else {
                    publisherTopics.EraseTopic( topic );
                }
            }
        }

    }
}
