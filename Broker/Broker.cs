﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Messaging;
using System.Text.RegularExpressions;
using System.Net.Sockets;
using System.Threading;

namespace SESDAD
{
    public class NamedSubscriber {
        public string name;
        public ISubscriber subcriber;

        public NamedSubscriber( string name, ISubscriber sub ) {
            this.name = name;
            this.subcriber = sub;
        }
    }
    public class NamedBroker {
        public string name;
        public IBroker broker;

        public NamedBroker( string name, IBroker bro ) {
            this.name = name;
            this.broker = bro;
        }
    }
    public class TopicSubscribers {
        public string topic;
        public List<NamedSubscriber> subscribers = new List<NamedSubscriber>();

        public bool HasSubscribers() { return subscribers.Count > 0; }
        public void AddSubscriber( string name, ISubscriber sub ) {
            if ( !subscribers.Exists( n => n.name == name ) ) {
                subscribers.Add( new NamedSubscriber( name, sub ) );
            }
        }
        public void RemoveSubscriber( string name ) {
            subscribers.RemoveAll( n => n.name == name );
        }
    }

    public class TopicBrokers {
        public string topic;
        public List<NamedBroker> brokers = new List<NamedBroker>();

        public bool HasBrokers() { return brokers.Count > 0; }
        public void AddBroker( string name, IBroker bro ) {
            if ( !brokers.Exists( n => n.name == name ) ) {
                brokers.Add( new NamedBroker( name, bro ) );
            }
        }
        public void RemoveBroker( string name ) {
            brokers.RemoveAll( n => n.name == name );
        }
    }

    public class TopicSubscriberList {

        public List<TopicSubscribers> topicSubscribers = new List<TopicSubscribers>();
        private object listLock = new object();

        public void AddTopicSubscriber( string topic, string name, ISubscriber sub ) {
            TopicSubscribers entry = FindTopic( topic );
            if ( entry != null ) {
                entry.AddSubscriber( name, sub );
                return;
            }
            entry = new TopicSubscribers();
            entry.topic = topic;
            entry.AddSubscriber( name, sub );
            topicSubscribers.Add( entry );
        }
        public void RemoveTopicSubscriber( string topic, string name ) {
            TopicSubscribers entry = FindTopic( topic );
            if ( entry != null ) {
                entry.RemoveSubscriber( name );
                if ( !entry.HasSubscribers() ) {
                    topicSubscribers.Remove( entry );
                }
            }
        }

        public TopicSubscribers FindTopic( string topic ) {
            return topicSubscribers.Find( n => n.topic == topic );
        }
        public HashSet<NamedSubscriber> FindAllSubscribers( string topic ) {
            HashSet<NamedSubscriber> subs = new HashSet<NamedSubscriber>();

            // Topic may be:  "/edu/ulisboa"
            // Sub topic may be "/edu/*"
            // So we're checking if the topic fits in the subscribed sub topic
            foreach ( TopicSubscribers subTopic in topicSubscribers ) {
                bool match = false;
                if ( subTopic.topic == topic ) {
                    match = true;
                }
                else if ( subTopic.topic.EndsWith( "/*" ) ) {
                    string regexTopic = "^" + subTopic.topic.Substring( 0, subTopic.topic.Length - 1 ) + ".*$";
                    Regex regex = new Regex( regexTopic );
                    if ( regex.IsMatch( topic ) ) {
                        match = true;
                    }
                }

                if ( match ) {
                    foreach ( NamedSubscriber sub in subTopic.subscribers ) {
                        subs.Add( sub );
                    }
                }
            }
            return subs;
        }

        public bool HasTopic( string topic ) {
            return topicSubscribers.Exists( n => n.topic == topic );
        }
        public int HowManySubscribed( string topic ) {
            return (HasTopic( topic ) ? FindTopic( topic ).subscribers.Count : 0 );
        }
    }

    public class TopicBrokerList {

        public List<TopicBrokers> topicBrokers = new List<TopicBrokers>();

        public void AddTopicBroker( string topic, string name, IBroker bro ) {
            TopicBrokers entry = FindTopic( topic );
            if ( entry != null ) {
                entry.AddBroker( name, bro );
                return;
            }
            entry = new TopicBrokers();
            entry.topic = topic;
            entry.AddBroker( name, bro );
            topicBrokers.Add( entry );
        }
        public void RemoveTopicBroker( string topic, string name ) {
            TopicBrokers entry = FindTopic( topic );
            if ( entry != null ) {
                entry.RemoveBroker( name );
                if ( !entry.HasBrokers() ) {
                    topicBrokers.Remove( entry );
                }
            }
        }

        public TopicBrokers FindTopic( string topic ) {
            return topicBrokers.Find( n => n.topic == topic );
        }
        public HashSet<NamedBroker> FindAllBrokers( string topic ) {
            HashSet<NamedBroker> bros = new HashSet<NamedBroker>();

            // Topic may be:  "/edu/ulisboa"
            // Sub topic may be "/edu/*"
            // So we're checking if the topic fits in the subscribed sub topic
            foreach ( TopicBrokers broTopic in topicBrokers ) {
                bool match = false;
                if ( broTopic.topic == topic ) {
                    match = true;
                }
                else if ( broTopic.topic.EndsWith( "/*" ) ) {
                    string regexTopic = "^" + broTopic.topic.Substring( 0, broTopic.topic.Length - 1 ) + ".*$";
                    Regex regex = new Regex( regexTopic );
                    if ( regex.IsMatch( topic ) ) {
                        match = true;
                    }
                }

                if ( match ) {
                    foreach ( NamedBroker bro in broTopic.brokers ) {
                        bros.Add( bro );
                    }
                }
            }
            return bros;
        }

        public bool HasTopic( string topic ) {
            return topicBrokers.Exists( n => n.topic == topic );
        }
        public int HowManySubscribed( string topic ) {
            return (HasTopic( topic ) ? FindTopic( topic ).brokers.Count : 0);
        }
    }



    class RemoteBroker : MarshalByRefObject, IBroker, IPuppetBroker, IPuppetProcess {

        public delegate void SendContentDelegate( Event ev );

        // Non-interface methods
        public static void PublishAsyncCallBack( IAsyncResult ar ) {
            SendContentDelegate del = (SendContentDelegate)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke( ar );
            return;
        }


        //PuppetMaster
        public void RegisterChild( string address, string name ) {
            Broker.children.Add( new NamedBroker( name, (IBroker)Activator.GetObject(
               typeof( IBroker ),
               address ) ) );

            //Console.WriteLine( "I have a kid" );
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
                            Broker.totalOrderEvents.AddEvent(evt);
                            foreach (Event orderedEvent in Broker.totalOrderEvents.GetOrderedEventsUpToDate())
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

        public void Subscribe( string processname, string topic ) {

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

        }
        public void Unsubscribe( string processname, string topic ) {

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

                    if ( Broker.parent != null ) {
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
                    }
                }
            }
            }).Start();
        }
        public void SubscribeBroker( string processname, string topic ) {

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
        }
        public void UnsubscribeBroker( string processname, string topic ) {

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

        static public string name;

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
            if (Broker.routing == FileParsing.RoutingPolicy.Filter)
            {
                SendContentFiltering(evt, name);
            }

            else
            {
                // Flooding
                foreach (NamedSubscriber subscriber in Broker.subscribers)
                {
                    subscriber.subcriber.ReceiveContent(evt);
                }

                foreach (NamedBroker broker in Broker.children)
                {
                    if (broker.name != name)
                    {
                        new Task(() => { broker.broker.SendContent(evt, Broker.name); }).Start();
                    }
                    //coiso.broker.SendContent( evt );
                }
            }
        }

        static public void SendContentFiltering( Event evt, String name ) {
            var subs = topicSubscribers.FindAllSubscribers( evt.Topic );
            foreach ( NamedSubscriber sub in subs ) {
                try {
                    sub.subcriber.ReceiveContent( evt );
                }
                catch ( Exception e ) {
                    Console.WriteLine( "Exception to susbcriber: " + e.Message );
                }
            }

            var bros = topicBrokers.FindAllBrokers( evt.Topic );
            foreach ( NamedBroker bro in bros ) {
                //bro.broker.SendContent( evt );
                new Task( () => { bro.broker.SendContent( evt, Broker.name ); } ).Start();
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

    }
}
