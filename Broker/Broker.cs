using System;
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

        private List<TopicSubscribers> topicSubscribers = new List<TopicSubscribers>();

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

        public bool HasTopic( string topic ) {
            return topicSubscribers.Exists( n => n.topic == topic );
        }
    }

    public class TopicBrokerList {

        private List<TopicBrokers> topicBrokers = new List<TopicBrokers>();

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

        public bool HasTopic( string topic ) {
            return topicBrokers.Exists( n => n.topic == topic );
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

            Console.WriteLine( "I have a kid" );
        }

        public void RegisterParent( string address ) {

            Broker.parent = (IBroker)Activator.GetObject(
               typeof( IBroker ),
               address );

            Console.WriteLine( "I have a parent" );
        }

        public void RegisterPublisher( string address ) {
            Broker.publishers.Add( (IPublisher)Activator.GetObject(
               typeof( IPublisher ),
               address ) );

            Console.WriteLine( "I have a publisher" );
        }

        public void RegisterSubscriber( string address, string name ) {
            Broker.subscribers.Add( new NamedSubscriber( name, (ISubscriber)Activator.GetObject(
               typeof( ISubscriber ),
               address ) ) );

            Console.WriteLine( "I have a subscriber" );
        }

        public void Status() {
            throw new NotImplementedException();
        }

        public void Crash() {
            throw new NotImplementedException();
        }

        public void Freeze() {
            throw new NotImplementedException();
        }

        public void Unfreeze() {
            throw new NotImplementedException();
        }

        //Broker
        public void SendContent( Event evt ) {
            //Devo fazer 1 chamada asyncrona para cada subscriber ou 1 para todos????????
            //??????????????????????
            //????????????????
            SendContentDelegate del = new SendContentDelegate( Broker.SendContent );
            AsyncCallback remoteCallback = new AsyncCallback( PublishAsyncCallBack );
            IAsyncResult remAr = del.BeginInvoke( evt, remoteCallback, null );

            new Task( () => { Broker.puppetMaster.Log( "BroEvent " + Broker.name + " " + evt.PublisherName + " " + evt.Topic + " " + evt.TopicEventNum ); } ).Start();

            /*foreach (ISubscriber coiso in Broker.subscribers)
            {
                coiso.ReceiveContent(evt);
            }

            foreach (IBroker coiso in Broker.children)
            {
                coiso.SendContent(evt);
            }*/
        }

        public void SendContentUp( Event evt ) {
            System.Console.WriteLine( "Up" );

            if ( Broker.parent != null ) {
                Console.WriteLine( "Calling send up" );
                SendContentDelegate del = new SendContentDelegate( Broker.parent.SendContentUp );
                AsyncCallback remoteCallback = new AsyncCallback( PublishAsyncCallBack );
                IAsyncResult remAr = del.BeginInvoke( evt, remoteCallback, null );
            }
            else {
                Console.WriteLine( "Calling send down" );
                SendContentDelegate del = new SendContentDelegate( Broker.SendContent );
                AsyncCallback remoteCallback = new AsyncCallback( PublishAsyncCallBack );
                IAsyncResult remAr = del.BeginInvoke( evt, remoteCallback, null );
            }
        }

        public void Subscribe( string processname, string topic ) {
            ISubscriber sub = Broker.subscribers.Find( n => n.name == processname ).subcriber;
            if ( sub != null ) {
                Console.WriteLine( "SUB: " + processname + " just subscribed to " + topic );
                Broker.topicSubscribers.AddTopicSubscriber( topic, processname, sub );

                if ( Broker.parent != null ) {
                    Broker.parent.SubscribeBroker( Broker.name, topic );
                }
            }
        }
        public void Unsubscribe( string processname, string topic ) {
            ISubscriber sub = Broker.subscribers.Find( n => n.name == processname ).subcriber;
            if ( sub != null ) {
                Console.WriteLine( processname + " just unsubscribed from " + topic );
                Broker.topicSubscribers.RemoveTopicSubscriber( topic, processname );

                if ( Broker.parent != null ) {
                    bool a = !Broker.topicSubscribers.HasTopic( topic );
                    bool b = !Broker.topicBrokers.HasTopic( topic );
                    Console.WriteLine( "Has Topic Subs? " + a.ToString() + "   Has Topic Bros? " + b.ToString() );
                    if ( a && b ) {
                        Broker.parent.UnsubscribeBroker( Broker.name, topic );
                    }
                }
            }
        }
        public void SubscribeBroker( string processname, string topic ) {
            IBroker bro = Broker.children.Find( n => n.name == processname ).broker;
            if ( bro != null ) {
                Console.WriteLine( "BRO " + processname + " just subscribed to " + topic );
                Broker.topicBrokers.AddTopicBroker( topic, processname, bro );

                if ( Broker.parent != null ) {
                    Broker.parent.SubscribeBroker( Broker.name, topic );
                }
            }
        }
        public void UnsubscribeBroker( string processname, string topic ) {
            IBroker bro = Broker.children.Find( n => n.name == processname ).broker;
            if ( bro != null ) {
                Console.WriteLine( "BRO " + processname + " just unsubscribed from " + topic );
                Broker.topicBrokers.RemoveTopicBroker( topic, processname );

                if ( Broker.parent != null ) {
                    bool a = !Broker.topicSubscribers.HasTopic( topic );
                    bool b = !Broker.topicBrokers.HasTopic( topic );
                    Console.WriteLine( "Has Topic Subs? " + a.ToString() + "   Has Topic Bros? " + b.ToString() );
                    if ( a && b ) {
                        Broker.parent.UnsubscribeBroker( Broker.name, topic );
                    }
                }
            }
        }

        public void RegisterPuppetMaster(string address)
        {
            Broker.puppetMaster = (IPuppetMaster)Activator.GetObject(
               typeof(IPuppetMaster),
               address);

            Console.WriteLine("I'm a puppet");
        }
    }
    class Broker
    {
        static public List<IPublisher> publishers = new List<IPublisher>();

        static public List<NamedSubscriber> subscribers = new List<NamedSubscriber>();

        static public List<NamedBroker> children = new List<NamedBroker>();

        static public IBroker parent;

        static public IPuppetMaster puppetMaster;

        static public TopicSubscriberList topicSubscribers = new TopicSubscriberList();
        static public TopicBrokerList topicBrokers = new TopicBrokerList();

        static public string name;

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
            Broker.name = args[2];

            TcpChannel channel = new TcpChannel(port);
            ChannelServices.RegisterChannel(channel, true);

            RemotingConfiguration.RegisterWellKnownServiceType(
              typeof(RemoteBroker),
              serviceName,
              WellKnownObjectMode.Singleton);


            System.Console.WriteLine("Hi, I'm a broker...");

           // addSubscriberToList();
           // SendToSubscribers("banana");

            System.Console.ReadLine();
        }

        static public void SendContent(Event evt)
        {

            System.Console.WriteLine("Down");

            //Broker.puppetMaster.Log("BroEvent " + Broker.name + " something somethin");

            foreach ( NamedSubscriber coiso in Broker.subscribers)
            {
                coiso.subcriber.ReceiveContent(evt);
            }

            foreach ( NamedBroker coiso in Broker.children)
            {
                coiso.broker.SendContent(evt);
            }
        }

    }
}
