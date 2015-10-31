using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Messaging;

namespace SESDAD
{


    class RemoteSubscriber : MarshalByRefObject, ISubscriber, IPuppetSubscriber, IPuppetProcess
    {
        public void RegisterBroker(string address)
        {
            Subscriber.broker = (IBroker)Activator.GetObject(
               typeof( IBroker ),
               address );

            //Console.WriteLine("I have a ~broker");
        }

        public void Crash()
        {
            throw new NotImplementedException();
        }

        public void ForceSubscribe(string topicname)
        {
            Subscriber.Subscribe( topicname );
        }

        public void ForceUnsubscribe(string topicname)
        {
            Subscriber.Unsubscribe( topicname );
        }

        public void Freeze()
        {
            throw new NotImplementedException();
        }

        public void ReceiveContent(Event evt)
        {
            new Task( () => { Subscriber.puppetMaster.Log( "SubEvent " + Subscriber.name + " " + evt.PublisherName + " " + evt.Topic + " " + evt.TopicEventNum ); } ).Start();
            System.Console.WriteLine("Topic: " + evt.Topic + " Content: " + evt.Content);
           // Subscriber.puppetMaster.Log("SubEvent " + Subscriber.name + " thing.");
        }


        public void Status()
        {
            Console.WriteLine("I'm " + Subscriber.name);
            Console.WriteLine("I'm alive");

            //Se os subscribers souberem as suas subscrições por aqui tambem
        }

        public void Unfreeze()
        {
            throw new NotImplementedException();
        }

        public void RegisterPuppetMaster(string address)
        {
            Subscriber.puppetMaster = (IPuppetMaster)Activator.GetObject(
                 typeof(IPuppetMaster),
                 address);

            //Console.WriteLine("I'm a puppet");
        }
    }


    public delegate void SubscriberDelegate( string name, string topic );

    class Subscriber
    {

        static public IBroker broker;
        static public IPuppetMaster puppetMaster;
        static public string name;

        public static void SubscriberCallback( IAsyncResult ar ) {
            SubscriberDelegate del = (SubscriberDelegate)((AsyncResult)ar).AsyncDelegate;
            del.EndInvoke( ar );
            return;
        }

        public static void Subscribe( string topic ) {
            Console.WriteLine( "Subscribing to: " + topic );

            //broker.Subscribe( name, topic );
            SubscriberDelegate del = new SubscriberDelegate( broker.Subscribe );
            AsyncCallback remoteCallback = new AsyncCallback( SubscriberCallback );
            IAsyncResult remAr = del.BeginInvoke( name, topic, remoteCallback, null );
        }

        public static void Unsubscribe( string topic ) {
            Console.WriteLine( "Unsubscribing from: " + topic );

            //broker.Unsubscribe( name, topic );
            SubscriberDelegate del = new SubscriberDelegate( broker.Unsubscribe );
            AsyncCallback remoteCallback = new AsyncCallback( SubscriberCallback );
            IAsyncResult remAr = del.BeginInvoke( name, topic, remoteCallback, null );
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

            TcpChannel channel = new TcpChannel(port);
            ChannelServices.RegisterChannel(channel, true);

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
