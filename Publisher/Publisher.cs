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
    public delegate void PublishPuppetLog( string message );
    public delegate void PublishTopicDelegate( Event ev );

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
            throw new NotImplementedException();
        }

        public void Freeze() {
            throw new NotImplementedException();
        }

        public void Unfreeze() {
            throw new NotImplementedException();
        }

        public void RegisterPuppetMaster(string address)
        {
            Publisher.puppetMaster = (IPuppetMaster)Activator.GetObject(
                typeof(IPuppetMaster),
                address);

            //Console.WriteLine("I'm a puppet");
        }
    }

    class Publisher
    {
        static public IBroker broker;
        static public string name;

        static int count = 0;
        static object lockObject = new object();

        static public IPuppetMaster puppetMaster;

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
                PublishTopicDelegate del = new PublishTopicDelegate( Publisher.broker.SendContentUp );
                AsyncCallback remoteCallback = new AsyncCallback( PublishAsyncCallBack );
                IAsyncResult remAr = del.BeginInvoke( new Event( topic, "banana", Publisher.name, i, getCountAndIncrement() ), remoteCallback, null );

                Thread.Sleep( interval_ms );

                //Publisher.puppetMaster.Log( "PubEvent" );
                PublishPuppetLog logDel = new PublishPuppetLog( Publisher.puppetMaster.Log );
                AsyncCallback remoteCallbackLog = new AsyncCallback( PublishLogCallBack );
                IAsyncResult remArLog = logDel.BeginInvoke( "PubEvent " + Publisher.name + ", " + topic + ", " + i.ToString(), remoteCallbackLog, null );
            }
        }

        static int getCountAndIncrement()
        {
            lock (lockObject)
            {
                return count++;
            }
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

            TcpChannel channel = new TcpChannel(port);
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
