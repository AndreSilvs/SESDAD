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
    class RemotePublisher : MarshalByRefObject, IPuppetPublisher, IPuppetProcess, IPublisher {

        public delegate void PublishPuppetLog( string message );
        public delegate void PublishTopicDelegate( Event ev );

        // Non-interface methods
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


        //----------------------

        public void RegisterBroker( string address ) {
            Publisher.broker = (IBroker)Activator.GetObject(
               typeof( IBroker ),
               address );

            Console.WriteLine("I have a ~broker");
        }

        public void ForcePublish( int numberEvents, string topicname, int interval_ms ) {
            //Publisher.broker.SendContent(new Event(topicname,"banana"));
            PublishTopicDelegate del = new PublishTopicDelegate( Publisher.broker.SendContent );
            AsyncCallback remoteCallback = new AsyncCallback( PublishAsyncCallBack );
            IAsyncResult remAr = del.BeginInvoke( new Event(topicname,"banana" ), remoteCallback, null );

            //Publisher.puppetMaster.Log( "PubEvent" );
            PublishPuppetLog logDel = new PublishPuppetLog( Publisher.puppetMaster.Log );
            AsyncCallback remoteCallbackLog = new AsyncCallback( PublishLogCallBack );
            IAsyncResult remArLog = logDel.BeginInvoke( "PubEvent " + Publisher.name + " banana",remoteCallbackLog, null );

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

        public void RegisterPuppetMaster(string address)
        {
            Publisher.puppetMaster = (IPuppetMaster)Activator.GetObject(
                typeof(IPuppetMaster),
                address);

            Console.WriteLine("I'm a puppet");
        }
    }

    class Publisher
    {
        static public IBroker broker;
        static public string name;

        static public IPuppetMaster puppetMaster;

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

            System.Console.WriteLine("Hi, I'm a publisher...");
            System.Console.ReadLine();
        }
    }
}
