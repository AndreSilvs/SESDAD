using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;

namespace SESDAD
{
    class RemotePublisher : MarshalByRefObject, IPuppetPublisher, IPuppetProcess, IPublisher {

        public void RegisterBroker( string address ) {
            Publisher.broker = (IBroker)Activator.GetObject(
               typeof( IBroker ),
               address );

            Console.WriteLine("I have a ~broker");
        }

        public void ForcePublish( int numberEvents, string topicname, int interval_ms ) {
            Publisher.broker.SendContent(new Event(topicname,"banana"));
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
    }

    class Publisher
    {
        static public IBroker broker;
        static void Main(string[] args)
        {
            if ( args.Length != 2 ) {
                return;
            }

            foreach ( string arg in args ) {
                Console.WriteLine( "Arg: " + arg );
            }

            int port; Int32.TryParse( args[ 0 ], out port );
            string serviceName = args[ 1 ];

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
