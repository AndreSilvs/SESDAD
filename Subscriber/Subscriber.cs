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


    class RemoteSubscriber : MarshalByRefObject, ISubscriber, IPuppetSubscriber, IPuppetProcess
    {
        public void RegisterBroker(string address)
        {
            Subscriber.broker = (IBroker)Activator.GetObject(
               typeof( IBroker ),
               address );

            Console.WriteLine("I have a ~broker");
        }

        public void Crash()
        {
            throw new NotImplementedException();
        }

        public void ForceSubscribe(string topicname)
        {
            throw new NotImplementedException();
        }

        public void ForceUnsubscribe(string topicname)
        {
            throw new NotImplementedException();
        }

        public void Freeze()
        {
            throw new NotImplementedException();
        }

        public void ReceiveContent(Event evt)
        {
            System.Console.WriteLine("Topic: " + evt.Topic + " Content: " + evt.Content);
        }


        public void Status()
        {
            throw new NotImplementedException();
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

            Console.WriteLine("I'm a puppet");
        }
    }

        class Subscriber
    {

        static public IBroker broker;
        static public IPuppetMaster puppetMaster;

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
              typeof(RemoteSubscriber),
              serviceName,
              WellKnownObjectMode.Singleton);

            /*IBroker obj = (IBroker)Activator.GetObject(
             typeof(IBroker),
             "tcp://localhost:8086/broker");*/


            System.Console.WriteLine("Hi, I'm a subscriber...");
            System.Console.ReadLine();
        }
    }
}
