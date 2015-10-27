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
    class RemoteBroker : MarshalByRefObject, IBroker, IPuppetBroker, IPuppetProcess
    {
        //PuppetMaster
        public void RegisterChild( string address ) {
            Broker.children.Add((IBroker)Activator.GetObject(
               typeof(IBroker),
               address));

            Console.WriteLine("I have a kid");
        }

        public void RegisterParent( string address ) {

            Broker.parent = (IBroker)Activator.GetObject(
               typeof(IBroker),
               address);

            Console.WriteLine("I have a parent");
        }

        public void RegisterPublisher( string address ) {
            Broker.publishers.Add((IPublisher)Activator.GetObject(
               typeof(IPublisher),
               address));

            Console.WriteLine("I have a publisher");
        }

        public void RegisterSubscriber( string address ) {
            Broker.subscribers.Add((ISubscriber)Activator.GetObject(
               typeof(ISubscriber),
               address));

            Console.WriteLine("I have a subscriber");
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
        public void SendContent(Event evt)
        {
            foreach (ISubscriber coiso in Broker.subscribers)
            {
                coiso.ReceiveContent(evt);
            }

            foreach (IBroker coiso in Broker.children)
            {
                coiso.SendContent(evt);
            }
        }

        public void Subscribe()
        {
          //  throw new NotImplementedException();
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

        static public List<ISubscriber> subscribers = new List<ISubscriber>();

        static public List<IBroker> children = new List<IBroker>();

        static public IBroker parent;

        static public IPuppetMaster puppetMaster;

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

    }
}
