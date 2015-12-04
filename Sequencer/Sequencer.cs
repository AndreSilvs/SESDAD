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

namespace SESDAD {

    class RemoteSequencer : MarshalByRefObject, ISequencer {

        public int GetNextSequenceNumber() {
            int seq = 0;
            lock ( Sequencer.sequenceMutex ) {
                seq = sequence;
                ++sequence;
                Console.Write( "\rCurrent Sequence: " + sequence );
            }
            return seq;
        }

        private int sequence = 0;
    }

    class Sequencer {
        public static object sequenceMutex = new object();

        static void Main( string[] args ) {
            int port = 8999;
            string serviceName = "seq";

            /*BinaryServerFormatterSinkProvider provider = new BinaryServerFormatterSinkProvider();
            IDictionary props = new Hashtable();
            props[ "port" ] = port;
            props[ "timeout" ] = 3000; // 3 secs
            TcpChannel channel = new TcpChannel( props, null, provider );*/

            TcpChannel channel = new TcpChannel(port);
            ChannelServices.RegisterChannel( channel, false );

            RemotingConfiguration.RegisterWellKnownServiceType(
              typeof( RemoteSequencer ),
              serviceName,
              WellKnownObjectMode.Singleton );

            /*IBroker obj = (IBroker)Activator.GetObject(
             typeof(IBroker),
             "tcp://localhost:8086/broker");*/


            System.Console.WriteLine( "Sequencer Process." );
            System.Console.Write( "Current sequence: 0" );
            System.Console.ReadLine();
        }
    }
}
