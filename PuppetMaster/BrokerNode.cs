using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SESDAD {
    class BrokerNode {
        public FileParsing.Site site;
        public List<IPuppetBroker> brokers = new List<IPuppetBroker>();
        public List<FileParsing.Process> brokersData = new List<FileParsing.Process>();

        public void AddBroker( IPuppetBroker broker, FileParsing.Process pData ) {
            brokers.Add( broker );
            brokersData.Add( pData );
        }

        public List<string> GetListOfAddresses() {
            List<string> addresses = new List<string>();
            foreach ( var data in brokersData ) {
                addresses.Add( data.url );
            }
            return addresses;
        }

        public List<string> GetListOfAddressesExcept( string url ) {
            List<string> addresses = new List<string>();
            foreach ( var data in brokersData ) {
                if ( data.url == url ) { continue; }
                addresses.Add( data.url );
            }
            return addresses;
        }
    }
}
