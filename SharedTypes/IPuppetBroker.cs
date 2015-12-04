using System;
using System.Collections.Generic;
using System.Text;

namespace SESDAD {
    public interface IPuppetBroker {
        void RegisterSequencer( string address );

        void RegisterSubscriber( string address, string name );
        void RegisterPublisher( string address );

        void RegisterParent( string address );
        void RegisterChild( string address, string name );

        // Original name is for the purposes of sending events through the network
        // There's no need to check against all brokers' names in the same site, just the original name
        void RegisterReplicas( List<string> addresses, string originalName, int id );

        // addresses of all replicas (including the original), name of the main broker (broker circle)
        void RegisterChildReplication( List<string> addresses, string name );
        // Assumes the parent is already registered.
        void RegisterParentReplication( string name );

        /*void Status();

        void Freeze();
        void Unfreeze();
        void Crash();*/
    }
}