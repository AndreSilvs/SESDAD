using System;
using System.Collections.Generic;
using System.Text;

namespace SESDAD {
    public interface IPuppetBroker {
        void RegisterSubscriber( string address, string name );
        void RegisterPublisher( string address );

        void RegisterParent( string address );
        void RegisterChild( string address, string name );

        /*void Status();

        void Freeze();
        void Unfreeze();
        void Crash();*/
    }
}