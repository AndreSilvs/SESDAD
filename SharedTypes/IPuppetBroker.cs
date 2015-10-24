using System;
using System.Collections.Generic;
using System.Text;

namespace SESDAD {
    public interface IPuppetBroker {
        void RegisterSubscriber( string address );
        void RegisterPublisher( string address );

        void RegisterParent( string address );
        void RegisterChild( string address );

        /*void Status();

        void Freeze();
        void Unfreeze();
        void Crash();*/
    }
}