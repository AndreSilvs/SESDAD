using System;
using System.Collections.Generic;
using System.Text;

namespace SESDAD {
    public interface IPuppetSubscriber {
        void RegisterBroker( string address );

        void ForceSubscribe( string topicname );
        void ForceUnsubscribe( string topicname );

        /*void Status();

        void Freeze();
        void Unfreeze();
        void Crash();*/
    }
}