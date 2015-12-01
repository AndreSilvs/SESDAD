using System;
using System.Collections.Generic;
using System.Text;

namespace SESDAD {
    public interface IPuppetPublisher {
        void RegisterBroker(string address);
        void RegisterBrokers( List<string> addresses );

        void ForcePublish( int numberEvents, string topicname, int interval_ms );

        /*void Status();

        void Freeze();
        void Unfreeze();
        void Crash();*/
    }
}