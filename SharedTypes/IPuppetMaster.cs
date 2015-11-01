using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace SESDAD {
    public interface IPuppetMaster {
        void Log( string message );
        void CreateBroker(string args);
        void CreatePublisher(string args);
        void CreateSubscriber(string args);
    }
}