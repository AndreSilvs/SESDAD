using System;
using System.Collections.Generic;
using System.Text;

namespace SESDAD
{
    public interface IBroker
    {
        void SendContent(Event evt, String name);
        void SendContentPub(Event evt, String name);

        void Subscribe( string processname, string topic );
        void Unsubscribe( string processname, string topic );

        void SubscribeBroker( string processname, string topic );
        void UnsubscribeBroker( string processname, string topic );
    }
}