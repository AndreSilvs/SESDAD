using System;
using System.Collections.Generic;
using System.Text;

namespace SESDAD
{

    public enum EventType { Subscribe, Unsubscribe, Publish }
    [Serializable]
    public struct Event
    {
        public string Topic;
        public string Content;

        public Event(string topic, string content )
        {
            Topic = topic;
            Content = content;
        }
    }
}
