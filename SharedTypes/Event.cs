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
        public string PublisherName;
        public string LastSenderName;
        public int TopicEventNum;
        public int EventCounter;

        public Event(string topic, string content, string publisherName, int eventNum, int counter )
        {
            Topic = topic;
            Content = content;
            PublisherName = publisherName;
            LastSenderName = publisherName;
            TopicEventNum = eventNum;
            EventCounter = counter;
        }
    }
}
