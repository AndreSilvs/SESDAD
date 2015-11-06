using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SESDAD {
    public class OrderedTopicEvent {
        public List<Event> list = new List<Event>();
        public int lastEvent = -1;

        public OrderedTopicEvent() {
        }

        public OrderedTopicEvent( int evt ) {
            lastEvent = evt - 1;
        }
    }

    public class PublisherTopicRegister {
        private Dictionary<string, OrderedTopicEvent> topics = new Dictionary<string, OrderedTopicEvent>();
        public object mutex = new object();

        public PublisherTopicRegister() {
        }

        public void AddEvent( Event evt ) {
            lock ( mutex ) {
                // Se nao tem registo do topic, criar
                if ( !topics.ContainsKey( evt.Topic ) ) {
                    topics.Add( evt.Topic, new OrderedTopicEvent() );
                    topics[ evt.Topic ].lastEvent = evt.TopicEventNum - 1;
                }
                topics[ evt.Topic ].list.Add( evt );

                // Ordenar lista por numero de evento de topico
                topics[ evt.Topic ].list.Sort( ( t1, t2 ) => (t1.EventCounter - t2.EventCounter) );
            }
        }
        public List<Event> GetListEvents( string topic ) {
            return topics[ topic ].list;
        }
        public List<Event> GetLastOrderedEvents( string topic ) {
            List<Event> events = new List<Event>();
            if ( topics.ContainsKey( topic ) ) {
                //lock ( mutex ) {
                // Enquanto houver eventos ordenados
                while ( (topics[ topic ].list.Count > 0) && topics[ topic ].list[ 0 ].EventCounter == topics[ topic ].lastEvent + 1 ) {
                    events.Add( topics[ topic ].list[ 0 ] );
                    topics[ topic ].list.RemoveAt( 0 );
                    topics[ topic ].lastEvent++;
                }
                //}
            }
            return events;
        }
    }

    public class PublisherTopicDictionary {
        private Dictionary<string, PublisherTopicRegister> dictionary = new Dictionary<string, PublisherTopicRegister>();

        public PublisherTopicDictionary() {
        }

        public void AddEvent( string publisherName, Event evt ) {
            if ( !dictionary.ContainsKey( publisherName ) ) {
                dictionary.Add( publisherName, new PublisherTopicRegister() );
            }
            dictionary[ publisherName ].AddEvent( evt );
        }
        public PublisherTopicRegister GetPublisherTopic( string publisherName ) {
            return dictionary[ publisherName ];
        }
    }
}
