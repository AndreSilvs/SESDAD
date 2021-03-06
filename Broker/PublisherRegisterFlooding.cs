﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SESDAD {

    public class EventListFlooding {
        public List<Event> list = new List<Event>();
        public int lastEvent = -1;
        public object mutex = new object();

        public void AddEvent( Event evt ) {
            //lock ( mutex ) {
                // Se nao tem registo do topic, criar
                list.Add( evt );
                Console.WriteLine( "Adding " + evt.EventCounter + " from " + evt.PublisherName + " topic " + evt.Topic );
                // Ordenar lista por numero de evento de topico
                list.Sort( ( t1, t2 ) => (t1.EventCounter - t2.EventCounter) );
            //}
        }

        public List<Event> GetOrderedEventsUpToDate() {
            List<Event> events = new List<Event>();
            while ( (list.Count > 0) && list[ 0 ].EventCounter == lastEvent + 1 ) {
                events.Add( list[ 0 ] );
                list.RemoveAt( 0 );
                lastEvent++;
            }
            return events;
        }


    }

    public class EventQueueFlooding {
        private Dictionary<string, EventListFlooding> dictionary = new Dictionary<string, EventListFlooding>();
        public object mutex = new object();

        public EventQueueFlooding() {}

        public void AddEvent( Event evt ) {
            lock ( mutex ) {
                if ( !dictionary.ContainsKey( evt.PublisherName ) ) {
                    dictionary.Add( evt.PublisherName, new EventListFlooding() );
                }
                dictionary[ evt.PublisherName ].AddEvent( evt );
            }
        }

        public EventListFlooding GetEventList( string publisherName ) {
            lock ( mutex ) {
                if ( dictionary.ContainsKey( publisherName ) ) {
                    return dictionary[ publisherName ];
                }
            }
            return null;
        }
    }
}
