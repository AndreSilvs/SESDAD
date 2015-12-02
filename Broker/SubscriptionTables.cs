using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SESDAD {
    public class NamedSubscriber {
        public string name;
        public ISubscriber subcriber;

        public NamedSubscriber( string name, ISubscriber sub ) {
            this.name = name;
            this.subcriber = sub;
        }
    }
    public class NamedBroker {
        public string name;
        public IBroker broker;

        public NamedBroker( string name, IBroker bro ) {
            this.name = name;
            this.broker = bro;
        }
    }
    public class TopicSubscribers {
        public string topic;
        public List<NamedSubscriber> subscribers = new List<NamedSubscriber>();

        public bool HasSubscribers() { return subscribers.Count > 0; }
        public void AddSubscriber( string name, ISubscriber sub ) {
            if ( !subscribers.Exists( n => n.name == name ) ) {
                subscribers.Add( new NamedSubscriber( name, sub ) );
            }
        }
        public void RemoveSubscriber( string name ) {
            subscribers.RemoveAll( n => n.name == name );
        }
    }

    public class TopicBrokers {
        public string topic;
        public List<NamedBroker> brokers = new List<NamedBroker>();

        public bool HasBrokers() { return brokers.Count > 0; }
        public void AddBroker( string name, IBroker bro ) {
            if ( !brokers.Exists( n => n.name == name ) ) {
                brokers.Add( new NamedBroker( name, bro ) );
            }
        }
        public void RemoveBroker( string name ) {
            brokers.RemoveAll( n => n.name == name );
        }
    }

    public class TopicSubscriberList {

        public List<TopicSubscribers> topicSubscribers = new List<TopicSubscribers>();
        private object listLock = new object();

        public void AddTopicSubscriber( string topic, string name, ISubscriber sub ) {
            TopicSubscribers entry = FindTopic( topic );
            if ( entry != null ) {
                entry.AddSubscriber( name, sub );
                return;
            }
            entry = new TopicSubscribers();
            entry.topic = topic;
            entry.AddSubscriber( name, sub );
            topicSubscribers.Add( entry );
        }
        public void RemoveTopicSubscriber( string topic, string name ) {
            TopicSubscribers entry = FindTopic( topic );
            if ( entry != null ) {
                entry.RemoveSubscriber( name );
                if ( !entry.HasSubscribers() ) {
                    topicSubscribers.Remove( entry );
                }
            }
        }

        public TopicSubscribers FindTopic( string topic ) {
            return topicSubscribers.Find( n => n.topic == topic );
        }
        public HashSet<NamedSubscriber> FindAllSubscribers( string topic ) {
            HashSet<NamedSubscriber> subs = new HashSet<NamedSubscriber>();

            // Topic may be:  "/edu/ulisboa"
            // Sub topic may be "/edu/*"
            // So we're checking if the topic fits in the subscribed sub topic
            foreach ( TopicSubscribers subTopic in topicSubscribers ) {
                bool match = false;
                if ( subTopic.topic == topic ) {
                    match = true;
                }
                else if ( subTopic.topic.EndsWith( "/*" ) ) {
                    string regexTopic = "^" + subTopic.topic.Substring( 0, subTopic.topic.Length - 1 ) + ".*$";
                    Regex regex = new Regex( regexTopic );
                    if ( regex.IsMatch( topic ) ) {
                        match = true;
                    }
                }

                if ( match ) {
                    foreach ( NamedSubscriber sub in subTopic.subscribers ) {
                        subs.Add( sub );
                    }
                }
            }
            return subs;
        }

        public bool HasTopic( string topic ) {
            return topicSubscribers.Exists( n => n.topic == topic );
        }
        public int HowManySubscribed( string topic ) {
            return (HasTopic( topic ) ? FindTopic( topic ).subscribers.Count : 0);
        }
    }

    public class TopicBrokerList {

        public List<TopicBrokers> topicBrokers = new List<TopicBrokers>();

        public void AddTopicBroker( string topic, string name, IBroker bro ) {
            TopicBrokers entry = FindTopic( topic );
            if ( entry != null ) {
                entry.AddBroker( name, bro );
                return;
            }
            entry = new TopicBrokers();
            entry.topic = topic;
            entry.AddBroker( name, bro );
            topicBrokers.Add( entry );
        }
        public void RemoveTopicBroker( string topic, string name ) {
            TopicBrokers entry = FindTopic( topic );
            if ( entry != null ) {
                entry.RemoveBroker( name );
                if ( !entry.HasBrokers() ) {
                    topicBrokers.Remove( entry );
                }
            }
        }

        public TopicBrokers FindTopic( string topic ) {
            return topicBrokers.Find( n => n.topic == topic );
        }
        public HashSet<NamedBroker> FindAllBrokers( string topic ) {
            HashSet<NamedBroker> bros = new HashSet<NamedBroker>();

            // Topic may be:  "/edu/ulisboa"
            // Sub topic may be "/edu/*"
            // So we're checking if the topic fits in the subscribed sub topic
            foreach ( TopicBrokers broTopic in topicBrokers ) {
                bool match = false;
                if ( broTopic.topic == topic ) {
                    match = true;
                }
                else if ( broTopic.topic.EndsWith( "/*" ) ) {
                    string regexTopic = "^" + broTopic.topic.Substring( 0, broTopic.topic.Length - 1 ) + ".*$";
                    Regex regex = new Regex( regexTopic );
                    if ( regex.IsMatch( topic ) ) {
                        match = true;
                    }
                }

                if ( match ) {
                    foreach ( NamedBroker bro in broTopic.brokers ) {
                        bros.Add( bro );
                    }
                }
            }
            return bros;
        }

        public bool HasTopic( string topic ) {
            return topicBrokers.Exists( n => n.topic == topic );
        }
        public int HowManySubscribed( string topic ) {
            return (HasTopic( topic ) ? FindTopic( topic ).brokers.Count : 0);
        }
    }






    /******************************************************************************/
    // REPLICATION BROKER SUBSCRIPTION TABLE
    /******************************************************************************/

    public class BrokerCircleTopicList {
        public string topic;
        public List<BrokerCircle> brokers = new List<BrokerCircle>();

        public bool HasBrokers() { return brokers.Count > 0; }
        public void AddBroker( BrokerCircle bro ) {
            if ( !brokers.Exists( n => n.name == bro.name ) ) {
                brokers.Add( bro );
            }
        }
        public void RemoveBroker( string name ) {
            brokers.RemoveAll( n => n.name == name );
        }
    }

    public class BrokerCircleSubscriptionTable {

        public List<BrokerCircleTopicList> table = new List<BrokerCircleTopicList>();

        public void AddTopicBroker( string topic, BrokerCircle bro ) {
            BrokerCircleTopicList entry = FindTopic( topic );
            if ( entry != null ) {
                entry.AddBroker( bro );
                return;
            }
            entry = new BrokerCircleTopicList();
            entry.topic = topic;
            entry.AddBroker( bro );
            table.Add( entry );
        }
        public void RemoveTopicBroker( string topic, string name ) {
            BrokerCircleTopicList entry = FindTopic( topic );
            if ( entry != null ) {
                entry.RemoveBroker( name );
                if ( !entry.HasBrokers() ) {
                    table.Remove( entry );
                }
            }
        }

        public BrokerCircleTopicList FindTopic( string topic ) {
            return table.Find( n => n.topic == topic );
        }
        public HashSet<BrokerCircle> FindAllBrokers( string topic ) {
            HashSet<BrokerCircle> bros = new HashSet<BrokerCircle>();

            // Topic may be:  "/edu/ulisboa"
            // Sub topic may be "/edu/*"
            // So we're checking if the topic fits in the subscribed sub topic
            foreach ( BrokerCircleTopicList broTopic in table ) {
                bool match = false;
                if ( broTopic.topic == topic ) {
                    match = true;
                }
                else if ( broTopic.topic.EndsWith( "/*" ) ) {
                    string regexTopic = "^" + broTopic.topic.Substring( 0, broTopic.topic.Length - 1 ) + ".*$";
                    Regex regex = new Regex( regexTopic );
                    if ( regex.IsMatch( topic ) ) {
                        match = true;
                    }
                }

                if ( match ) {
                    foreach ( BrokerCircle bro in broTopic.brokers ) {
                        bros.Add( bro );
                    }
                }
            }
            return bros;
        }

        public bool HasTopic( string topic ) {
            return table.Exists( n => n.topic == topic );
        }
        public int HowManySubscribed( string topic ) {
            return (HasTopic( topic ) ? FindTopic( topic ).brokers.Count : 0);
        }
    }
}
