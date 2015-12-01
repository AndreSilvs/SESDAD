using System;
using System.Collections.Generic;
using System.Text;

namespace SESDAD {
    // Implements the same functions as an IBroker to wrap the calls
    public class BrokerCircle : IBroker {
        public List<IBroker> brokers;
        public string name;

        public BrokerCircle( string circleName ) {
            name = circleName;
            brokers = new List<IBroker>();
        }

        public void AddBroker( IBroker broker ) {
            brokers.Add( broker );
        }

        public void SendContent( Event evt, string name ) {
            brokers[ 0 ].SendContent( evt, name );
            /*foreach ( IBroker broker in brokers ) {
                try {
                    broker.SendContent( evt, name );
                }
                catch ( Exception e ) {
                    // Something went wrong with this broker
                    // Remove broker
                    // Foreach is not the best way to do this
                }
            }*/
        }

        public void SendContentPub( Event evt, string name ) {
            brokers[ 0 ].SendContentPub( evt, name );
            /*foreach ( IBroker broker in brokers ) {
                try {
                    broker.SendContentPub( evt, name );
                }
                catch ( Exception e ) {
                    // Something went wrong with this broker
                    // Remove broker
                    // Foreach is not the best way to do this
                }
            }*/
        }

        public void Subscribe( string processname, string topic ) {
            brokers[ 0 ].Subscribe( processname, topic );
            /*foreach ( IBroker broker in brokers ) {
                try {
                    broker.Subscribe( processname, topic );
                }
                catch ( Exception e ) {
                    // Something went wrong with this broker
                    // Remove broker
                    // Foreach is not the best way to do this
                }
            }*/
        }

        public void SubscribeBroker( string processname, string topic ) {
            brokers[ 0 ].SubscribeBroker( processname, topic );
            /*foreach ( IBroker broker in brokers ) {
                try {
                    broker.SubscribeBroker( processname, topic );
                }
                catch ( Exception e ) {
                    // Something went wrong with this broker
                    // Remove broker
                    // Foreach is not the best way to do this
                }
            }*/
        }

        public void Unsubscribe( string processname, string topic ) {
            brokers[ 0 ].Unsubscribe( processname, topic );
            /*foreach ( IBroker broker in brokers ) {
                try {
                    broker.Unsubscribe( processname, topic );
                }
                catch ( Exception e ) {
                    // Something went wrong with this broker
                    // Remove broker
                    // Foreach is not the best way to do this
                }
            }*/
        }

        public void UnsubscribeBroker( string processname, string topic ) {
            brokers[ 0 ].UnsubscribeBroker( processname, topic );
            /*foreach ( IBroker broker in brokers ) {
                try {
                    broker.Unsubscribe( processname, topic );
                }
                catch ( Exception e ) {
                    // Something went wrong with this broker
                    // Remove broker
                    // Foreach is not the best way to do this
                }
            }*/
        }
    }
}
