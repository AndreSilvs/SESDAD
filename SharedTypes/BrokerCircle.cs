using System;
using System.Collections.Generic;
using System.Text;

namespace SESDAD {
    // Implements the same functions as an IBroker to wrap the calls
    public class BrokerCircle : IBroker {
        public List<IBroker> brokers;
        public string name;

        private int testIndex = 2;

        public BrokerCircle( string circleName ) {
            name = circleName;
            brokers = new List<IBroker>();
        }

        public void AddBroker( IBroker broker ) {
            brokers.Add( broker );
        }

        public void SendContent( Event evt, string name ) {
            brokers[ testIndex ].SendContent( evt, name );
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
            try {
                brokers[ testIndex ].SendContentPub( evt, name );
            }
            catch ( Exception e ){
                Console.WriteLine( "Error sending event: " + e.Message );
            }
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
            brokers[ testIndex ].Subscribe( processname, topic );
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
            brokers[ testIndex ].SubscribeBroker( processname, topic );
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
            brokers[ testIndex ].Unsubscribe( processname, topic );
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
            brokers[ testIndex ].UnsubscribeBroker( processname, topic );
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
