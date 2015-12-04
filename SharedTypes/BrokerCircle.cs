using System;
using System.Collections.Generic;
using System.Text;

namespace SESDAD {
    // Implements the same functions as an IBroker to wrap the calls
    public class BrokerCircle : IBroker {
        public List<IBroker> brokers;
        public List<int> ids;
        public string name;

        private object circleLock = new object();

        private int testIndex = 0;

        public BrokerCircle( string circleName ) {
            name = circleName;
            brokers = new List<IBroker>();
            ids = new List<int>();
        }

        public void AddBroker( IBroker broker, int id ) {
            brokers.Add( broker );
            ids.Add( id );
        }

        public void InformOfDeath( int replicaIndex )
        {
            Console.WriteLine( "Replica death: " + replicaIndex );
            for ( int i = 0; i < brokers.Count; ++i ) {
                if ( ids[ i ] == replicaIndex ) { continue; }
                brokers[ i ].InformOfDeath( replicaIndex );
            }
            Console.WriteLine( "New leader: " + testIndex );
        }

        // Assumindo que os nos falhados existem apenas no intervalo [0,testIndex[
        // Caso a solucao seja alterada para qualquer no ser substituido
        // E necessario remover da lista os nos que falharam
        public void InformNeighbourDeath( string circleName, int replicaId ) {
            Console.WriteLine( "Neighbour death: " + circleName + " " + replicaId );
            for ( int i = testIndex; i < brokers.Count; ++i ) {
                brokers[ i ].InformNeighbourDeath( circleName, replicaId );
            }
        }
        public void NewCircleLeader( int replicaId ) {
            testIndex = replicaId;
        }

        public void MakeLeader()
        {
            throw new NotImplementedException();
        }

        public void SendContent( Event evt, string name ) {
            //lock ( circleLock ) {
                try {
                    brokers[ testIndex ].SendContent( evt, name );
                }
                catch ( Exception e ) {
                    Console.WriteLine( "Error sending event: " + e.Message );
                    int indexOfDeath = testIndex;
                    testIndex++;
                    InformOfDeath( indexOfDeath );
                    brokers[ testIndex ].MakeLeader();
                    SendContent( evt, name );
                }
            //}
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
            //lock ( circleLock ) {
                try {
                    brokers[ testIndex ].SendContentPub( evt, name );
                }
                catch ( Exception e ) {
                    Console.WriteLine( "Error sending event: " + e.Message );
                    int indexOfDeath = testIndex;
                    testIndex++;
                    InformOfDeath( indexOfDeath );
                    brokers[ testIndex ].MakeLeader();
                    SendContentPub( evt, name );
                }
            //}
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
            //lock ( circleLock ) {
                try {
                    brokers[ testIndex ].Subscribe( processname, topic );
                }
                catch ( Exception e ) {
                    Console.WriteLine( "Error subscribing: " + e.Message );
                    int indexOfDeath = testIndex;
                    testIndex++;
                    InformOfDeath( indexOfDeath );
                    brokers[ testIndex ].MakeLeader();
                    Subscribe( processname, topic );
                }
            //}
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
            //lock ( circleLock ) {
                try {
                    brokers[ testIndex ].SubscribeBroker( processname, topic );
                }
                catch ( Exception e ) {
                    Console.WriteLine( "Error subscribing: " + e.Message );
                    int indexOfDeath = testIndex;
                    testIndex++;
                    InformOfDeath( indexOfDeath );
                    brokers[ testIndex ].MakeLeader();
                    SubscribeBroker( processname, topic );
                }
            //}
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
            //lock ( circleLock ) {
                try {
                    brokers[ testIndex ].Unsubscribe( processname, topic );
                }
                catch ( Exception e ) {
                    Console.WriteLine( "Error unsubscribing: " + e.Message );
                    int indexOfDeath = testIndex;
                    testIndex++;
                    InformOfDeath( indexOfDeath );
                    brokers[ testIndex ].MakeLeader();
                    Unsubscribe( processname, topic );
                }
            //}
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
            //lock ( circleLock ) {
                try {
                    brokers[ testIndex ].UnsubscribeBroker( processname, topic );
                }
                catch ( Exception e ) {
                    Console.WriteLine( "Error unsubscribing: " + e.Message );
                    int indexOfDeath = testIndex;
                    testIndex++;
                    InformOfDeath( indexOfDeath );
                    brokers[ testIndex ].MakeLeader();
                    UnsubscribeBroker( processname, topic );
                }
            //}
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
