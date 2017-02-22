/**
 * Keyspace - production version
 * may need to adjust the actual keyspace name
 */
CREATE KEYSPACE iotus WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 2 };
