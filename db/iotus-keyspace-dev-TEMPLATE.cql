/**
 * Keyspace - dev version
 * may need to adjust the actual keyspace name
 */
CREATE KEYSPACE {IOTUS_KEYSPACE} WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

