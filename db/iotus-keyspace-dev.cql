/**
 * Keyspace - dev version
 * may need to adjust the actual keyspace name
 */
CREATE KEYSPACE iotus WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

