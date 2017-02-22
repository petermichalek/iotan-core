/** 
 * okin/iotus database schema
 
 To initialize:
 cqlsh localhost < db/iotus-dev.cql
 or 
 cqlsh {IP} < db/iotus-dev.cql

*/


/*
Note: this file is now obsolete
See:
-rw-r--r--  1 peter  staff    152 Feb  6 15:45 iotus-keyspace-prod.cql
-rw-r--r--  1 peter  staff    137 Feb  6 15:45 iotus-keyspace-dev.cql
-rw-r--r--  1 peter  staff  16274 Feb  6 16:06 iotus-populate.cql
-rw-r--r--  1 peter  staff   5270 Feb  6 16:04 iotus-schema.cql
*/



/**
 * Keyspace - dev version
 *
 */
CREATE KEYSPACE IF NOT EXISTS iotus WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
USE iotus;


/**
 * Tables for user/group/project management.
 */
/* disabled or now
CREATE TABLE IF NOT EXISTS user (
  // point id
  id uuid,

  // ---- audit fields
  // -----------------
  // created timestamp
  created timestamp,
  // last modification timestamp
  mod timestamp,
  // author of last modification
  author text,
  // deleted flag
  deleted boolean,
  // -----------------
  // ---- end of audit fields
  email text,
  firstname text,
  lastname text,
  password text,
  // user tags to be used for simplified user profile
  tags set<text>,
  // properties/attributes to be used for user profile
  props map<text, text>,
  PRIMARY KEY(id, mod)
) WITH CLUSTERING ORDER BY ( mod ASC);

CREATE INDEX email_key ON user (email);
CREATE INDEX user_tags_key ON user (tags);
CREATE INDEX user_props_key ON user (props);

*/

/**
 * Metadata table for project metadata/nodes, including:
 * node metadata:
   - sites
   - equips
   - points
   - etc.

 */
CREATE TABLE IF NOT EXISTS node (
  // point id
  id uuid,
  // ---- audit fields
  // -----------------
  // created timestamp
  created timestamp,
  // last modification timestamp
  mod timestamp,
  // author of last modification
  author text,
  // deleted flag
  deleted boolean,
  // ------------------------
  // ---- end of audit fields

  // project id to which this record belongs
  // corresponds to the project pid in project table
  pid text,
  tags set<text>,
  keyss set<text>,
  keysi set<text>,
  keysb set<text>,
  //props map<text, text>,
  json text,
  PRIMARY KEY(id, mod)
) WITH CLUSTERING ORDER BY ( mod ASC);

CREATE INDEX node_tags_key ON node (tags);
CREATE INDEX pid_key ON node (pid);

/**
 * Time series/readings/measurement/sample table.
 */
CREATE TABLE IF NOT EXISTS measurement (
  // node id, corresponding to the node id/primary key in node table
  id text,
  // project id to which this record belongs
  // corresponds to the project pid in project table
  pid text,
  // timestamp 
  ts timestamp,
  // day part of timestamp
  // date TEXT,
  // value 
  val TEXT,
  PRIMARY KEY(id, ts)
) WITH CLUSTERING ORDER BY ( ts ASC);

CREATE INDEX measurement_pid_key ON measurement (pid);

/**
 * Time series rollup table.
 * Used for on-demand and pre-computed rollups, calculated from time series table.
 * This table doesn't need to be archived, since it can be calculaterd from readings/time series.
*/
CREATE TABLE IF NOT EXISTS rollup (
  // point id 
  id text,
  // timestamp (beginning of rollup interval) 
  ts timestamp,
  // interval, in minutes (60*24=1440 for day)
  interval int,
  // value 
  minval float, 
  maxval float, 
  avgval float, 
  sum float, 
  // count
  count int,

  // ---- reduced audit fields
  // --------------------------------
  // created timestamp
  created timestamp,
  // last modification timestamp
  mod timestamp,
  // author of last modification
  author text,
  // --------------------------------
  // ---- end of reduced audit fields

  PRIMARY KEY(id, ts)
) WITH CLUSTERING ORDER BY ( ts ASC);



/**
 * Project table for entities, including:
 */
CREATE TABLE IF NOT EXISTS project (
  // project uuid
  id uuid,

  // ---- audit fields
  // -----------------
  // created timestamp
  created timestamp,
  // last modification timestamp
  mod timestamp,
  // author of last modification
  author text,
  // deleted flag
  deleted boolean,
  // ------------------------
  // ---- end of audit fields

  // project id which has to be limited to web url frienly names.
  // dashes are allowed, such as project-test, my-project etc.
  // it's recommended that it's all lowercase, starting with a alpha character
  pid text,

  // short name/id: it's shorter than description, but allows any ascii characters,
  //   unlike pid
  name text,

  // nominal interval for sampling, in seconds, e.g. 900 for 15 minutes
  nominalInterval int,

  // timezone as Java/Python timezone id format, e.g. America/Los_Angeles
  tz text,

  // geo fields for address etc.
  geoAddr text,
  geoCity text,
  geoZip text,

  // project description
  description text,
  // project tags
  tags set<text>,
  // group map with read/write/execute permissions, such as:
  // admin-group: rwx
  // guest-group: r
  groups map<text, text>,
  // other miscellanious properties
  json text,
  PRIMARY KEY(id)
);

//) WITH CLUSTERING ORDER BY ( mod ASC);

CREATE INDEX pid_key ON project (pid);
CREATE INDEX project_tags_key ON project (tags);
