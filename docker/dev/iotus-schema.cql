/** 
 * okin/iotus database schema
 
 To initialize:
 cqlsh localhost  -k {keyspace} < db/iotus-dev.cql
 or 
 cqlsh IP_ADDRESS -k {keyspace} < db/iotus-dev.cql

*/

/**
 * Tables for user/group/project management.
 */
CREATE TABLE user (
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


/**
 * Nodes/Metadata table for project metadata/nodes, including:
 * node metadata:
   - sites
   - equips
   - points
   - etc.

 */
CREATE TABLE node (
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
  -- string properties
  props map<text, text>,
  -- number (int or float) properties
  propsn map<text, float>,
  -- boolean properties
  propsb map<text, boolean>,
  PRIMARY KEY((pid), id)
);

CREATE INDEX node_deleted_key ON node (deleted);

-- Lucene index
CREATE CUSTOM INDEX IF NOT EXISTS node_index
ON node ()
USING 'com.stratio.cassandra.lucene.Index'
WITH OPTIONS = {
    'refresh_seconds'       : '60',
    'ram_buffer_mb'         : '64',
    'max_merge_mb'          : '5',
    'max_cached_mb'         : '30',
    'schema' : '{
        fields : {

            created     : {type : "date", pattern: "yyyy-MM-dd HH:mm:ss.SSSZ"},
            mod         : {type : "date", pattern: "yyyy-MM-dd HH:mm:ss.SSSZ"},
            author      : {type : "string"},
            deleted     : {type : "boolean"},

            pid         : {type : "string"},
            tags        : {type : "string"},
            props       : {type : "string"},
            propsn      : {type : "float"}
        }
    }'
};

/**
 * Time series/readings/measurement/sample table.
 */
CREATE TABLE measurement (
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
CREATE TABLE rollup (
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
CREATE TABLE project (
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

CREATE INDEX project_pid_key ON project (pid);
CREATE INDEX project_tags_key ON project (tags);
