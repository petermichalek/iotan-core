/** 
 * okin/iotus database schema
 
 Experiments with lucene based schema

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
--             mod         : {type : "date", pattern: "yyyy-MM-dd"},

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

