package iotus.core

import collection.JavaConversions._
import scala.collection.{JavaConversions, mutable}
import java.time.{ZoneId, ZonedDateTime}
import java.util
import java.util.{Date, TimeZone, UUID}

import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.QueryBuilder
import org.projecthaystack.{CassandraEval, GridWrap, HBool, HDateTime, HDict, HDictBuilder, HFilter, HGrid, HGridBuilder, HNum, HRef, HStr, HTimeZone, HVal}

import scala.util.Try

//import io.getquill._
//import io.getquill.context.cassandra.CassandraSessionContext

//import scala.concurrent.Future
//import scala.reflect.ClassTag
import model.User
import model.Project
import model.BaseEntity

/*
INSERT INTO node (id, created, mod, author, deleted, pid, tags, props, propsn, json)
      VALUES (
          756716f7-2e54-4715-9f00-91dcbea6c101,
          '2016-11-11T01:00:00-0800',
          '2016-11-11T01:00:00-0800',
          'sysdeamon',
          false,
          'test-project',
          {'point', 'his'},
          {'dis': 'test pt 02', 'equipRef': '756716f7-2e54-4715-9f01-91dcbea6c300',
            'kind': 'Number', 'tz': 'Los_Angeles'},
          {'hisSize': 10},
          '{}'
          -- '{"dis": "test pt 02", "equipRef": "756716f7-2e54-4715-9f01-91dcbea6c300",
          --  "hisSize": 10, "kind": "Number", "tz": "Los_Angeles"}'
        );
 */

/**
  * Assists in building cassandra queries for store operations.
  */
object CassandraQueryBuilder {

}

// Helper/utility functions for cassandra queries
object CassandraHelper {

  val jsontest_text = """{"id":"r:0003fd1e-fbec-4fc2-a58d-658e4b46bf1f RTU5 zoneCoolROCL", "hisStart":"s:2016-04-26T05:00:00.808Z UTC", "equipRef":"r:RTU05", "point":"m:", "his":"m:", "hisSize":"n:19056", "hisEndVal":"0.00933922", "tz":"Chicago", "cxId":"571fe542acacd9424c9198eb", "hisEnd":"s:2017-02-02T17:30:00.191Z UTC", "rateOfChange":"m:", "import":"m:", "temp":"m:", "haystackHis":"H.SuperRoscoeHighSchool.RTU5_zoneCoolROCL", "navName":"zoneCoolROCL", "cool":"m:", "siteRef":"r:RoscoeSchoolDistrict", "createdBy":"5461d73acb0a46150ed1cbf7", "dis":"RTU5 zoneCoolROCL", "unit":"ROC/min", "loss":"m:", "kind":"Number", "bldgRef":"r:RoscoeHighSchool", "zone":"m:"}"""

  val COMMIT_QUERY_INSERT =
    s"""|INSERT INTO node
        | (id, created, mod, author, deleted, pid, tags, props, propsn, propsb) VALUES
        | (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        |""".stripMargin
  val COMMIT_QUERY_ENRICH =
    s"""|UPDATE node SET json = ?, mod = ?, author = ? WHERE pid = ? and id = ?
        |""".stripMargin

  def gridToEnrichQueryUpdate(frame: IMFrame, session: Session, author: String, pid: String): (List[BoundStatement], IMFrame) = {
    val gridWrap: GridWrap = new GridWrap(frame.toGrid)
    val boundStatements: mutable.ListBuffer[BoundStatement] = mutable.ListBuffer()
    val queryEnrichPrepared = session.prepare(COMMIT_QUERY_ENRICH)
    val mod = new Date()
    for (row <- gridWrap.srows) {
      val id:HVal = row.getOrElse("id", HStr.make("unknown"))
      val bound: BoundStatement = queryEnrichPrepared.bind()
      // reset json field
      bound.bind("", mod, author, pid, UUID.fromString(id.toString))
      // performance test: set json field to some text
      //bound.bind(jsontest_text, mod, author, pid, UUID.fromString(id.toString))
      boundStatements.append(bound)
    }
    (boundStatements.toList, IMFrame(HGrid.EMPTY, isHis = false))
  }

    /**
    * Response frame/Grid contains response to be set to client, containing ids of newly created records
    *
    * @param frame
    * @param session
    * @param author
    * @param pid
    * @return tuple(boundstatement, response grid)
    */
  def gridToCommitQueryInsert(frame: IMFrame, session: Session, author: String, pid: String): (List[BoundStatement], IMFrame) = {

    val gridWrap: GridWrap = new GridWrap(frame.toGrid)
    val queryCommitPrepared = session.prepare(COMMIT_QUERY_INSERT)
    val boundStatements: mutable.ListBuffer[BoundStatement] = mutable.ListBuffer()
    val created = new Date()
    val gridBuilder = new HGridBuilder()
    gridBuilder.addCol("id")

    val allTags = frame.typeMap.get("tags").get.toSet
    val allProps = frame.typeMap.get("props").get.toSet
    val allPropsn = frame.typeMap.get("propsn").get.toSet
    val allPropsb = frame.typeMap.get("propsb").get.toSet
    val columnNames: List[String] = gridWrap.colNames
    if (gridWrap.colNames.contains("id")) {
      // check that ids don't exist
      val uuids = gridWrap.srows map { x => x.get("id") }
      if (uuids.length != gridWrap.srows.length) {
        throw new RuntimeException(s"Either all or no grid rows have to contain id. ids: ${uuids.length} grid rows: ${gridWrap.srows.length}.")
      }

      if (uuids.length > uuids.distinct.length) {
        throw new RuntimeException(s"Ids in the id column should be unique.")
      }
      val ids = uuids map { id =>
        UUID.fromString(
          id.get.toString
        )
      }
      /*
      for (id <- ids) {
        println(id)
      }*/
      // TODO: check if ids already in the database
      /*
        if (uuids.length != ids.length) {
          throw new RuntimeException(s"Some ids are not UUIDs: $ids")
        }
      val queryReadByIdsPrepared = session.prepare(QUERY_READBYIDS_TEMPLATE.format(DEFAULT_LIMIT))
      val bound: BoundStatement = queryReadByIdsPrepared.bind()
      bound.bind(uuids)
      val rs = session.execute(bound)

      if (rs.all.length > 0) {
        val existingIds = rs.all....
        throw new RuntimeException(s"Some ids already exist in the database: $diffIds")
        val queryReadByIdsPrepared = session.prepare(QUERY_READBYIDS_TEMPLATE.format(DEFAULT_LIMIT))

        }

      }
      */
    }

    for (row <- gridWrap.srows) {
      val uuid:UUID = {
        if (gridWrap.colNames.contains("id")) {
          UUID.fromString(row.get("id").get.toString)
        } else {
          UUID.randomUUID()
        }
      }
      val bound: BoundStatement = queryCommitPrepared.bind()
      //println("row: " + row)
      var tagSet: mutable.Set[String] = mutable.Set()
      var propsMap: mutable.HashMap[String, String] = mutable.HashMap()
      var propsnMap: mutable.HashMap[String, java.lang.Float] = mutable.HashMap()
      var propsbMap: mutable.HashMap[String, java.lang.Boolean] = mutable.HashMap()
      // iterate cells in row
      for ((name,hval) <- row ) {
        if (allTags.contains(name)) {
          tagSet.add(name)
        } else if (allProps.contains(name)) {
          propsMap.put(name, hval.toString)
        } else if (allPropsn.contains(name)) {
          //println(s"name=$name, hval: $hval")
          if (hval.isInstanceOf[HNum]) {
            propsnMap.put(name, hval.asInstanceOf[HNum].`val`.toFloat)
          } else {
            //TODO: do proper fix: mis-match in row field and expected column type by
            //   checking other types. e.g. for propsn, if value is bool, convert to 0/1
            //   for string, convert to string
            /*
            if (hval.isInstanceOf[HBool]) {
              propsnMap.put(name, if (hval.asInstanceOf[HBool].`val`) 1.0.toFloat else 0.0.toFloat)
            } else {

            }
            */
            println(s"Problem: HNum expected for name=$name, hval: $hval. Will render as string")
            propsMap.put(name, hval.toString)
          }
        } else if (allPropsb.contains(name)) {
          propsbMap.put(name, hval.asInstanceOf[HBool].`val`)
        }
      }
      //val tags: Set[String] = frame.typeMap.get("tags").get.toSet
      val jtags = JavaConversions.setAsJavaSet[String](tagSet)
      val props = JavaConversions.mapAsJavaMap(propsMap)
      val propsn = JavaConversions.mapAsJavaMap(propsnMap)
      val propsb = JavaConversions.mapAsJavaMap(propsbMap)
      //performance test with json field
      //bound.bind(uuid, created, created, author, false:java.lang.Boolean, pid, jtags, props, propsn, propsb, jsontest_text)
      bound.bind(uuid, created, created, author, false:java.lang.Boolean, pid, jtags, props, propsn, propsb)
      boundStatements.append(bound)
      gridBuilder.addRow(Array(HRef.make(uuid.toString)))
    }
    /*
    for (cell <- dict1.iterator) {
      dumpIfVerbose("cell: " + cell)
      val name:String = cell.asInstanceOf[java.util.Map.Entry[String, Any]].getKey
      grid1.col(name) should not be null
    }
    // resolve types to determine which map to save to
    //val (props, propsn, propsb) = gridToProps
    //frame.
    */
    (boundStatements.toList, IMFrame(gridBuilder.toGrid, false))
  }

}

/**
  * Performs conversions between database rows/statements and database independent entities.
  */
object EntityConverter {

  /**
    * Returns  insert/modify CQL statement and arguments for given entity.
    * @param entity
    * @param table
    * @return Tuple of (statement, values)
    */
  def entityToStatement[T](entity: BaseEntity, table: String): (String, Seq[Any]) = {
    table match {
      case "project" =>
        /*
        val id = row.getUUID("id").toString
        val pid = row.getString("pid")
        val tzstr = row.getString("tz")
        val p = new Project(id, pid, tzstr)
        Option(p)
        */
        val project = entity.asInstanceOf[Project]
        val dt = new Date()
        val id = UUID.fromString(project.id)
        // create new project or modifying existing project?
        if (project.savedId == None) {
          (s"INSERT INTO $table (id, pid, tz, mod, created) VALUES (?, ?, ?, ?, ?)", Seq(id, project.pid, project.tzstr, dt, dt))
        } else {
          (s"UPDATE $table set pid=?, tz=?, mod=? where id=?", Seq(project.pid, project.tzstr, dt, id))
        }
      case _ => throw new RuntimeException("table not recognized: " + table)
    }
  }
  //  BoundStatement bound = prepared.bind();


  def rows2Entities(rows: List[Row], table: String): List[BaseEntity] = {
    val entities: List[Option[BaseEntity]] = rows.map(x => row2Entity(x, table))
    //List()
    //for (row <- rows) {
    //}
    entities.flatten
  }


  // val info = new DataLine.Info(classOf[TargetDataLine], null)

  /**
    * Converts database row to an entity instance
    * @param row
    * @param table table name/entity name
    * @return
    */
  def row2Entity(row: Row, table: String): Option[BaseEntity]= {

    /*
    var entity: Option[Any] = None
    if (table == "project") {
      val id = row.getUUID("id").toString
      val pid = row.getString("pid")
      val tzstr = row.getString("tz")
      val p = Project(id, pid, tzstr)
      entity = Option(p)
    }
    else if (table == "user") {
      val id = row.getUUID("id").toString
      /*
      val tzstr = row.getString("tz")
      val p = User(id, first_name, last_name)
      entity = Option(p)
      */
    } else {
      Option("Invalid table: " + table)
    }
    */

    /*
    match instead of if
    val entity: Option[Any] =
     */
    //val entity: Option[Any] =
    table match {
      case "project" =>
        /*
        val id = row.getUUID("id").toString
        val pid = row.getString("pid")
        val tzstr = row.getString("tz")
        val p = new Project(id, pid, tzstr)
        Option(p)
        */
        //val instant = row.getDate("mod").getMillisSinceEpoch()
        //val instant = row.getTimestamp("mod").toInstant()
        val mod = if (row.getTimestamp("mod") != null) Some(ZonedDateTime.ofInstant(row.getTimestamp("mod").toInstant(), ZoneId.of("UTC")))
                  else None
        if (row.getBool("deleted")) None else Option(new Project(row.getUUID("id").toString, row.getString("pid"), row.getString("tz"), mod))
      case "user" =>
        /*
        val id = row.getUUID("id").toString
        val pid = row.getString("pid")
        val tzstr = row.getString("tz")
        val p = new Project(id, pid, tzstr)
        Option(p)
        */
        //val instant = row.getDate("mod").getMillisSinceEpoch()
        //val instant = row.getTimestamp("mod").toInstant()
        val mod = if (row.getTimestamp("mod") != null) Some(ZonedDateTime.ofInstant(row.getTimestamp("mod").toInstant(), ZoneId.of("UTC")))
        else None
        if (row.getBool("deleted")) None else Option(new User(row.getUUID("id").toString,
            row.getString("email"),
            row.getString("firstname"),
            row.getString("lastname"),
            row.getString("password"),
          mod))
      case _ => None
      //case _ => Option("Invalid table: " + table)
    }

    //entity
  }

}

case class StoreSession(val dbSession: Session) {
  def close: Unit = dbSession.close
}

object StoreSession {

  /**
    * Factory method to make database session (cassandra session)
    * @param storeUri
    * @return
    */
  def make(storeUri: String): StoreSession = {
    val uri = CassandraConnectionUri(storeUri)
    val defaultConsistencyLevel: ConsistencyLevel = QueryOptions.DEFAULT_CONSISTENCY_LEVEL
    val cluster = new Cluster.Builder().
      addContactPoints(uri.hosts.toArray: _*).
      withPort(uri.port).
      withQueryOptions(new QueryOptions().setConsistencyLevel(defaultConsistencyLevel)).build
    val dbSession = cluster.connect
    dbSession.execute(s"USE ${uri.keyspace}")
    new StoreSession(dbSession)
  }
}


trait Store {
  val DEFAULT_LIMIT = 10000

  /**
    * Delete one or more records.
    * If operation fails and more than one records where specified, no records will be deleted.
    * @param metaFrame specifies on ore more records to be deleted
    * @param project
    * @return Left (Unit) on success, Error message on failure
    */
  def delete(metaFrame: IMFrame, project: String): Either[Unit, String]
  /**
    * Delete records.
    * If operation fails and more than one records where specified, no records will be deleted.
    * @param ids
    * @param project
    * @return Left (Unit) on success, Error message on failure
    */
  def delete(ids: Array[String], project: String): Either[Unit, String]

  /**
    * Delete one record.
    * @param id
    * @param project
    * @return Left (Unit) on success, Error message on failure
    */
  def delete(id: String, project: String): Either[Unit, String]

  /**
    * Commit (create or update) a record.
    * @param metaFrame
    * @param op
    * @param project
    * @return
    */
  def commit(metaFrame: IMFrame, op: String, project: String, author: String): Either[IMFrame, String]

  def experiment1(metaFrame: IMFrame, project: String, author: String): Either[IMFrame, String]

  def count(filter: String, project: String, limit: Int=0): Int

  def readAll(filter: String, project: String, limit: Int=0): IMFrame
  def readById(id: String, project: String): IMFrame
  //def readByIds(ids: List[String], project: String): IMFrame
  /**
    * read history for given range
    *
    * @param metaFrame
    * @param range: start-exclusive, end-inclusive
    * @param project
    * @param tz
    * @param limit
    * @return
    */
  def hisRead(metaFrame: IMFrame, range: (ZonedDateTime, ZonedDateTime),
              project: String, tz: TimeZone, limit: Int = 0): Option[IMFrame]
  def hisWrite(metaFrame: IMFrame, hisFrame: IMFrame, project: String): Either[Unit, String]

  /**
    * Clear history for given (optional) range
    * @param metaFrame
    * @param range: start-exclusive, end-inclusive
    * @return
    */
  def hisClear(metaFrame: IMFrame, range: Option[(ZonedDateTime, ZonedDateTime)] = None, project: String): Either[String, Int]
  //def hisClear(id: String, range: Option[(ZonedDateTime, ZonedDateTime)], project: String): Either[String, Int]

}


/**
  * Metadata and history store for Cassandra
  */
class CassandraStore(val uri:CassandraConnectionUri) extends Store {
  val READ_FIELDS = "id, mod, author, tags, props, propsn, propsb"
  val QUERY_INSERT = String.format("INSERT INTO measurement (id, ts, val, pid) VALUES (?, ?, ?, ?)");
  // cannot search on non-pripery key pid
  //val QUERY_DELETE_HIS_ALL = String.format("DELETE FROM measurement where id = ? and pid = ?");
  val QUERY_DELETE_HIS_ALL = "DELETE FROM measurement WHERE id = ?"
  val QUERY_DELETE_HIS = "DELETE FROM measurement WHERE id = ? AND ts = ?"
  // TODO: optimize - remove allowFiltering, replace by lucene expression
  //val QUERY_READALL_TEMPLATE = "SELECT * FROM node WHERE pid = ? AND deleted = false AND expr(node_index, ?) LIMIT %s ALLOW FILTERING"
  /*
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
   */
  val QUERY_COUNT_TEMPLATE = s"SELECT COUNT(id) FROM node WHERE pid = ? AND deleted = false AND expr(node_index, ?) LIMIT %s ALLOW FILTERING"
  val QUERY_READALL_TEMPLATE = s"SELECT $READ_FIELDS FROM node WHERE pid = ? AND deleted = false AND expr(node_index, ?) LIMIT %s ALLOW FILTERING"
  val QUERY_READBYIDS_TEMPLATE = s"SELECT $READ_FIELDS FROM node WHERE pid = ? AND id IN ? deleted = false AND LIMIT %s ALLOW FILTERING"
  val QUERY_READBYID_TEMPLATE = s"SELECT $READ_FIELDS FROM node WHERE pid = ? AND deleted = false AND id = ?"
  val QUERY_DELETE_NODE_ONE = "DELETE FROM node WHERE pid = ? and id = ?"
  val QUERY_DELETE_NODE_MANY = "DELETE FROM node WHERE pid = ? and id IN ( ? )"
  val QUERY_MARK_DELETED_NODE_ONE = "UPDATE node SET deleted = true, mod = ? WHERE pid = ? and id = ?"
  val defaultConsistencyLevel: ConsistencyLevel = QueryOptions.DEFAULT_CONSISTENCY_LEVEL

  private val cluster = new Cluster.Builder().
    addContactPoints(uri.hosts.toArray: _*).
    withPort(uri.port).
    withQueryOptions(new QueryOptions().setConsistencyLevel(defaultConsistencyLevel)).build
  private val session = cluster.connect
  session.execute(s"USE ${uri.keyspace}")


  //val queryDeleteNodeOnePrepared = session.prepare(QUERY_DELETE_NODE_ONE)
  val queryMarkDeletedNodeOnePrepared = session.prepare(QUERY_MARK_DELETED_NODE_ONE)

  override def readAll(filter: String, project: String, limit: Int=0): IMFrame = {
    val actualLimit: Integer = if (limit == 0) DEFAULT_LIMIT else limit
      if (filter.startsWith("@")) {
        // allow non-axon expression in filter
        readById(filter.substring(1), project)
      } else {
        val hfilter = HFilter.make(filter)
        val jsonquery = CassandraEval.axonToString(hfilter)
        //CassandraEval.axonToString(hfilter)
        val queryReadallPrepared = session.prepare(QUERY_READALL_TEMPLATE.format(actualLimit))
        val bound: BoundStatement = queryReadallPrepared.bind()
        bound.bind(project, jsonquery)
        //println("%s: store.readAll: invoking session.execute".format(new Date()))
        val rs = session.execute(bound)
        //println("%s: store.readAll: invoking rs.all".format(new Date()))
        val rows:util.List[Row] = rs.all()
        //println("%s: store.readAll: done invoking session.execute".format(new Date()))
        val frame = IMFrame(rows)
        frame
      }
  }

  override def count(filter: String, project: String, limit: Int=0): Int = {
    val actualLimit: Integer = if (limit == 0) DEFAULT_LIMIT else limit
    val hfilter = HFilter.make(filter)
    val jsonquery = CassandraEval.axonToString(hfilter)
    //CassandraEval.axonToString(hfilter)
    val queryCountPrepared = session.prepare(QUERY_COUNT_TEMPLATE.format(actualLimit))
    val bound: BoundStatement = queryCountPrepared.bind()
    bound.bind(project, jsonquery)
    //println("%s: store.readAll: invoking session.execute".format(new Date()))
    val rs = session.execute(bound)
    //println("%s: store.readAll: invoking rs.all".format(new Date()))
    val record: Row = rs.one()
    return record.getLong("system.count(id)").toInt
  }

  override def readById(id: String, project: String): IMFrame = {
    // TODO: optimize - remove allowFiltering, replace by lucene expression
    /*
    val selectStmt = QueryBuilder.select()
      .from("node").allowFiltering()
      .where(QueryBuilder.eq("pid", project))
      .and(QueryBuilder.eq("deleted", false))
      .and(QueryBuilder.eq("id", UUID.fromString(id)))


    val resultSet = session.execute(selectStmt)
    */
    val queryPrepared = session.prepare(QUERY_READBYID_TEMPLATE)
    val bound: BoundStatement = queryPrepared.bind()
    bound.bind(project, UUID.fromString(id))
    //println("%s: store.readById: invoking session.execute".format(new Date()))
    val rs = session.execute(bound)
    val rows:util.List[Row] = rs.all()
    // convert to frame
    val frame = IMFrame(rows)
    frame
  }

  //         hisitems = hisRead(point['id'], hrange, truncate_seconds=truncate_seconds, read_extra=read_extra, limit=limit)
  //def _hisRead(self, point, hrange, read_extra=False, truncate_seconds=True, limit=None): # , rollup_interval=None, rollup_alg="avg"):
  // def hisRead(self, hrange, point_spec=None, rollup_interval=None, rollup_alg=None, truncate_seconds=True, limit=None):
  // TODO: add support for:
  // rollup_interval=None, rollup_alg=None, truncate_seconds=True, limit=None
  /**
    * Performs hisRead (history read)
    * @param metaFrame contains points for which hisRead is being performed
    * @param range time range for which hisRead should be performed. Range is start exclusive and end inclusive
    * @param project project pid to which data belongs
    * @param limit limit of records/rows to return 0 of system default should be used
    * @return Option of result history frame or None if empty
    */
  override def hisRead(metaFrame: IMFrame, range: (ZonedDateTime, ZonedDateTime), project: String, tz: TimeZone, limit: Int = 0): Option[IMFrame] = {

    // get point from metaFrame
    val metaGrid = metaFrame.toGrid

    //if (metaGrid.numRows != 1) throw new RuntimeException("metaFrame must contain exactly one point for hisRead to proceed")

    val metaBuilder = new HDictBuilder()
    val start:Date = Date.from(range._1.toInstant)
    val end:Date = Date.from(range._2.toInstant)
    metaBuilder.add("start", HDateTime.make(start.getTime(), HTimeZone.make(tz)))
    metaBuilder.add("end", HDateTime.make(end.getTime(), HTimeZone.make(tz)))
    //val gridBuilder = new HGridBuilder()
    //gridBuilder.addCol("ts")
    var tslist: mutable.ArrayBuffer[Map[Date, String]] = mutable.ArrayBuffer()
    //val points =
    //var rowAccum: java.util.List[util.List[Row]] = new java.util.LinkedList[util.List[Row]]()
    if (metaGrid.numRows > 0) {
      // process invidivudal points
      for (i <-  0 until metaGrid.numRows) {
        val row = metaGrid.row(i)
        val id = row.get("id").toString

        //val colName = s"v$i"
        //metaBuilder.add(colName, id)
        //gridBuilder.addCol(colName)

        // TODO: how to append rows to accumulated over several points
        val actualLimit = if (limit == 0) DEFAULT_LIMIT else limit

        val selectStmt = QueryBuilder.select()
          .from("measurement")
          .where(QueryBuilder.eq("pid", project))
          .and(QueryBuilder.eq("id", id))
          .and(QueryBuilder.gt("ts", start))
          .and(QueryBuilder.lte("ts", end)
          ).limit(actualLimit)
        //val resultSet = session.execute(selectStmt)
        //val rows:util.List[Row] = resultSet.all()

        // convert recordset to List of tuples (ts, val)
        val tsRows: collection.mutable.Map[Date, String] = collection.mutable.LinkedHashMap()

        for (record: Row <- session.execute(selectStmt).all()) {
          //ZonedDateTime.ofLocal(LocalDateTime(record.getTimestamp("ts")), ZoneId.of("Europe/Paris"))
          //val ts = ZonedDateTime(record.getTimestamp("ts").getTime)
          val ts = record.getTimestamp("ts")
          val value = record.getString("val")
          tsRows.put(ts, value)
        }
        // GREG * syntactic sugar warnings and other warnings
        /*

Information:12/8/16, 4:07 PM - Compilation completed successfully with 4 warnings in 7s 899ms
Warning:scalac: Class javax.annotation.Nullable not found - continuing with a stub.
/Users/peter/git-clones/iotus-core/src/main/scala/iotus/core/IMContext.scala
Warning:(172, 15) non-variable type argument java.time.ZonedDateTime in type pattern (java.time.ZonedDateTime, java.time.ZonedDateTime) is unchecked since it is eliminated by erasure
      case t: Tuple2[java.time.ZonedDateTime, java.time.ZonedDateTime] => t
/Users/peter/git-clones/iotus-core/src/main/scala/iotus/core/Model.scala
Warning:(143, 22) non-variable type argument String in type pattern scala.collection.immutable.Map[String,Any] (the underlying of Map[String,Any]) is unchecked since it is eliminated by erasure
        case Some(e: Map[String,Any]) => {
Warning:(142, 7) match may not be exhaustive.
It would fail on the following input: None
      parsed match {


         */
        tslist.append(tsRows.toMap)
      }
      val tsgrid = hsutil.tslistToGrid(metaGrid, tslist.toList, start, end, tz)
      Some(IMFrame(tsgrid, isHis=true))
    } else {
      None
    }
  }

  /**
    * Response frame/Grid contains response to be set to client, containing ids of newly created records
    *
    * @param metaFrame
    * @param op
    * @param project
    * @return Either response frame or error message
    */
  override def commit(metaFrame: IMFrame, op: String, project: String, author: String): Either[IMFrame, String] = {
     {
      if (op == "add") {
        try {
          val (boundStatements: List[BoundStatement], frame: IMFrame) = CassandraHelper.gridToCommitQueryInsert(metaFrame, session, author, project)
          for (bound <- boundStatements) {
            val resultSet = session.execute(bound)
          }
          Left(frame)
        } catch {
          case e: Exception =>
            return Right(e.toString)
          //case _ =>
          //  return Right("Exception while builing query: ")
        }
      } else if (op == "update") {
        if (metaFrame.toGrid.col("id", false) != null) {
            Right("id column not allowed in new commit records.")
        } else {
          Right("update not implemented")
        }
      } else {
        Right(s"op not recognized: $op")
      }
     }
  }
  override def experiment1(metaFrame: IMFrame, project: String, author: String): Either[IMFrame, String] = {
    if (metaFrame.toGrid.col("id", false) == null) {
      Right("id column needed for 'enrich records' function.")
    } else {
      val (boundStatements: List[BoundStatement], frame: IMFrame) = CassandraHelper.gridToEnrichQueryUpdate(metaFrame, session, author, project)
      for (bound <- boundStatements) {
        //bound
        val resultSet = session.execute(bound)
      }
      //val frame = IMFrame(rows)
      Left(frame)
    }
  }


  override def delete(metaFrame: IMFrame, project: String): Either[Unit, String] = {
    val wgrid = GridWrap(metaFrame.toGrid)
    val ids = wgrid.srows.map(_.get("id").get.toString).toArray
    delete(ids, project)
  }

  override def delete(ids: Array[String], project: String): Either[Unit, String] = {
    val bound: BoundStatement = queryMarkDeletedNodeOnePrepared.bind()
    for (id <- ids) {
      bound.bind(new Date(), project, UUID.fromString(id))
      val queryResult:ResultSet = session.execute(bound)
    }
    Left()
  }

  override def delete(id: String, project: String): Either[Unit, String] = {
    // instead of purging/deleting from db, just mark as deleted
    val bound: BoundStatement = queryMarkDeletedNodeOnePrepared.bind()
    bound.bind(new Date(), project, UUID.fromString(id))
    val queryResult:ResultSet = session.execute(bound)
    Left()
  }

  override def hisClear(metaFrame: IMFrame, range: Option[(ZonedDateTime, ZonedDateTime)] = None, project: String): Either[String, Int] = {
    // get point from metaFrame
    val metaGrid = metaFrame.toGrid
    val boundStatement: BoundStatement = new BoundStatement(session.prepare(QUERY_DELETE_HIS))

    if (metaGrid.numRows > 0) {
      // process invidivudal points
      for (i <- 0 until metaGrid.numRows) {
        val row = metaGrid.row(i)
        val id = row.get("id").toString

        range match {
          case Some((tstart, tend)) =>
            // clear all entries for given id in date range

          val start: Date = Date.from(tstart.toInstant)
          val end: Date = Date.from(tend.toInstant)

          /*
            Note that range deletes (on the cluster key) are in the next major version (3.0-beta2). Patch got committed to trunk just recently: https://issues.apache.org/jira/browse/CASSANDRA-6237
          on 2.1: do a select first, then delete each result.

           Therefore, first read, then delete
           */

            val selectStmt = QueryBuilder.select()
              .from("measurement")
              .where(QueryBuilder.eq("pid", project))
              .and(QueryBuilder.eq("id", id))
              .and(QueryBuilder.gt("ts", start))
              .and(QueryBuilder.lte("ts", end)
              )
            for (record: Row <- session.execute(selectStmt).all()) {
              val ts = record.getTimestamp("ts")
              val rc = session.execute(boundStatement.bind(id, ts))
            }

          case _ =>
            // clear all entries for given id
            val boundStatement: BoundStatement = new BoundStatement(session.prepare(QUERY_DELETE_HIS_ALL))
            val rc = session.execute(boundStatement.bind(id))
        }

      }
    }


    Right(0)
  }

  /**
    * Performs hisWrite (history write)

    * @param metaFrame contains points for which hisWrite is being performed
    * @param project project pid to which data belongs
    * @return Either nothing or error message if an error occured processing the data
    */
  override def hisWrite(metaFrame: IMFrame, hisFrame: IMFrame, project: String): Either[Unit, String] = {
    if (metaFrame.toGrid.numRows == 1) {
      // point for which history is being written
      val pt: HDict = metaFrame.toDict
      // process individual entries
      val timeSeries = hsutil.gridToSeries(hisFrame.toGrid)

      val id: String = pt.getRef("id").toString
      val kind: String = pt.getStr("kind")
      val boundStatement: BoundStatement = new BoundStatement(session.prepare(QUERY_INSERT))

      for (row <- timeSeries.rows) {
        /*
        val strval =
          kind match {
            case "Bool" => row._2.toString
            case "Number" => row._2.toString
            case _ => row._2.toString
        }
        */
        session.execute(boundStatement.bind(id,
          new Date(row._1), row._2.toString, project));

      }

      // create a batch to write
      // Iterator.continually(block).takeWhile(_ => test).toList
      Left()
    } else {
      Right("Only one-point hisWrite is supported right now")
    }
  }

}

// Project/User store

trait BaseRepositoryComponent[E <: BaseEntity] {

  val table: String
  val session: StoreSession


  def readById(id: String) : Option[E] = {

    val selectStmt = QueryBuilder.select()
      .from(table)
      .where(QueryBuilder.eq("id", UUID.fromString(id)))
    val resultSet = session.dbSession.execute(selectStmt)
    val row: Row = resultSet.one()
    EntityConverter.row2Entity(row, table).asInstanceOf[Option[E]]
    // convert to frame
    // Row[856716f7-2e54-4715-9f00-91dcbea6c101, Thu Nov 17 00:00:00 PST 2016, sysdeamon, Thu Nov 17 00:00:00 PST 2016, false, This is a test project #1, 1161 W Georgia St, Vancouver, BC V6E 0C6, Canada, Vancouver, V6E 0C6, {guest-group=r, iotus-admin-group=rwx}, {"dis": "test 01", "custId": "targetCorp"}, Test project 01, 900, test-project, [regionNW, test], America/Los_Angeles]
  }

  /**
    * Query by property name/value.
    * Property has to be represented by an index to avoid a run time error.
    * @param name
    * @param value
    * @return
    */
  def readByProp(name: String, value: Any): Option[E] = {
    val selectQuery = QueryBuilder.select()
      .from(table).allowFiltering()
    val selectStmt = selectQuery.where(QueryBuilder.eq(name, value))
    //val sel:Select = QueryBuilder
    // QueryBuilderEx.contains
    // .and.QueryBuilder.contains("tags", filter))
    val resultSet = session.dbSession.execute(selectStmt)
    val row: Row = resultSet.one()
    if (row == null) None else EntityConverter.row2Entity(row, table).asInstanceOf[Option[E]]
  }

  /**
    * read all records satisfying the given filter
    * @param filter axon expression to filter by
    * @return list of entities found in the database
    */
  def readAll(filter: Option[String]=None) : List[E] = {
    val selectStmt = QueryBuilder.select()
      .from(table)
    val resultSet = session.dbSession.execute(selectStmt)
    val rows: util.List[Row] = resultSet.all()
    // filter via deleted flag
    EntityConverter.rows2Entities(rows.toList, table).asInstanceOf[List[E]]
  }

  /**
    * Save given entity
    * @param entity
    * @return
    */
  def save(entity: E) : Unit = {
    // translate entity to statement, values
    val (statement, values) = EntityConverter.entityToStatement(entity, table)

    // prepare statement, bind and execute
    val prepared = session.dbSession.prepare(statement)
    val bound: BoundStatement = prepared.bind()
    val ovalues = values.asInstanceOf[Seq[Object]]
    // unpack sequence into varargs needed for bind
    bound.bind(ovalues:_*)
    session.dbSession.execute(bound)
  }

  /**
    * Delete given entity by marking the record as deleted in the database.
    * @param id id of record to mark as deleted
    * @return
    */
  def delete(id: String) : Unit = {
    session.dbSession.execute(s"UPDATE $table set deleted=true where id=?", UUID.fromString(id))
  }

  //def updateById(id: String, row: E) : Option[Int]

  /**
    * Remove all records marked as deleted from the table.
    * @return number of records purged
    */
  def purge(): Int = {
    val selectStmt = QueryBuilder.select()
      .from(table)
    val resultSet = session.dbSession.execute(selectStmt)
    val rows: util.List[Row] = resultSet.all()

    // filter rows marked as deleted
    val deletedRows = rows.toList.filter((x: Row) => x.getBool("deleted"))
    deletedRows.map(x => session.dbSession.execute(s"DELETE from $table where id=?", x.getUUID("id")))
    deletedRows.size
  }
}

class ProjectRepository(val table: String, val session: StoreSession) extends BaseRepositoryComponent[Project] {
}

class UserRepository(val table: String, val session: StoreSession) extends BaseRepositoryComponent[User] {
}


