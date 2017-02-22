package iotus.core

import scala.collection.JavaConversions._
import java.time.ZonedDateTime
import java.util.{List, TimeZone, UUID}

import org.projecthaystack._
import com.datastax.driver.core.Row
import hsutil.primitiveToHVal

import scala.collection.{mutable, _}
import scala.collection.mutable.{Map => _, Seq => _, Set => _, _}

//*******************************************************************
//********************** Model/Frames etc. **************************
//*******************************************************************

/**
  * Encapsulates dynamic grid entity for iotus data.
  */
class IMFrame(isHis: Boolean=false) {

  //private var fframe: Frame[Int, Int] = null;
  private var rows: List[Map[String, Any]] = null
  private var _keys: Set[String] = Set();
  /**
    * TODO: move this to a db class
    * Map of column names to types that represent it in the Cassandra database table
    */
  private var _typeMap: Map[String, HashSet[String]] = Map()
  private var grid: Option[HGrid] = None;

  /**
    * Shows a few initial lines of frame
    * @return
    */
  override def toString: String = {
    val buffer: ListBuffer[String] = ListBuffer()
    // show only a few lines
    for (row <- rows(0)) {
      buffer += row.toString + "\n"
      buffer += "..."

    }
    buffer.toString
  }

  /**
    * Return an HDict for first row.
    * @return
    */
  def toDict: HDict = {
    val dictBuilder: HDictBuilder = new HDictBuilder()
    // if no rows, return empty dict
    if (rows.size > 0) {
      val row = rows(0)
      // append id
      val idStr:String = row("id").asInstanceOf[String]
      val disStr = row.getOrElse("dis", null)
      val idVal = if (disStr != null) HRef.make(idStr, disStr.asInstanceOf[String]) else HRef.make(idStr)
      //println("toGrid: %s - idVal=%s, zinc=%s, json=%s".format(idVal.getClass, idVal.toString, idVal.toZinc, idVal.toJson))
      dictBuilder.add("id", idVal)
      // TODO: rewrite as Map with single line
      for ((key, value) <- row) {
        if (key != "id") { // id was added first
          val hval = primitiveToHVal(key, value)
          dictBuilder.add(key, hval)
        }
      }
    }
    dictBuilder.toDict
  }


  def toGrid: HGrid = {

    if (!isHis) {
      if (grid.isEmpty) {
        if (_keys.isEmpty) {
          grid = Some(HGrid.EMPTY)
        } else {
          var gridBuilder: HGridBuilder = new HGridBuilder()
          //var bmeta = new HDictBuilder()
          // add cols
          gridBuilder.addCol("id")
          for (key <- _keys) {
            //gridBuilder.meta().add(s"v$i", key)
            gridBuilder.addCol(key)
          }
          // add rows
          for (row <- rows) {
            /*
            //val dictBuilder:HDictBuilder = new HDictBuilder()
            //row.foreach { pair =>
            //  dictBuilder.add(pair._1, s"$pair._2")
            //}
            //println("toGrid: add cell")
            var cells: collection.mutable.ListBuffer[HVal] = collection.mutable.ListBuffer()
            // append id
            //val idval2 = HRef.make("test", "dis")
            val idStr:String = row.get("id").get.asInstanceOf[String]
            val disStr = row.getOrElse("dis", null)
            val idVal = if (disStr != null) HRef.make(idStr, disStr.asInstanceOf[String]) else HRef.make(idStr)
            //println("toGrid: %s - idVal=%s, zinc=%s, json=%s".format(idVal.getClass, idVal.toString, idVal.toZinc, idVal.toJson))
            cells.append(idVal)
            for (key <- keys) {
              val value = row.getOrElse(key, null)
              if (value != null) {
                val hval = primitiveToHVal(key, value)
                //println("toGrid: %s - hval=%s, zinc=%s, json=%s".format(hval.getClass, hval.toString, hval.toZinc, hval.toJson))
                cells.append(hval)
              } else {
                cells.append(null)
              }
            }

            gridBuilder.addRow(cells.toArray[HVal])
            */
            gridBuilder.addRow(buildCells(row))

          }
          /*
          -----
            var i = 0
            while (i < cells.toList.length)  {
              val bc = cells.toList.get(i)
              bc.

          -----
      // meta
      HDict meta = this.meta.toDict();

      // cols
      HCol[] hcols = new HCol[this.cols.size()];
      for (int i=0; i<hcols.length; ++i)
      {
        BCol bc = (BCol)this.cols.get(i);
        hcols[i] = new HCol(i, bc.name, bc.meta.toDict());
      }

      // let HGrid constructor do the rest...
      return new HGrid(meta, hcols, rows);

  // meta
  val meta: HDict = this.meta.toDict

      // cols
      val hcols: Array[HCol] = new Array[HCol](this.cols.size)
      var i: Int = 0
  while (i < hcols.length)  {  { val bc: HGridBuilder.BCol = this.cols.get(i).asInstanceOf[HGridBuilder.BCol]
  hcols(i) = new HCol(i, bc.name, bc.meta.toDict)
  }
  {i += 1; i}}

      // let HGrid constructor do the rest...
      return new HGrid(meta, hcols, rows)

           */
          //println("toGrid: invoking gridBuilder.toGrid")
          grid = Some(gridBuilder.toGrid)
          //println("toGrid: done invoking gridBuilder.toGrid")
        }
      }
    }
    //HGridWrap(grid.get)
    grid.get
    // TODO: convert to wrapper that doesn't cause scala REPL to crash due to HGrid.toString not being implemented

  }

  private def buildCells(row:Map[String, Any]): Array[HVal] = {
    val cells: ListBuffer[HVal] = ListBuffer()
    // append id
    //val idval2 = HRef.make("test", "dis")
    val idStr:String = row("id").asInstanceOf[String]
    val disStr = row.getOrElse("dis", null)
    val idVal = if (disStr != null) HRef.make(idStr, disStr.asInstanceOf[String]) else HRef.make(idStr)
    //println("toGrid: %s - idVal=%s, zinc=%s, json=%s".format(idVal.getClass, idVal.toString, idVal.toZinc, idVal.toJson))
    //println("toGrid: idVal=%s".format(idVal.toString))
    cells.append(idVal)
    for (key <- _keys) {
      val value = row.getOrElse(key, null)
      //println("toGrid: key=%s, value=%s".format(key, value))
      if (value != null) {
        val hval = primitiveToHVal(key, value)
        //println("toGrid: %s - hval=%s, zinc=%s, json=%s".format(hval.getClass, hval.toString, hval.toZinc, hval.toJson))
        cells.append(hval)
      } else {
        cells.append(null)
      }
    }
    cells.toArray
  }

  // accessors
  def keys: List[String] = _keys.toList



  def typeMap: Map[String, HashSet[String]] = _typeMap
}

object IMFrame {

  private val regularColumsNames = Array("id", "created", "mod", "author")

  def apply(grid: HGrid, isHis: Boolean): IMFrame = {

    //var keys: collection.mutable.Set[String] = collection.mutable.HashSet()


    for (cell <- grid.iterator()) {
      //println("cell: " + cell)
      /*
      for (col <- cell.asInstanceOf[HRow].iterator) {
        println("col: " + col)
        //col.asInstanceOf[MapEntry]
      }
      val name:String = cell.asInstanceOf[java.util.Map.Entry[String, Any]].getKey
      keys.add(name)
      */
      //grid1.col(name) should not be null
    }
    val imframe = new IMFrame(isHis)
    val gridWrap: GridWrap = new GridWrap(grid)
    imframe._keys = gridWrap.colNames.toSet
    imframe._typeMap = gridWrap.typeMap
    imframe.grid = Option(grid)
    imframe
  }

  def apply(rows: List[Row]): IMFrame = {
    // TODO: get rid of this funtion, use Grid ctor only.
    var imframe = new IMFrame(false)
    //var modelRows: collection.mutable.ListBuffer[Map[String, String]] = collection.mutable.ListBuffer()
    var modelRows: ListBuffer[Map[String, Any]] = ListBuffer()
    var keys: mutable.Set[String] = HashSet()
    //var keyTypes: collection.mutable.LinkedHashMap[String, String] = collection.mutable.LinkedHashMap()

    for (row:Row <- rows) {
      //var obj:collection.mutable.Map[String, Any] = collection.mutable.Map()
      val id = row.getUUID("id")
      // process tags (set) field
      val tags:Set[String]  = row.getSet("tags", classOf[String]).toSet

      // convert to map of name -> checkMark
      val tagMap = tags.map { (_, "\u2713")}

      // process maps: string props and number/float propsn
      //avoid conflict with colums from regular fields (id, mod etc.)
      val props:Map[String, String]  =
          row.getMap("props", classOf[String], classOf[String]).toMap -- regularColumsNames
      val propsn:Map[String, java.lang.Float]  =
          row.getMap("propsn", classOf[String], classOf[java.lang.Float]).toMap -- regularColumsNames
      val propsb:Map[String, java.lang.Boolean]  =
          row.getMap("propsb", classOf[String], classOf[java.lang.Boolean]).toMap -- regularColumsNames
      keys ++= tags
      keys ++= props.keySet
      keys ++= propsn.keySet
      keys ++= propsb.keySet
      modelRows.append(props  ++ propsn ++ propsb ++ tagMap + ("id" -> id.toString))
    }

    imframe._keys = keys.toSet[String]

    /*
    */
    imframe.rows = modelRows.toList
    //modelRows.add
    //imframe.fframe = Frame.fromRows(modelRows)
    imframe
  }

}


//*******************************************************************
//************** Miscellaneous model traits and classes *************
//**************   (Storage/Model/Frame Conversions)    *************
//*******************************************************************


/*
TODO: switch to quill, make quill work for storage

Rationale for choosing quill:

* Slick seems more modern/scala like, but seems to focus on RDBMS, so may not be easy to adjust to Cassandra
* Phantom is non-free
* Quill has examples for Cassandra
*
 */

package model {


  //*******************************************************************
  //**************************** Entities *****************************
  //*******************************************************************

  trait BaseEntity {
    val id: String
    def toMap(): Map[String, Any]
  }

  case class Project(id: String, pid: String, tzstr: String, var mod: Option[ZonedDateTime]=None) extends BaseEntity {
    val tz: TimeZone = TimeZone.getTimeZone(tzstr)
    //var mod: Option[ZonedDateTime] = None
    // savedId is only set if instance is backed by database record, otherwise None
    var savedId: Option[String] = Some(id)

    override def toMap(): Map[String, Any] =  Map(
      "id" -> this.id, "pid" -> this.pid, "tz" -> this.tzstr, "mod" -> this.mod)

  }

  object Project {
    /**
      * Create new project not backed by database and database id.
      * @param pid
      * @param tzstr
      * @return
      */
    def apply(pid: String, tzstr: String): Project = {
      val uuid = UUID.randomUUID().toString
      val p = new Project(uuid, pid = pid, tzstr = tzstr)
      p.savedId = None
      //p.mod = Some(ZonedDateTime.now)
      p
    }


  }

  case class User(id: String, email: String, firstname: String, lastname: String, passwordHash: String,
                  var mod: Option[ZonedDateTime]=None) extends BaseEntity {
    // savedId is only set if instance is backed by database record, otherwise None
    var savedId: Option[String] = Some(id)

    override def toMap(): Map[String, Any] =  Map(
      "id" -> this.id, "email" -> this.email, "mod" -> this.mod)

  }

  object User {
    /**
      * Create new user not backed by database and database id.
      * @param email
      * @return
      */
    def apply(email: String, firstname: String, lastname: String, passwordHash: String) = {
      val uuid = UUID.randomUUID().toString
      val p = new User(uuid, email, firstname, lastname, passwordHash)
      p.savedId = None
      p
    }


  }

  // light-weight time series class that encapsulates a list of tuples
  case class TimeSeriesLight(rows: List[(Long, Any)])

  //*******************************************************************
  //********************* IMFrame Conversions etc. ********************
  //*******************************************************************
  object Conversions {

    def entityToIMFrame(entity: BaseEntity) : IMFrame = {
      val props: Map[String, Any] = entity.toMap
      val gridBuilder: HGridBuilder = new HGridBuilder
      //var metaBuilder:HDictBuilder = gridBuilder.meta()

      /*
      Instead of this:

      ver:"2.0" v0:"id" v1:"pid" v2:"tz" v3:"mod"
      v0,v1,v2,v3
      @856716f7-2e54-4715-9f00-91dcbea6c103,"demo-02","America/New_York",2016-11-17T00:00:00-08:00 Los_Angeles
      @856716f7-2e54-4715-9f00-91dcbea6c101,"test-project","America/Los_Angeles",2016-11-17T00:00:00-08:00 Los_Angeles
      @856716f7-2e54-4715-9f00-91dcbea6c102,"demo","America/New_York",2016-11-17T00:00:00-08:00 Los_Angeles

      we will serve this:

      ver:"2.0"
      id,pid,tz,mod
      @856716f7-2e54-4715-9f00-91dcbea6c103,"demo-02","America/New_York",2016-11-17T00:00:00-08:00 Los_Angeles
      @856716f7-2e54-4715-9f00-91dcbea6c101,"test-project","America/Los_Angeles",2016-11-17T00:00:00-08:00 Los_Angeles
      @856716f7-2e54-4715-9f00-91dcbea6c102,"demo","America/New_York",2016-11-17T00:00:00-08:00 Los_Angeles

       */
      for ( (col, i) <- props.keys.zipWithIndex) {
        // metaBuilder.add(s"v$i", col)
        //gridBuilder.addCol(s"v$i")
        gridBuilder.addCol(col)
      }

      val cells: ListBuffer[HVal] = ListBuffer()
      for ( (key, value) <- props.iterator) {
        cells.append(primitiveToHVal(key, value))
      }
      gridBuilder.addRow(cells.toArray[HVal])

      IMFrame(gridBuilder.toGrid, isHis = false)
    }

    def entitiesToIMFrame(entities: Seq[_ <: BaseEntity]) : IMFrame = {
      val listOfMaps: Seq[Map[String, Any]] = entities.map(x => x.toMap())
      val gridBuilder:HGridBuilder = new HGridBuilder
      //var metaBuilder:HDictBuilder = gridBuilder.meta()
      if (listOfMaps.nonEmpty) {
        for ( (col, i) <- listOfMaps.head.keys.zipWithIndex) {
          //metaBuilder.add(s"v$i", col)
          //gridBuilder.addCol(s"v$i")
          gridBuilder.addCol(col)
        }
        for (i <- listOfMaps.indices) {
          val cells: ListBuffer[HVal] = ListBuffer()
          for ( (key, value) <- listOfMaps(i).iterator) {
            cells.append(primitiveToHVal(key, value))
          }
          gridBuilder.addRow(cells.toArray[HVal])
        }

      }

      IMFrame(gridBuilder.toGrid, isHis = false)
    }

  }
} // end of model package


