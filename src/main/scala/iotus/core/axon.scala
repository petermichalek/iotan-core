package org.projecthaystack

import java.time.{Instant, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.UUID

import scala.collection.mutable
import collection.JavaConversions._
import scala.collection.mutable.HashSet
import net.liftweb.json._
import net.liftweb.json.JsonDSL._
import net.liftweb.json.Extraction._
import org.projecthaystack.HDict.MapEntry
import org.projecthaystack.io.HZincWriter

/**
  * HGrid wrapper that provides access to package data members
  * @param grid
  */
class GridWrap(grid: HGrid)  {

  //  val g = new HGrid()
  var types: mutable.Map[String, HashSet[String]] = mutable.HashMap()
  var _srows: mutable.MutableList[Map[String, HVal]] = mutable.MutableList()
  types.put("tags", HashSet())
  types.put("props", HashSet())
  types.put("propsn", HashSet())
  types.put("propsb", HashSet())

  // initialized scala-style rows
  // types["tags", "props", "propsn" etc.]
  for (row <- grid.rows) {
    //println("row: " + row)
    var cells: mutable.HashMap[String, HVal] = mutable.HashMap()
    for (col <- row.iterator) {
      val entry = col.asInstanceOf[MapEntry]
      //println("col key, hval ", entry.getKey, entry.getValue)
      /*
      for (col <- row.asInstanceOf[HRow].iterator) {
        println("col: " + col)
      }
      */
      val typeStr = entry.getValue match {
        case _: HMarker => "tags"
        case _: HNum => "propsn"
        case _: HBool => "propsb"
        case _ => "props"
      }
      cells.put(entry.getKey.asInstanceOf[String], entry.getValue.asInstanceOf[HVal])
      val setForType: scala.collection.mutable.HashSet[String] = types.get(typeStr).get
      setForType.add(entry.getKey.asInstanceOf[String])
    }
    _srows += cells.toMap
  }
  val typeMap = types.toMap
  val srows: List[Map[String, HVal]] = _srows.toList
  //var _srows2: mutable.MutableList[Map[String, HVal]] = mutable.MutableList()



  val colNames = grid.cols.toList.map(_.name)

  //private var typeMap: Map[String, Set[String]] = null

  def toGrid = grid
  def cols = grid.cols
  def rows = grid.rows
  def meta: HDict = grid.meta
  def numRows = grid.numRows
  def numCols = grid.numCols

  /**
    * Generate id column
    */
  //def generateIds(): Either[String, GridWrap] = {
  //}

    /**
    * Rename columns in grid.
    * @param colnamesMap map of old a new columns names
    * @param overwrite if true, allow overwrting existing columns
    * @return Either String on error, or newly created grid
    */
  def renameCols(colnamesMap: Map[String, String], generateIds: Boolean, overwrite: Boolean = false): Either[String, GridWrap] = {


    /**
      * Inner function to assist in pre-pending id if requested.
      * uses parent function colnamesMap
      * @param row
      * @return
      */
    def transformRow(row: Map[String, HVal]): Map[String, HVal] = {
      val newrow = row map { case(k, v) =>
          if (colnamesMap.contains(k)) {
            (colnamesMap.get(k).get, v)
          } else
            (k, v)
        }
      Map("id" -> HRef.make(UUID.randomUUID.toString)) ++ newrow
    }


    // conNames.map(x => postVals.contains(x))
    val willOverwrite = this.colNames.map(
        x => colnamesMap.contains(this.colNames))
        .reduceLeft((acc, n)  => acc || n)

    if (willOverwrite && !overwrite) {
      return Left("There is some overlap in new and old column names: use overwrite=true to overwrite")
    }

    val newColnames = this.colNames.map(x => colnamesMap.getOrElse(x, x))
    if (generateIds) {
      if (newColnames.contains("id") && !overwrite) {
        return Left("generateIds is specified but overwrite if false")
      }
    }
    //var newrows: mutable.ListBuffer[Map[String, HVal]] = mutable.ListBuffer(this.srows.toArray:_*)
    var newrows: mutable.ListBuffer[Map[String, HVal]] = mutable.ListBuffer()
    // resolve hrefs
    for (row: Map[String, HVal] <- this.srows) {
      newrows.append(transformRow(row))
    }
    // append id
    val newColnames2 = if (generateIds) List("id") ++ newColnames else newColnames

    val newwgrid: GridWrap = GridWrap.fromRows(this.meta, newColnames2, newrows.toList)
    Right(newwgrid)


  }

  def addIdToRow(orig:  Map[String, HVal]): Map[String, HVal] = {
    orig ++ Map("id" -> HRef.make("dummy"))
  }

  def filterCols(wg: ((String) => Boolean)): GridWrap = {
    val b = new HGridBuilder
    b.meta() add this.meta
    var newcols: collection.mutable.ListBuffer[String] = collection.mutable.ListBuffer()
    for (col <- this.colNames) {
      if (wg(col)) {
        b.addCol(col)
        newcols.append(col)
      } else {
        //println(s"col $col filtered.")
      }
    }
    for (row: Map[String, HVal] <- this.srows) {
      var cells: collection.mutable.ListBuffer[HVal] = collection.mutable.ListBuffer()
      for (col:String <- newcols) {
        //val newrow = row -- colnames
        //println(s"col $col")
        if (wg(col)) {
          val newhval:Any = row.get(col).getOrElse(null)
          if (newhval != null) cells.append(newhval.asInstanceOf[HVal]) else cells.append(null)
          //println(s"error for col $col: $newhval")
        } else {
          cells.append(null)
        }
      }
      //newcols.(_ => cells.append(row.get(_).get))
      b.addRow(cells.toArray[HVal])
    }
    new GridWrap(b.toGrid)
  }

  def filterCols(colnames: Set[String]): GridWrap = {
    filterCols( (col: String) => colnames.contains(col))
    /*
    val b = new HGridBuilder
    b.meta() add this.meta
    var newcols: collection.mutable.ListBuffer[String] = collection.mutable.ListBuffer()
    for (col <- this.colNames) {
      if (!colnames.contains(col)) {
        b.addCol(col)
        newcols.append(col)
      } else {
        //println(s"col $col filtered.")
      }
    }
    for (row: Map[String, HVal] <- this.srows) {
      var cells: collection.mutable.ListBuffer[HVal] = collection.mutable.ListBuffer()
      for (col:String <- newcols) {
        //val newrow = row -- colnames
        println(s"col $col.")
        if (row.contains(col)) {
          cells.append(row.get(col).get)
        } else {
          cells.append(null)
        }
      }
      //newcols.(_ => cells.append(row.get(_).get))
      b.addRow(cells.toArray[HVal])
    }
    new GridWrap(b.toGrid)
    */
  }

  /**
    *
    * @param limit only render the last n rows
    * @return
    */
  def asZinc(limit: Int=0): String = {
    val zincStr = HZincWriter.gridToString(toGrid)
    if (limit > 0) {
      zincStr.split("\n").take(limit+2).mkString("\n")
    } else {
      zincStr
    }
  }

}

object GridWrap {

  def apply(grid: HGrid ): GridWrap = {
    new GridWrap(grid)
  }

  def fromRows(meta: HDict, cols: List[String], rows: List[Map[String, HVal]]): GridWrap = {

    val b = new HGridBuilder
    b.meta() add meta
    //var newcols: collection.mutable.ListBuffer[String] = collection.mutable.ListBuffer()
    for (col <- cols) {
      b.addCol(col)
    }
    for (row: Map[String, HVal] <- rows) {
      var cells: collection.mutable.ListBuffer[HVal] = collection.mutable.ListBuffer()
      for (col:String <- cols) {
        val newhval:HVal = row.get(col).getOrElse(null)
        cells.append(newhval)
      }
      b.addRow(cells.toArray[HVal])
    }
    new GridWrap(b.toGrid)
  }

}
  /**
  * AxonEval base trait.
  * This can be extended by AxonEval classes/objects for different store, such as Cassandra or MongoDB
  */
trait AxonEval {

  def eval(f: HFilter): JValue

  def filterType(hfilter: HFilter): String = {
    // assume type like: org.projecthaystack.HFilter.And
    val longName = hfilter.getClass.getCanonicalName.split("\\.")
    val shortName = longName(longName.length - 1)
    shortName
  }

}

case class MatchInt(`type`: String, field: String, value: Int)
case class MatchStr(`type`: String, field: String, value: String)
case class LteStr(`type`: String, field: String, upper: String, include_upper:Boolean = true)
case class LtStr(`type`: String, field: String, upper: String)
case class GtStr(`type`: String, field: String, lower: String)
case class GteStr(`type`: String, field: String, lower: String, include_lower:Boolean = true)

case class LteNum(`type`: String, field: String, upper: Double, include_upper:Boolean = true)
case class LtNum(`type`: String, field: String, upper: Double)
case class GtNum(`type`: String, field: String, lower: Double)
case class GteNum(`type`: String, field: String, lower: Double, include_lower:Boolean = true)

/**
  * Cassandra eval object.
  * Used to provide translation from axon to Cassandra, like this:
  *
  * // init filter by parsing it from an axon expression
    val hfilter = HFilter.make("point and his")
    // translate into a cassandra/lucene search query
    val jsonquery:String = CassandraEvalFunctor.axonToString(hfilter, true)
    // result looks something like this:
    {
      "query":{
        "must":[{
          "type":"match",
          "field":"tags",
          "value":"point"
        },{
          "type":"match",
          "field":"tags",
          "value":"his"
        }]
      }
    }

  *
  */
object CassandraEval extends AxonEval {
  val compoundKeywordsTable = Map("and" -> "must", "or" -> "should")
  val compareKeywordsTable = Map(
    "==" -> "match",
    "<=" -> "lower",
    "<" -> "lower",
    ">=" -> "higher",
    ">" -> "higher"
  )
  val dbFieldSet = Set(
    "mod",
    "created",
    "author"
  )

  val compoundPattern = "(And|Or)".r
  val orPattern = "(Or)".r
  val andPattern = "(And)".r
  val cmpPattern = "(Eq|Neq)".r
  val hasPattern = "(Has)".r
  implicit val formats = net.liftweb.json.DefaultFormats

  val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  def axonToString(f: HFilter, prettify: Boolean = false): String = {
    val jsonvalue:JValue = eval(f)
    val query = Map(("query" -> jsonvalue))
    if (prettify) pretty(render(query)) else compact(render(query))
  }

  /**
    * Allow inclusion of non-props fields mod, created, author
    * @param path
    * @return
    */
  def strPath(path: String): String = {
    if (dbFieldSet.contains(path)) path else "props$" + path
  }

  /**
    * Convert hval of string or hdate or hdatetime type to search string
    * @param hval
    * @return
    */
  def hvalStr(hval: HVal): String = {
    hval match {
      case hval: HDateTime =>
        val hdt = hval.asInstanceOf[HDateTime]
        ZonedDateTime.ofInstant(Instant.ofEpochMilli(hdt.millis), ZoneId.of("UTC")).format(datetimeFormatter) + "UTC"
      case hval: HDate =>
        val hdt = hval.asInstanceOf[HDate]
        ZonedDateTime.of(hdt.year, hdt.month, hdt.day, 0, 0, 0, 0, ZoneId.of("UTC")).format(datetimeFormatter) + "UTC"
      case _ => hval.toString
    }
  }
  /**
    * Perform eval a filter to convert to a JValue json.
    * This function is recursive.
    * TODO: convert into a tail recursive scala function.
    * @param f
    * @return
    */
  override def eval(f: HFilter): JValue = {


    // http://stackoverflow.com/questions/5177180/can-we-use-match-to-check-the-type-of-a-class

    f match {
        // process for compound filter (or, and)
      case _: HFilter.CompoundFilter =>
        val thisfilter = f.asInstanceOf[HFilter.CompoundFilter]
        val leftctx = eval(thisfilter.a)
        val rightctx = eval(thisfilter.b)
        val op = compoundKeywordsTable.get(thisfilter.keyword)
        val mlist = JArray(List(leftctx,rightctx))
        val ret:JObject = (op.get -> mlist)
        ret

      // process for compare filter (eq, neq, lt etc.) - property comparison
      case _: HFilter.CmpFilter =>
        val thisfilter = f.asInstanceOf[HFilter.CmpFilter]
        val hval:HVal = thisfilter.`val`
        // figure out if number
        val valIsNum = hval match {
          case x: HNum =>
            true
          case _ => false
        }
        val path = thisfilter.path.toString
        val op = compareKeywordsTable.get(thisfilter.cmpStr)

        val expr = thisfilter.cmpStr match {
          case "==" =>
            MatchStr("match", strPath(path), hvalStr(hval))
          case "<=" =>
            if (!valIsNum) {
              LteStr("range", strPath(path), hvalStr(hval))
            } else {
              LteNum("range", "propsn$" + path, hval.asInstanceOf[HNum].`val`)
            }
          case "<" =>
            if (!valIsNum) {
              LtStr("range", strPath(path), hvalStr(hval))
            } else {
              LtNum("range", "propsn$" + path, hval.asInstanceOf[HNum].`val`)
            }
          case ">" =>
            if (!valIsNum) {
              GtStr("range", strPath(path), hvalStr(hval))
            } else {
              GtNum("range", "propsn$" + path, hval.asInstanceOf[HNum].`val`)
            }
          case ">=" =>
            if (!valIsNum) {
              GteStr("range", strPath(path), hvalStr(hval))
            } else {
              GteNum("range", "propsn$" + path, hval.asInstanceOf[HNum].`val`)
            }
          case "!=" =>
            Map("not" -> MatchStr("match", strPath(path), hvalStr(hval)))
          case _ => throw new RuntimeException("CmpFilter not supported: %s - %s".format(f.getClass, thisfilter.cmpStr))
        }
        decompose(expr)
      // process for "missing" filter (not)
      case _: HFilter.Missing =>
        val thisfilter = f.asInstanceOf[HFilter.Missing]
        decompose(Map("not" -> MatchStr("match", "tags", thisfilter.path.toString)))
      // process for "has" filter (exact tag match)
      case _: HFilter.Has =>
        val thisfilter = f.asInstanceOf[HFilter.Has]
        decompose(MatchStr("match", "tags", thisfilter.path.toString))
      case _ =>
        throw new RuntimeException("HFilter not supported: %s - %s".format(f.getClass, f))
    }

  }
}

