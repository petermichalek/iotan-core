package iotus.core

import collection.JavaConversions._
import java.time._
import java.util.{Date, TimeZone}

import org.projecthaystack._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Haystack utility functions
  *
  */
object hsutil {


  val VAL_MARKER = "\u2713"

  // ----------- standard errors
  val STANDARD_ERROR_UNKNOWN_REC = "haystack::UnknownRecErr"
  // Input provided has incorrect format
  val STANDARD_ERROR_INCORRECT_INPUT = "haystack::BadInput"
  // Error processing data while updating store
  val STANDARD_ERROR_UPDATE = "haystack::UpdateErr"

  val STANDARD_ERROR_PERMISSION_DENIED = "haystack::PermissionDenied"

  //val STANDARD_ERROR_BAD_GRID = "haystack::BadGridErr"

  /**
    *
    * Example error grid
    ver:"2.0" errTrace:"haystack::UnknownRecErr:... ..."
    dis:"haystack::UnknownRecErr: Headquarters.AHU-1aa.ZoneTempSp"
    errType:"haystack::UnknownRecErr" err
    empty
    *
    * @param disAddString string to add to dis meta, in addition to errType
    * @param errType
    * @return
    */
  def errorGrid(errType: String, disAddString: Option[String], errTrace: Array[StackTraceElement]=Array() ): HGrid = {
    var gridBuilder:HGridBuilder = new HGridBuilder
    var metaBuilder:HDictBuilder = gridBuilder.meta()

    val dis =
      disAddString match {
      case Some(s) => s"$errType: $s"
      case _ => s"$errType"
    }

    metaBuilder.add("dis", dis)
    metaBuilder.add("errType", errType)

    val partialTrace = if (errTrace.size > 0) errTrace.slice(1,4) else Thread.currentThread().getStackTrace().slice(2, 5)
    // only return elements 1 to 4
    val stackString = partialTrace.mkString("\n")

    // or more readable, adjust this code
    /*
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    val stackString = sw.toString
    */
    metaBuilder.add("errTrace", s"$dis $stackString")
    /*
    if (errTrace.size > 0) {
      metaBuilder.add("errTrace", errTrace.mkString("\n"))
    } else {
      metaBuilder.add("errTrace", "")

    }
    */
    metaBuilder.add("err")
    gridBuilder.toGrid
  }
  /**
    * Convert primitive type returned by the database query to HVal.
    * if key/name is "id" or ".*Ref", return HRef as HVal
    * @param key
    * @param value
    * @return
    */
  def primitiveToHVal(key:String, value:Any, valType: Option[String]=None): HVal = {
    // TODO: change return type to Either[Hval, String]
    var hval:HVal = null
    try {
      value match {
        case zdt: Some[ZonedDateTime] =>
          hval = HDateTime.make(Date.from(zdt.get.toInstant).getTime)
        case s: String =>

          if (key == "id") hval = HRef.make(s)
          else if (key.endsWith("Ref")) {
            hval = HRef.make(s)
          }
          else if (valType != None && valType.get == "Number") hval = HNum.make(s.toDouble)
          else if (value == VAL_MARKER) hval = HMarker.VAL
          else hval = HStr.make(s)
        /*
        TODO: recognize a string is datetime, date, etc.
        see http://project-haystack.org/doc/Zinc
        http://project-haystack.org/tag/kind

            HCoord
            Ref
            Time
            Bin
            XStr
            List

            Coord: C(37.55,-77.45)
            Time: 08:12:05 (hh:mm:ss.FFF)
            List: [1, 2, 3]
            Uri: `http://project-haystack.com/`
            Ref: @17eb0f3a-ad607713
            NA: NA
            Null: N

            Possibly use json encoding within db string field to be able to parse type:
            http://project-haystack.org/doc/Json
                        Ref           "r:<id> [dis]"  "r:abc-123" "r:abc-123 RTU #3"
Date          "d:2014-01-03"
Time          "h:23:59"
Bin           "b:<mime>" "b:text/plain"
Coord         "c:<lat>,<lng>" "c:37.545,-77.449"
              "c:95,-88"

XStr          "x:Type:value"


            elif isinstance(ts, datetime.datetime):
                hts = HDateTime.make_from_dt(ts, htz)
            elif isinstance(ts, datetime.date):
                hts = HDate.make(ts.year, ts.month, ts.day)


         */
        case i: Int =>
          hval = HNum.make(i)
        case d: Double =>
          hval = HNum.make(d)
        case f: java.lang.Float =>
          hval = HNum.make(f.toDouble)
        case date: Date =>
          hval = HDateTime.make(date.getTime)
        case b: Boolean =>
          hval = HBool.make(b)
        // elif isinstance(val,bool) or isinstance(val,np.bool_):
        //   hval = HBool.make(val)
        case None => null
        case _ =>
          val otype = value.getClass
          throw new RuntimeException(s"Don't know how to instantiate HVal for type $otype.")
      }
    } catch {
      case _ => hval = HStr.make("Internal error converting.")
    }
    hval
  }

  /**
    * Convert haystack range to ZonedDateTime tuple.
    * @param hsrange
    * @return
    */
  def hsRangeToZtRange(hsrange: HDateTimeRange): Tuple2[ZonedDateTime, ZonedDateTime] = {
    val start: HDateTime = hsrange.start
    val end: HDateTime = hsrange.end
    val stz = "America/Los_Angeles"
    val etz = "America/Los_Angeles"
    // start.tz
    val lstart = LocalDateTime.ofInstant(Instant.ofEpochMilli(start.millis()), ZoneId.of(stz))
    //LocalDateTime.ofInstant(Instant.ofEpochMilli(longValue), ZoneId.systemDefault())
    val lend = LocalDateTime.ofInstant(Instant.ofEpochMilli(end.millis()), ZoneId.of(etz))
    val ztStart = ZonedDateTime.ofLocal(lstart, ZoneId.of(stz), ZoneOffset.ofTotalSeconds(start.tzOffset))
    val ztEnd = ZonedDateTime.ofLocal(lend, ZoneId.of(etz), ZoneOffset.ofTotalSeconds(end.tzOffset))
    Tuple2(ztStart, ztEnd)
  }
  /**
    * Convert time series list to HGrid.
    * Returned HGrid may be empty if time series was empty.
    *
    * @param pointGrid HGrid of points
    * @param tslist list of lists of (ts, val) tuples.
    * @return HGrid
    */
  def tslistToGrid(pointGrid: HGrid, tslist: List[Map[Date, Any]], start: Date, end: Date, tz: TimeZone): HGrid = {
    val hstz = HTimeZone.make(tz)
    var pointType: ArrayBuffer[String] = ArrayBuffer()
    var gridBuilder:HGridBuilder = new HGridBuilder
    var metaBuilder:HDictBuilder = gridBuilder.meta()
    gridBuilder.addCol("ts")
    for (i <- 0 until pointGrid.numRows) {
      // see rationale below
      val colName = if (pointGrid.numRows > 1) s"v$i" else "val"
      val metacolName = if (pointGrid.numRows > 1) s"v$i" else "id"
      val idVal = pointGrid.row(i).get("id")
      gridBuilder.addCol(colName)
      metaBuilder.add(metacolName, idVal)
      pointType += ( try {
        pointGrid.row(i).get("kind").toString
      } catch {
        case _: Exception  => "Str"
      })
    }

    /* rationale for colName above:

    For single point history, comply with haystack spec - use "id" column name and meta attribute:

    hisRead(2014-01-01T00:00-05:00,2016-12-01T00:00-05:00)
    ver:"2.0" id:@756716f7-2e54-4715-9f01-91dcbea6c111 "demo pt 01"
    ts,id

    for multi point history:
    this is not part of haystack spec - use v0, v1 etc. column names and meta attributes:

    hisRead(2014-01-01T00:00-05:00,2016-12-01T00:00-05:00)
    ver:"2.0" v0:@756716f7-2e54-4715-9f01-91dcbea6c111 "demo pt 01" v1: @756716f7-2e54-4715-9f01-91dcbea6c222 "demo pt 02"
    ts,v0,v1
    ...

     */

    // create master time stamp index merging all indexes
    var tsIndex: scala.collection.mutable.Set[Date] = mutable.SortedSet()

    // for all items in tslist
    for (tseries <- tslist) {
      // for all rows in time serries, add timestamp to index
      tsIndex ++= tseries.keySet
      //for (row <- tseries.keySet) {
      //  tsIndex.add(row)
      //}
    }
    val tsIndexList = tsIndex.toList
    //if (tsIndexList.size > 0) metaBuilder.add("start", HDateTime.make(tsIndexList(0).getTime(), hstz))
    //    .add("end", HDateTime.make(tsIndexList.last.getTime(), hstz))

    metaBuilder.add("start", HDateTime.make(start.getTime(), hstz))
      .add("end", HDateTime.make(end.getTime(), hstz))

    // merge time series
    // i.e. create grid rows based on availability of columns for each timestamp i time series
    for (ts <- tsIndex) {
      var cells: collection.mutable.ListBuffer[HVal] = collection.mutable.ListBuffer()
      cells.append(HDateTime.make(ts.getTime, hstz))

      for (i <- 0 until tslist.size) {
        val tseries = tslist(i)
        val value = tseries.getOrElse(ts, None)
        cells.append(primitiveToHVal(s"v$i", value, Option(pointType(i))))
      }
      gridBuilder.addRow(cells.toArray[HVal])
    }

    gridBuilder.toGrid

  }

  /**
    * Convert HGrid to time series
    * TODO: add colNumber: Int argument to extract other columns
    * @param grid grid to extract time series from, the first column has ts, 2nd columns has values
    * @return Time series
    */
  def gridToSeries(grid: HGrid): model.TimeSeriesLight = {
    val hisGrid = grid

    var timeSeriesBuffer: collection.mutable.ListBuffer[(Long, Any)] = collection.mutable.ListBuffer()
    // TODO: use tail recursion with immutable collection insteaad of mutable collection?
    val col1 = hisGrid.col(0)
    val col2 = hisGrid.col(1)
    for (i <- 0 until hisGrid.numRows) {
      val row = hisGrid.row(i)
      val dt = row.get(col1, false)
      val hval = row.get(col2, false)
      //timeSeriesBuffer.append((new Date(dt.asInstanceOf[HDateTime].millis()), hval.toString))
      timeSeriesBuffer.append((dt.asInstanceOf[HDateTime].millis(), hval.toString))
    }
    model.TimeSeriesLight(timeSeriesBuffer.toList)
  }

  def maplistToGrid(points: Map[String, Any]): HGrid = {
    val ret = HGrid.EMPTY
    ret
  }

}
