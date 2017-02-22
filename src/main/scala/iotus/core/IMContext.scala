package iotus.core
import scala.collection.mutable

import java.time.ZonedDateTime
import java.util.{TimeZone, UUID}

import org.projecthaystack.{HDateTimeRange, HDict, HGrid, HTimeZone}
import hsutil.hsRangeToZtRange
import model.Project


/**
  * IoTus context. Provides API entry point for management of metadaga and histories
  * via read and write operations.
  */
class IMContext(project: String, var author: String, var filter: String=null, var limit: Int=0) {

  // TODO: replace temporary list of admin users by db based
  //  per-project and system-wide admins

  val ADMIN_USERS = List("peter@michalek.org", "admin@admin.org" )

  // -------- other public read-only data members ---------
  //
  val tzstr = Env.projectByName(project) match {
    case Some(p) => p.tzstr
    case None => throw new RuntimeException(s"Project $project doesn't exist.")
    case _ => Env.defaultTimezone
  }
  val tz:TimeZone = {
    var ltz = TimeZone.getTimeZone(tzstr)
    if (ltz.getID().equals("GMT")) {
      // this indicats there was an error: set defaut

      ltz = TimeZone.getTimeZone(Env.defaultTimezone)
    }
    ltz
  }
  val tzhs: HTimeZone = HTimeZone.make(tz)

  // ------------- private writable data members ----------

  //var project = "";
  //var filter:String = null;
  private var _metaFrame:IMFrame = null
  private var _hisFrame:Option[IMFrame] = None
  private var _opType:String = null
  private var _errors:mutable.ListBuffer[String] = mutable.ListBuffer[String]()

  // ------------ private read-only data members --------------------
  //
  //val dbUrl = "cassandra://localhost:9042/iotus"
  private val dbUrl = Iotus.getConfig().getString("db")
  // arguments used in read function(s) (useful in constructing error message for hisRead etc.)
  private var readArg: Option[String] = None

  //private var cut:CassandraConnectionUri = null
  private var store:CassandraStore = new CassandraStore(CassandraConnectionUri(dbUrl))
  private var projectRepo:ProjectRepository = Iotus.projectRepo

  /**
    * Projects readable by the user associated with this context instance.
    */
  //var entities: List[Project]

  // ------- Additional ctors ----------------

  def this(project: String, author: String, filter: String) = {
    this(project, author, filter, 0)
  }

  def this(project: String, author: String) = {
    this(project, author, null, 0)
  }
  // ---------- Getters --------------------

  def pid = project
  def opType = _opType
  def metaFrame = _metaFrame
  def hisFrame = _hisFrame
  // TODO: change name to user
  def getAuthor = author
  def setAuthor(a: String): Unit = author = a

  // ------- Public methods ----------------

  def toGrid():HGrid = {
    _hisFrame match {
      case Some(frame) => frame.toGrid
      case _ =>
        /*
        TODO GREG
        _metaFrame match {
          case Some(frame) => _metaFrame.toGrid
          case _ => HGrid.EMPTY
        }
        */
        if (_metaFrame == null) HGrid.EMPTY else _metaFrame.toGrid
        //_metaFrame.toGrid
    }
  }

  def toDict():HDict = {
    _metaFrame.toDict
  }

  def clear() = {
    _errors.clear
    _metaFrame = null;
    _hisFrame = None
    _opType = null
    this
  }

  /**
    * Return count of records satisfying filter expression.
    * @param filter
    * @param limit
    * @return
    */
  def count(filter: String, limit: Int=0): Int = {
    _opType = IMContext.OP_COUNT
    val actualLimit: Int = if (limit == 0) this.limit else limit
    if (filter != null) {
      this.filter = filter
    }
    //println("%s: count: invoking store.readAll".format(new Date()))
    this.store.count(this.filter, this.project, actualLimit)
  }

  def readAll(filter: String, limit: Int=0): IMContext = {
    // TODO: change filter argument to pterm
    // pterm can be one of:
    //   String (axon expression)
    //   Seq[String]: sequence of ids (must be starting with '@')
    // i.e.:
    //   pterm: Either[String, Seq[String]]
    //if (_metaFrame != null) {
    //  throw new RuntimeException("_metaFrame already set. Invoke clear first.")
    //}
    _opType = IMContext.OP_READ
    readArg = Some(filter)
    val actualLimit: Int = if (limit == 0) this.limit else limit
    if (filter != null) {
      this.filter = filter
    }
    if (this.filter == null) throw new RuntimeException("filter argument missing")
    //"readAll: " + toString()
    //println("%s: readAll: invoking store.readAll".format(new Date()))
    _metaFrame = this.store.readAll(this.filter, this.project, actualLimit)
    //println("%s: readAll: done invoking store.readAll".format(new Date()))

    this
  }

  def readAll(): IMContext = {
    readAll(filter, 0)
  }


  /**
    * validate id
    * @param id
    * @return either unit or error string
    */
  private def validateId(id: String): Either[Unit, String] = {
    if (!id.startsWith("@")) {
      //throw new RuntimeException("id must start with '@': " + id)
      Right("id must start with '@': " + id)
    } else {
      try {
        // will throw exception if invalid uuid
        UUID.fromString(id.substring(1))
        Left()
      } catch {
        case e:Exception => Right("id must be of UUID format, such as: @1111-2222-...." )
      }
    }
  }

  def readById(id: String): IMContext = {
    _opType = IMContext.OP_READ
    readArg = Some(id)
    // TODO: use match here? alternative for RuntimeException?
    /*
    if match {
      case id.startsWith("@") =>
    }
    */
    _metaFrame =
    validateId(id) match  {
      case Right(e) => IMFrame(hsutil.errorGrid("haystack::UnknownRecErr", Some(e)), true)
      //case Right(e) => IMFrame(HGrid.EMPTY, false)
      case _ => this.store.readById(id.substring(1), this.project)
    }

    this
  }

  def hisReadJava(range: HDateTimeRange, id: String): IMContext = {
    hisRead(hsRangeToZtRange(range), id)
  }

  def hisRead(range: HDateTimeRange, pterm: Any): IMContext = {
    hisRead(hsRangeToZtRange(range), pterm)
  }

  def hisRead(range: HDateTimeRange): IMContext = {
    hisRead(hsRangeToZtRange(range), null)
  }

  //def hisRead(self, hrange, point_spec=None, rollup_interval=None, rollup_alg=None, truncate_seconds=True, limit=None):

  // def parseRange(s: String) Tuple2
  // def hisRead(range: Tuple, pterm: Any=null): IMContext = {
  // client hisRead(parseRange(...
  def hisRead(range: (java.time.ZonedDateTime, java.time.ZonedDateTime)): IMContext = {
    hisRead(range, null)
  }

  protected def hisRead(range: (java.time.ZonedDateTime, java.time.ZonedDateTime), pterm: Any): IMContext = {
    if (!checkPermissionForCall) {
      val msg = "Permission denied for function %s for user %s".format("hisRead", this.author)
      _errors.append(msg)
      _metaFrame = IMFrame(hsutil.errorGrid(hsutil.STANDARD_ERROR_PERMISSION_DENIED, Some(this._errors.mkString(" "))), true)
      return this
    }
    pterm match {
      case s: String => readById(s)
      case null =>
        if (_metaFrame == null) {
          // actualize _metaFrame from pterm
          readAll()
        }
    }
    // now that we know read is done, set our operation in progress
    _opType = IMContext.OP_HISREAD

    _hisFrame = this.store.hisRead(this._metaFrame, range, this.project, tz, this.limit) match {
      case Some(ifframe) => Some(ifframe)
      case _ =>
        //val stackTrace = Thread.currentThread().getStackTrace()
        //Option(IMFrame(hsutil.errorGrid("haystack::UnknownRecErr", this.readArg, stackTrace), true))
        Option(IMFrame(hsutil.errorGrid("haystack::UnknownRecErr", this.readArg), true))
    }
    this
  }

  def hisRead(range: String): IMContext = {
    hisRead(hsRangeToZtRange(HDateTimeRange.make(range, this.tzhs)), null)
  }
    /**
    * Performs history read.
    * hisRead results are stored in _hisFrame
    * Side-effects:
    *   _metaFrame will be populated with results of point retrieval (if pterm not null)
    *   _hisFrame will be populated with results of history retrieval
    * @param pterm: point term - specification of points for which histories should be retrieved
    *             pterm may encapsulate one or more points. If null, previously establish pterm
    *             for this instance will be used to retrieve histories.
    *
    * @param range datetime range specification, in one of the following formats:
    *                  string: haystack date range string
    *                  tuple (start, end) as tuple of datetimes
    *
    *
    * @return this instance
    */
    //def hisRead(range: String, pterm: Any=null): IMContext = {
    def hisRead(range: String, pterm: Any): IMContext = {
      //       ditto remove Any with pterm: String id, Map[String, Any], or IMFrame
      hisRead(hsRangeToZtRange(HDateTimeRange.make(range, this.tzhs)), pterm)
    }

  /**
    * Clear history for one or more points.
    * @param range Option for range, if None, all history to be cleared
    * @param pterm: point term (point specification) for points to be cleared
    * @return
    */
  //def hisClear(pterm: Either[String, IMFrame], range: Option[(ZonedDateTime, ZonedDateTime)]) : Either[Int, String] = {
  def hisClear(range: Option[(ZonedDateTime, ZonedDateTime)] = None, pterm: Any = null) : Either[String, Int] = {

    pterm match {
      case s: String => readById(s)
      case null =>
        if (_metaFrame == null) {
          // actualize _metaFrame from pterm
          readAll()
        }
    }
    _opType = IMContext.OP_HISCLEAR
    this.store.hisClear(this._metaFrame, range, this.project)

  }

  /**
    * Write new data for a point or a set of points
    * @param data: timeseries dataframe containing one or more point data histories
    * @return
    */
  def hisWrite(data: IMFrame): IMContext = {
    if (!checkPermissionForCall()) {
      val msg = "Permission denied for function %s for user %s".format("hisWrite", this.author)
      _errors.append(msg)
      _metaFrame = IMFrame(hsutil.errorGrid(hsutil.STANDARD_ERROR_PERMISSION_DENIED, Some(this._errors.mkString(" "))), true)
    } else {
      // unpack grid:
      _opType = IMContext.OP_HISWRITE
      val grid = data.toGrid
      /*
      single point:

          """ver:"2.0" id:@hisId
            |ts,val
            |2012-04-21T08:30:00-04:00 New_York,72.2
            |2012-04-21T08:45:00-04:00 New_York,76.3

        multipoint:

          """ver:"2.0"
            |ts,v0 id:@a,v1 id:@b
            |2012-04-21T08:30:00-04:00 New_York,72.2,76.3
            |2012-04-21T08:45:00-04:00 New_York,N,76.3


       */

      if (grid.numCols == 2) {
        val id = grid.meta().get("id").toZinc
        validateId(id) match {
          case Right(e) => _metaFrame =
            IMFrame(hsutil.errorGrid(hsutil.STANDARD_ERROR_INCORRECT_INPUT, Some(e)), false)
          case _ =>
            val ptframe = this.store.readById(id.substring(1), this.project)
            val pt = ptframe.toDict
            if (ptframe.toGrid.numRows == 0) _metaFrame = IMFrame(hsutil.errorGrid(hsutil.STANDARD_ERROR_UNKNOWN_REC, Some(id)), false)
            else {
              //_metaFrame = IMFrame(hsutil.errorGrid(hsutil.STANDARD_ERROR_UNKNOWN_REC, Some(id)), false)
              store.hisWrite(ptframe, data, project) match {
                // success
                case Left(u) => _metaFrame = IMFrame(HGrid.EMPTY, false)
                // failure
                case Right(e) => _metaFrame = IMFrame(hsutil.errorGrid(hsutil.STANDARD_ERROR_UPDATE, Some(e)), false)
              }
            }
        }
      } else {
        _metaFrame = IMFrame(hsutil.errorGrid(hsutil.STANDARD_ERROR_INCORRECT_INPUT, Some("Input grid must have 2 columns")), false)
      }
    }

    this
  }

  def delete(): IMContext = {
    _opType = IMContext.OP_DELETE
    Some(_metaFrame).map(store.delete(_, project)) match {
      case Some(Right(msg)) => _errors.append(msg)
      case _ => // noop
    }
    this
  }

  def delete(id: String): IMContext = {
    _opType = IMContext.OP_DELETE
    store.delete(id, project) match {
      case Right(msg) => _errors.append(msg)
      case Left(frame)  => _metaFrame = IMFrame(HGrid.EMPTY, false)
    }
    this
  }

  def delete(ids: Array[String]): IMContext = {
    _opType = IMContext.OP_DELETE
    store.delete(ids, project) match {
      case Right(msg) => _errors.append(msg)
      case Left(frame)  => _metaFrame = IMFrame(HGrid.EMPTY, false)
    }
    this
  }

  /**
    * Commit a new record or update an existing record.
    *
    * A new record frame may contain ids that are compliant with UUID standard format.
    * and must not existing in the node database for the current projet.
    * If not specified, random ids are generated.
    *
    * Example of frame specifying a new record with idds
    *
      ver:"2.0"
      id,equip,area,equipRef,point,his,geoAddr,geoCity,site,tz,discharge,geoState,fan,geoStreet,geoPostalCode,cov,navName,siteRef,origId,dis,power,geoCountry
      @2475ab1f-9c9b-4be2-9a48-51f7e6bb941a "Office",,6000,,,,"900 N Roop St, Carson City, NV 89701","Carson City",M,"Las_Vegas",,"NV",,"3504 W Cary St","89701",,,,"Office","Office",,"US"
      @642325dc-ca80-4ec0-b5a5-811c5f1d9a3b "Home",,3000,,,,"10800 Torre Ave, Cupertino, CA 95014","Cupertino",M,"Los_Angeles",,"CA",,"600 W Main St","95014",,,,"Home","Home",,"US"
      @6d9b70e3-af1b-469b-ad6d-365e67de3c44 "Office RTU-2",M,,,,,,,,"Las_Vegas",,,,,,,"RTU-O-02",@2475ab1f-9c9b-4be2-9a48-51f7e6bb941a,"Office.RTU-2","Office RTU-2",,

    *
    * @param diff frame to commit (diff for update, complete record set for add)
    * @param op operation - allows values add and update.
    *           add to create  new record, update for existing record update
    * @return
    */
  def commit(diff: IMFrame, op:String): IMContext = {
    _opType = IMContext.OP_COMMIT
    if (!checkPermissionForCall()) {
      val msg = "Permission denied for function %s for user %s".format("commit", this.author)
      _errors.append(msg)
      _metaFrame = IMFrame(hsutil.errorGrid(hsutil.STANDARD_ERROR_PERMISSION_DENIED, Some(this._errors.mkString(" "))), true)
    } else {
      val result = store.commit(diff, op, project, this.author)
      result match {
        case Left(result) => _metaFrame = result
        case Right(msg) => _errors.append(msg)
          _metaFrame = IMFrame(hsutil.errorGrid(hsutil.STANDARD_ERROR_INCORRECT_INPUT, Some(this._errors.mkString(" "))), true)
      }
    }
    this
  }

  override def toString() = {
    s"project=$project, filter=$filter, opType=$opType, readArg=$readArg"
  }

  def projSave(project: Project) = {
    projectRepo.save(project)
  }

  def checkPermissionForCall(): Boolean = {
    val stackTraceElements: Array[StackTraceElement] = Thread.currentThread().getStackTrace()
    val method = stackTraceElements(2)
    //println("checkPermissionForCall: " + method.getMethodName)
    // check read or write permissions based on function calling us
    method.getMethodName match {
      case "commit" => checkPermission("rw")
      case "hisWrite" => checkPermission("rw")
      case "eval" => checkPermission("rw")
      case "readAll" => checkPermission("r")
      case "readById" => checkPermission("r")
      case "hisRead" => checkPermission("r")
      case "count" => checkPermission("r")
      case _ => false
    }
  }

  def eval(expr: String): IMContext = {
    val expr = "adminEnrichRecords"
    if (!checkPermissionForCall) {
      val msg = "Permission denied for function %s for user %s".format("eval", this.author)
      _errors.append(msg)
      _metaFrame = IMFrame(hsutil.errorGrid(hsutil.STANDARD_ERROR_PERMISSION_DENIED, Some(this._errors.mkString(" "))), true)
      return this
    }

    // now that we know read is done, set our operation in progress
    _opType = IMContext.OP_EVAL
    _metaFrame = IMFrame(hsutil.errorGrid("haystack::UnspecifiedErr", Some("Operation disabled")), true)

    /* disabled for now
    _metaFrame = this.store.experiment1(this._metaFrame, this.project, this.author) match {
      case Left(ifframe) => ifframe
      case Right(s) =>
        IMFrame(hsutil.errorGrid("haystack::UnspecifiedErr", Some(s)), true)
      case _ =>
        IMFrame(hsutil.errorGrid("haystack::UnspecifiedErr", None), true)
    }
    */
    this


  }

    /**
    * Check permission for the current user
    * @param permission
    * @return
    */
  private def checkPermission(permission: String): Boolean = {

    val isAdmin = ADMIN_USERS.contains(this.author)
    // for now, only allow a white list of users
    permission match {
      case "r" => true
      case "rw" => isAdmin
      case "rwm" => isAdmin
      case _ => false
    }
  }

}

object IMContext {
  // possible operation that may be in progress
  // when hisWrite is invoked, OP_READ, OP_HISWRITE state changes occur
  // when readById or readAll is invoked, OP_READ state change occurs
  // when OP_HISREAD invoked, OP_READ, OP_HISREAD state changes occur
  val OP_READ     = "read"
  val OP_COUNT = "count"
  val OP_HISREAD  = "hisRead"
  val OP_HISWRITE = "hisWrite"
  val OP_HISCLEAR = "hisClear"
  val OP_COMMIT = "commit"
  val OP_DELETE = "delete"
  val OP_EVAL = "eval"
}