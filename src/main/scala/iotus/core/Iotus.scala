package iotus.core

import model.Project
import model.Conversions.{entitiesToIMFrame, entityToIMFrame}
import org.projecthaystack.HGrid
/**
  * Encapsulates the singleton management class for IoTus System.
  */
object Iotus {



  private var config:YamlConfig = null
  private var authValidator:AuthValidator = null

  // load config file
  var ldapUrl:String = null
  var ldapUsername:String = null
  var ldapPassword:String = null
  var ldapSearchBase:String = null

  var auth:String = null

  var dbUrl:String = null
  var defaultTimezone:String = null
  var projectRepo:ProjectRepository = null
  /**
    * database session to use for the repository
    */
  private var session: StoreSession = null;

  // preload so that config is available before strat()
  // reload()
  Iotus()

  def Iotus() = {

    try {
      reload
    } catch {
      case e:Exception => throw new RuntimeException(
        "Initialization failed: perhaps configuration is incorrect or resources are down, such as the database can't be reached?",
        e)
    } finally {}
  }

  /**
    * Reloads IoTus configuration from the yaml file.
    */
  private def reload(): Unit = {
    config = new YamlConfig
    // load config file
    auth = config.getString("auth")
    if (auth == "ldap") {
      val ldapInfo:Map[String, String] = config.getSection("ldap")
      ldapUrl = ldapInfo("url")
      ldapUsername = ldapInfo("username")
      ldapPassword = ldapInfo("password")
      ldapSearchBase = ldapInfo("searchBase")
    }
    dbUrl = config.getString("db")
    defaultTimezone = config.getString("defaultTimezone")
    session = StoreSession.make(dbUrl)
    projectRepo = new ProjectRepository("project", session)

  }
  /**
    * Starts IoTus service.
    */
  def start(): Unit = {
    reload()
    /*
    case auth "ldap":
      authValidator =
    */
    if (auth == "ldap") {
      authValidator = new LdapAuthValidator(ldapUrl, ldapUsername, ldapPassword, ldapSearchBase)
    } else if (auth == "db") {
      authValidator = new DbAuthValidator(config.getString("db"))
    } else {
      throw new RuntimeException("Unsupported auth validator: " + auth)
    }

  }

  def validateCredentials(username: String, password: String): java.lang.Boolean = {
    authValidator.validateCredentials(username, password)
  }

  /**
    * Returns IoTus configuration/properties
    * @return
    */
  def getConfig(): YamlConfig = { return config}


  /**
    * Stops IoTus service.
    */
  def stop(): Unit = {
    session.close
    projectRepo = null
  }


  /**
    * Returns a list of entities available for the user that is currently authenticated with
    *  the system.
    * @return IMFrame representing a response, including list of entities.
    */
  def listProjects(filter: String): IMFrame = {
    // translate from filter Java-compatible possibly null argument to scala Option
    val actualFilter = if (filter == null) None else Some(filter)
    val projects: Seq[Project] = projectRepo.readAll(actualFilter)
    entitiesToIMFrame(projects)
  }
  /**
    * Retrieve project instance by project short name (pid)
    * @param projectName project short name (pid) for the project to retrieve
    * @return IMFrame representing the project entity
    */
  def projectByName(projectName: String): IMFrame = {
    // since our database (Cassandra) supports only search on primary or secondary indexes,
    //  we can retrieve project by name (pid) if pid is a secondary index
      //  other wise we would have to do this:
      // we would have to retrieve all projects and then filter by pid
      //val projects: Seq[Project] = projectRepo.readAll(null).filter(x => x.pid == projectName)

    val project: Option[Project] = projectRepo.readByProp("pid", projectName)
    project match  {
      case Some(p) => entityToIMFrame(p)
      case None => IMFrame(HGrid.EMPTY, false)
    }
  }


}