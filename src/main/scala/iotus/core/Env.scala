package iotus.core

import model.Project

/**
  * Encapsulates the singleton environment class for IoTus System.
  *
  * Similar to Iotus class/object, but more low-level with direct object model access and
  * without Java compatibility, thus making it easier to use from Scala.
  */
object Env {

  // handle to Iotus instance for helper object access
  val iotus = Iotus
  val defaultTimezone = iotus.defaultTimezone
  /**
    * Returns a list of entities available for the user that is currently authenticated with
    *  the system.
    * @return IMFrame representing a response, including list of entities.
    */
  def listProjects(filter: Option[String]): Seq[Project] = {
    iotus.projectRepo.readAll(filter)
  }
  /**
    * Retrieve project instance by project short name (pid)
    * @param projectName project short name (pid) for the project to retrieve
    * @return IMFrame representing the project entity
    */
  def projectByName(projectName: String): Option[Project] = {
    iotus.projectRepo.readByProp("pid", projectName)
  }


}