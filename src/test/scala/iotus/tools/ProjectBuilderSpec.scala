package iotus.tools

import com.typesafe.config.{Config, ConfigFactory}
import iotus.core._
import _root_.iotus.core.model.Project
import org.projecthaystack.GridWrap
import org.projecthaystack.io.HZincReader
import org.scalatest.{FunSpec, Matchers}

import scala.util.Left

class ProjectBuilderSpec extends BaseSpec {

  //val zincFile = "./src/main/resources/smalloffice-structure.zinc"
  val zincFile = "./src/main/resources/r.zinc"
  val zincFileSimple = "./src/main/resources/iotus-simple.zinc"
  val r = scala.util.Random
  val config: YamlConfig = Iotus.getConfig()
  val session: StoreSession = StoreSession.make(config.getString("db"))
  val projectRepo = new ProjectRepository("project", session)

  describe("Build demo") {

    it("Transformation from file should work") {
      // generate random project name
      val num = r.nextInt(1000)
      val projectName = "unittest-proj-%04d".format(num)
      //val projectName = "unittest-proj-0545"
      ProjectBuilder.createProject(projectName, "America/Los_Angeles", zincFileSimple, skipProjectCreate = false) match {
        case Right(u) => // ok
          println("Returned Right(u) as expected")
        case Left(s) =>
          fail(s)
      }
      cleanupProject(projectName)
    }

    it("Project creation negative test") {
      // generate random project name
      val num = r.nextInt(1000)
      val projectName = "unittest-proj-%04d".format(num)
      dumpIfVerbose(s"project: $projectName")
      ProjectBuilder.createProject(projectName, "America/Los_Angeles",
          zincFileSimple, skipProjectCreate = true) match {
        case Right(u) => fail("Populating non-existing project should fail.")
        case Left(s) =>
          // ok
          dumpIfVerbose(s"Response as expected: $s")
      }
      // create project for next test
      ProjectBuilder.createProject(projectName, "America/Los_Angeles",
          zincFileSimple, skipProjectCreate = false) match {
        case Right(u) => // ok
          println("Returned Right(u) as expected")
        case Left(s) => fail(s)
      }

      ProjectBuilder.createProject(projectName, "America/Los_Angeles",
        zincFileSimple, skipProjectCreate = false) match {
        case Right(u) => fail("Populating an existing project while creating it should fail.")
        case Left(s) =>
          // ok
          dumpIfVerbose(s"Response as expected: $s")
      }
      cleanupProject(projectName)

    }

    it("transformGrid test with display of subset of columns") {
      val source = scala.io.Source.fromFile(zincFileSimple)
      val data = source.mkString
      val grid = new HZincReader(data).readGrid()
      source.close()
      val wgrid = GridWrap(grid)
      val newgrid: GridWrap = ProjectBuilder.transformGrid(wgrid) match {
        case Right(g) =>
          g
        case Left(s) =>
          fail(s)
      }
      //dumpIfVerbose(newgrid.filterCols(Set("id", "dis", "oldId", "origId", "cxId", "equipRef", "siteRef", "bldgRef")).asZinc(3))
      dumpIfVerbose(newgrid.filterCols(Set("id", "dis", "origId", "equipRef", "siteRef", "bldgRef")).asZinc(100))
    }
    /*
    it("Small Office demo should work") {
      ProjectBuilder.createProject("x3", "Los_Angeles", zincFile)
    }
    */
  }

  // -------------------------------------------------------
  // --------------- Helper Functions ----------------------
  // -------------------------------------------------------
  def cleanupProject(pid: String) = {
    // cleanup
    val project: Option[Project] = projectRepo.readByProp("pid", pid)
    project match {
      case Some(p) =>
        dumpIfVerbose(s"Removing project ${p.pid} by id ${p.id}")
        projectRepo.delete(project.get.id)
      case _ => fail(s"Didn't find project $pid in project repo after creation.")
    }
  }


}
