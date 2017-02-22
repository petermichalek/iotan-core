package iotus.core

import org.scalatest.{FunSpec, Matchers}
import model.{Conversions, Project, User}
import org.projecthaystack.io.HZincWriter

class RepositorySpec extends BaseSpec {

  val config: YamlConfig = Iotus.getConfig()
  val session: StoreSession = StoreSession.make(config.getString("db"))
  // project=Some(Project(856716f7-2e54-4715-9f00-91dcbea6c101,test-project,America/Los_Angeles))
  val TEST_PROJECT_ID = "856716f7-2e54-4715-9f00-91dcbea6c101"
  val UNIT_TEST_PID_PREFIX = "unit-test"
  val TEST_PROJECT_PID1 = s"$UNIT_TEST_PID_PREFIX-01"
  val TEST_PROJECT_PID2 = s"$UNIT_TEST_PID_PREFIX-02"
  //

  val TEST_USER_ID = "856716f7-2e54-4715-9f00-91dcbea66664"
  val UNIT_TEST_USERNAME_PREFIX = "unittest"
  val TEST_USER_FIRSTNAME1 = s"$UNIT_TEST_PID_PREFIX-Freddy01"
  val TEST_USER_FIRSTNAME2 = s"$UNIT_TEST_PID_PREFIX-Freddy02"

  val projectRepo = new ProjectRepository("project", session)
  val userRepo = new UserRepository("user", session)

  describe("ProjectRepository") {
    it("readById should work") {
      val p: Option[Project] = projectRepo.readById(TEST_PROJECT_ID)
      dumpIfVerbose("project=" + p)
    }

    it("readByProp should work for existing project pid") {
      val p: Option[Project] = projectRepo.readByProp("pid", "test-project")
      dumpIfVerbose("project=" + p)
      p shouldBe an [Option[Project]]
      p should not be None
    }

    it("readByProp should work for non-existent pid and return None") {
      val p: Option[Project] = projectRepo.readByProp("pid", "dummy-project")
      dumpIfVerbose("project=" + p)
      p shouldBe an [Option[Project]]
      p shouldEqual None
    }

    it("project readAll should work and entities should convert to IMFrame") {
      val projects: Seq[Project] = projectRepo.readAll()
      dumpIfVerbose("entities=" + projects)
      // entities=List(Project(856716f7-2e54-4715-9f00-91dcbea6c103,demo-02,America/New_York),
      // Project(856716f7-2e54-4715-9f00-91dcbea6c101,test-project,America/Los_Angeles),
      // Project(856716f7-2e54-4715-9f00-91dcbea6c102,demo,America/New_York))
      projects.size should be > 0
      val frame = Conversions.entityToIMFrame(projects(0))
      val grid = frame.toGrid
      dumpIfVerbose(HZincWriter.gridToString(grid))

      val frame2 = Conversions.entitiesToIMFrame(projects)
      dumpIfVerbose(HZincWriter.gridToString(frame2.toGrid))

    }


    it("save should work for new project") {
      val p = Project(TEST_PROJECT_PID1, "America/Los_Angeles")
      dumpIfVerbose("saving project=" + p)
      projectRepo.save(p)
      // newProject.mod should be > yesterday
      //dumpIfVerbose("save result for new project=" + p)
      // newProject should be type Option[Project]
      //newProject.get.id shouldBe a [String]

      // now query make sure it exists
      val createdP: Option[Project] = projectRepo.readById(p.id)
      createdP match {
        case Some(created) =>
          created.id shouldEqual p.id
          created.pid shouldEqual p.pid
          created.tz shouldEqual p.tz
          created.mod should not equal p.mod
        case _ => fail("Created project should be Some(project)")
      }

    }

    it("save should work for modified project") {
      /*
      // create project to be modified
      val p = Project(TEST_PROJECT_PID2, "America/Los_Angeles")
      dumpIfVerbose("saving project=" + p)
      val newProject = projectRepo.save(p)
      */
      val projects: Seq[Project] = projectRepo.readAll()
      // filter by unit test id
      val unitTestProjects: Seq[Project] = projects.filter((x: Project) => x.pid.contains(UNIT_TEST_PID_PREFIX) )

      dumpIfVerbose("unitTestProjects: " + unitTestProjects)

      // take the first one
      val p:Project = unitTestProjects(0)
      val pmodified = Project(p.id, p.pid, "Asia/Tokyo")
      dumpIfVerbose("saving project=" + pmodified)
      projectRepo.save(pmodified)
      //dumpIfVerbose("save result for modified project=" + modifiedProjectResult)
      // newProject.mod should be > yesterday
      //p.id shouldEqual modifiedProjectResult.get.id
      //modifiedProjectResult.get.tzstr shouldEqual "Asia/Tokyo"
      val readModifiedProject = projectRepo.readById(p.id).get
      p.id shouldEqual readModifiedProject.id
      readModifiedProject.tzstr shouldEqual "Asia/Tokyo"

    }
    it("cleanup of recently created test entities via save should work and remove some entities") {

      val projects: Seq[Project] = projectRepo.readAll()
      // filter by unit test id
      val unitTestProjects: Seq[Project] = projects.filter((x: Project) => x.pid.contains(UNIT_TEST_PID_PREFIX) )
      val size = unitTestProjects.size
      dumpIfVerbose(s"There are $size unitTestProjects: $unitTestProjects")

      // for all ids, remove entities
      for (id <- unitTestProjects.map(x => x.id)) {
        dumpIfVerbose(s"Removing $id")
        projectRepo.delete(id)
      }

      val projectsAfter: Seq[Project] = projectRepo.readAll().filter((x: Project) => x.pid.contains(UNIT_TEST_PID_PREFIX) )
      projectsAfter.size shouldEqual 0

    }

    // TODO: implement purge test
    it("test purge repo of records marked deleted.") {
      // projectRepo.purge()
    }

  }

  describe("UserRepository") {
    it("readById should work") {
      val u: Option[User] = userRepo.readById(TEST_USER_ID)
      dumpIfVerbose("user=" + u)
      u shouldBe an[Option[User]]

    }
  }


}
