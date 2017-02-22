package iotus.core

import com.typesafe.config.ConfigFactory
import org.projecthaystack.io.HZincWriter
import org.scalatest.{FunSpec, Matchers}

class IotusSpec extends BaseSpec {

  val config = ConfigFactory.load("ldaptest.conf")
  val regularUserUsername = config.getString("ldapuser.username")
  val regularUserPassword = config.getString("ldapuser.password")


  describe("Singleton instance of Iotus") {
    it("should allow access to properties and sections of config") {
      //val iotus = Iotus()
      //iotus.start()
      Iotus.start()
      val config: YamlConfig = Iotus.getConfig()
      val db: String = config.getString("db")
      dumpIfVerbose("db=" + db)
      db shouldBe a[String]

      //db should not be null
      //db.toString should startWith "cassandra"
      // Some("cassandra://localhost:9042/iotus") was not an instance of java.lang.String, but an instance of scala.Some
      //iotus.getString("ldap") should not be null
      val ldapSection: Map[String, AnyRef] = config.getSection("ldap")
      dumpIfVerbose("ldapSection=" + ldapSection)
      // extra .get needed to retrieve String from Some(ldap://ldap.example.com:389)
      val url = ldapSection.get("url").get
      dumpIfVerbose("url=" + url)
      // url=Some(ldap://u16:389)
      url shouldBe a[String]

      val url2: String = config.getString("url", "ldap")
      dumpIfVerbose("url2=" + url2)
      // url=Some(ldap://u16:389)
      url2 shouldBe a[String]

    }

    it("should return multiple projects") {
      //val iotus = Iotus()
      //iotus.start()
      Iotus.start()
      val grid = Iotus.listProjects(null).toGrid
      dumpIfVerbose("All projects:\n" + HZincWriter.gridToString(grid))
      grid.numCols should be > 3
      grid.numRows should be > 1
    }

    it("projectByName should return a project if pid exists.") {
      Iotus.start()
      val grid = Iotus.projectByName("test-project").toGrid
      dumpIfVerbose("test-project by name:\n" + HZincWriter.gridToString(grid))
      grid.numCols should be > 3
      grid.numRows shouldEqual 1
    }

    it("projectByName should return empty grid if pid doesn't exist.") {
      Iotus.start()
      val grid = Iotus.projectByName("dummy-project").toGrid
      dumpIfVerbose("dummy-project by name:\n" + HZincWriter.gridToString(grid))
      grid.isEmpty shouldEqual true
      //grid.numCols shouldEqual 0
      //grid.numRows shouldEqual 0
    }

    it("should validate credentials") {
      Iotus.validateCredentials(regularUserUsername, regularUserPassword)
    }
  }

}
