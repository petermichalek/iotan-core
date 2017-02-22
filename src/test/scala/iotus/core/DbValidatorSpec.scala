package iotus.core

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FunSpec, Matchers}

class DbValidatorSpec extends BaseSpec {

  val config:Config = ConfigFactory.load("ldaptest.conf")
  //val config: YamlConfig = Iotus.getConfig()

  describe("Db auth") {

    /*
    it("passwordToHash should work") {
      //           'pbkdf2(1000,20,sha512)$ad75f71b70cc23f7$e34b95270e6886c49ebfd3c1d5d2711a69706dab'

      val authValidator:AuthValidator = new DbAuthValidator(config.getString("dburl"))
      val r1 = authValidator.passwordToHash("beta", "pbkdf2", "ad75f71b70cc23f7")
      println("r1=", r1)
      r1 shouldEqual("e34b95270e6886c49ebfd3c1d5d2711a69706dab")
    }
    */

    it("validateCredentials should work") {
      val dbUsername = config.getString("dbuser.username")
      val dbPassword = config.getString("dbuser.password")
      //val user = "frank@michalek.org"
      val authValidator:AuthValidator = new DbAuthValidator(config.getString("dburl"))
      //val rc = authValidator.validateCredentials("frank", "wrongsecret")
      val rc = authValidator.validateCredentials(dbUsername, "wrongsecret")
      rc shouldBe false
      dumpIfVerbose("rc=" + rc)
      val rc2 = authValidator.validateCredentials(dbUsername, dbPassword)
      rc2 shouldBe true
      println("rc2=" + rc2)
      val dbUsername2 = config.getString("dbuser.username2")
      val dbPassword2 = config.getString("dbuser.password2")
      val rc3 = authValidator.validateCredentials(dbUsername2, dbPassword2)
      rc3 shouldBe true
      dumpIfVerbose("rc3=" + rc3)

    }
  }

}
