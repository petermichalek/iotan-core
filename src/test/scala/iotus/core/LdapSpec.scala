package iotus.core.target

import iotus.core.{BaseSpec, LdapAuthValidator}
import org.scalatest.{FunSpec, Matchers}
import com.typesafe.config.ConfigFactory

class LdapValidatorSpec extends BaseSpec {

  val config = ConfigFactory.load("ldaptest.conf")
  val ldapUrl = config.getString("ldap.url")
  val searchBase = config.getString("ldap.searchBase")
  val adminUser = config.getString("ldap.username")
  val adminPassword = config.getString("ldap.password")


  describe("Ldap auth") {
    it("should work") {

      val authValidator = new LdapAuthValidator(ldapUrl, adminUser, adminPassword, searchBase)
      val rc = authValidator.validateCredentials("frank", "wrongsecret")
      rc shouldBe false
      dumpIfVerbose("rc=" + rc)
      val ldapuserUsername = config.getString("ldapuser.username")
      val ldapuserPassword = config.getString("ldapuser.password")
      val rc2 = authValidator.validateCredentials(ldapuserUsername, ldapuserPassword)
      rc2 shouldBe true
      dumpIfVerbose("rc2=" + rc2)

    }
  }

}
