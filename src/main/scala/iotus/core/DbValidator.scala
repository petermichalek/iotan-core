package iotus.core

import javax.naming.{Context, NamingEnumeration}
import javax.naming.directory.{InitialDirContext, SearchControls, SearchResult}

import com.typesafe.config.Config
import iotus.core.crypto.{CryptoEncoder, PBKDF2Encoder, SSHAEncoder}
import iotus.core.model.Conversions._
import iotus.core.model.User
import org.projecthaystack.HGrid

import scala.util.{Failure, Success, Try}


/**
  * Ldap implementation of AuthValidator
  */
case class DbAuthValidator(db: String) extends AuthValidator {

  val session: StoreSession = StoreSession.make(db)
  val userRepo = new UserRepository("user", session)

  // encoders available for auth validation
  val encoders:Map[String, CryptoEncoder] = collection.immutable.HashMap(
      "pbkdf2" -> PBKDF2Encoder,
      "ssha" -> SSHAEncoder
  )

  override def validateCredentials(username: String, password: String): Boolean = {
    doValidateCredentials(username, password)
  }

  /**
    * Allows validation of password based on the following hash saved in password field:
    * See Encoders in crypto package.
    * 1. {SSHA} as used by ldap
    *   e.g. ldap original {SSHA}NSnH/YyS+VvtNvdthYCw1MlyRiYF+sVX
    *    {SSHA}h3FFDd1A9SC2AQ9DRwog0EbMDrr5NvZP
    *    {SSHA}8IOH0VM6uEPD/u765Yfwwut6mLnu6UQP

    *   tranlatest to db {SSHA}$$NSnH/YyS+VvtNvdthYCw1MlyRiYF+sVX for
    *   compatibility with web2py pbkdf2 3-parts format
    *   Algorithm - see https://gist.github.com/rca/7217540
    *   This is so that a transition from ldap to db and db to ldap can be made, if needed
    * 2. {pbkdf2} as used by web2py
    *   e.g. pbkdf2(1000,20,sha512)$bd8848decc977593$9fb52e83f7271355fb06d4fb86382b35f96b0a5e
    *     where :
    *     alg: pbkdf2(1000,20,sha512)
    *     salt: bd8848decc977593
    *     hash: 9fb52e83f7271355fb06d4fb86382b35f96b0a5e
    *   Algorithm - see https://github.com/web2py/web2py/blob/master/gluon/contrib/pbkdf2.py
    *                   http://howtodoinjava.com/security/how-to-generate-secure-password-hash-md5-sha-pbkdf2-bcrypt-examples/
    *                   https://gist.github.com/tmyymmt/3727124
    *                   new PBEKeySpec("p2016".toCharArray, HexBytesUtil.hex2bytes("bd8848decc977593"), 1000, 20*8)
    *   This is a more modern and much more secure method of storing passwords, but may not be available
    *   in a given ldap installation:
    *   https://github.com/hamano/openldap-pbkdf2
    * @param username
    * @param passcode
    * @return
    */
  private def doValidateCredentials(username: String, passcode: String): Boolean = {
      val user:Option[User] = userRepo.readByProp("email", username)
      user match  {
        case Some(u) =>

          // user found
          // validate password
          // supported format: 3-part, parts delimited by $ sign
          // examples:
          // pbkdf2(1000,20,sha512)$bd8848decc977593$9fb52e83f7271355fb06d4fb86382b35f96b0a5e
          // {SSHA}$$NSnH/YyS+VvtNvdthYCw1MlyRiYF+sVX

          val alg = if (u.passwordHash.startsWith("pbkdf2")) "pbkdf2" else if (u.passwordHash.startsWith("{SSHA}")) "ssha" else "unknown"
          if (alg == "unknown") false
          else {
            val encoder = encoders.get(alg)
            encoder match {
              case Some(e) => e.check_password(u.passwordHash, passcode)
              case _ => false
            }
          }
        case _ => false
      }
  }

}
