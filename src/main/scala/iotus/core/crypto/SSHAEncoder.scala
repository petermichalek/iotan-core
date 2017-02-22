package iotus.core.crypto

import java.security.SecureRandom
import java.io.FileInputStream
import java.io.File
import java.security.MessageDigest
import java.util.Base64

/**
  * SSHA Encoder
  */
object SSHAEncoder extends CryptoEncoder {

  val prefix = "{SSHA}"

  def encrypt(password: String, salt: Option[Array[Byte]] = None, alg: Option[String] = None): String = {

    val passwordData = password.getBytes("UTF-8")
    // for now, only SSHA algorithm supported
    /*
    val (prefix, messageDigest) = alg.get.toUpperCase match {
      case "SHA256" | "SSHA256" => (if (salt.isDefined) "{SSHA256}" else "{SHA256}", MessageDigest
        .getInstance("SHA-256"))
      case "SHA" | "SSHA" => (if (salt.isDefined) "{SSHA}" else "{SHA}", MessageDigest.getInstance("SHA-1"))
      case "MD5" | "SMD5" => (if (salt.isDefined) "{SMD5}" else "{MD5}", MessageDigest.getInstance("MD5"))
      case _ => throw new UnsupportedOperationException("Not implemented")
    }
    */
    val md = MessageDigest.getInstance("SHA-1")
    //val digest = new Array[Byte](md.getDigestLength());

    md.update(passwordData)
    val realSalt = salt match {
      case Some(ba) => ba
      case _ => randomSalt(4)
    }
    md.update(realSalt)
    val digest = md.digest
    val out = digest ++ realSalt
    prefix + new String(encodeBase64(out), "UTF-8")
  }


  /**
    * Based on python code for SSHA ldap encoding/decoding
    *  https://gist.github.com/rca/7217540
    *
    * In the future, if there is a need to use other SSHA algorithmes other than SHA-1, see
    *   https://github.com/untoldwind/lostsocks/blob/master/server/app/utils/crypto/PasswordEncoder.scala
    * @param tagged_digest_salt hashed password to verify
    * @param password
    * @return
    */
  override def check_password(tagged_digest_salt: String, password: String): Boolean = {
    if (!tagged_digest_salt.startsWith(prefix)) {
      false
    } else {
      // extract hash from tagged hash
      val digest_salt_b64 = tagged_digest_salt.substring(6)
      val md = MessageDigest.getInstance("SHA-1")
      val digest_salt = decoder.decode(digest_salt_b64)
      // extract digest and salt from original complete hash
      val digest = digest_salt.slice(0, 20)
      val salt = digest_salt.slice(20, digest_salt.length)
      // calculate new digest
      md.update(password.getBytes)
      md.update(salt)
      val newdigest = md.digest()
      // compare new and original digest
      newdigest sameElements digest
    }
  }
}
