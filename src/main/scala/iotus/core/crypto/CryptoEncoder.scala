package iotus.core.crypto

import java.io.{File, FileInputStream}
import java.security.{SecureRandom}
import java.util.Base64

/**
  * Crypto Encoder.
  * Used as interface/trait/base class for concrete encoders
  */
trait CryptoEncoder {

  val encoder = Base64.getEncoder
  val decoder = Base64.getDecoder

  def encodeBase64(in: Array[Byte]):Array[Byte] = encoder.encode(in)
  def decodeBase64(in: Array[Byte]):Array[Byte] = decoder.decode(in)


  /**
    * Encrypt password using optional given salt, returning hash to be stored in user's "password" record
    * @param password
    * @param salt
    * @param alg: ignored for now
    * @return
    */
  def encrypt(password: String, salt: Option[Array[Byte]] = None, alg: Option[String] = None): String

  /**
    * Verify/check password.
    * @param tagged_digest_salt hashed password from user's "password" record
    * @param password actual password to verify
    * @return
    */
  def check_password(tagged_digest_salt: String, password: String): Boolean

  /**
    * Create random salt.
    * @param length
    * @return
    */
  def randomSalt(length: Short = 64): Array[Byte] = {
    val salt = new Array[Byte](length)
    random.nextBytes(salt)
    salt
  }

  private val random: SecureRandom = {
    try {
      val instance = SecureRandom.getInstance("SHA1PRNG")
      val urandom = new File("/dev/urandom")

      if (urandom.exists()) {
        val is: FileInputStream = new FileInputStream(urandom);
        val salt = Iterator.continually(is.read).take(8192).map(_.toByte).toArray
        is.close()
        instance.setSeed(salt)
      }
      instance
    } catch {
      case _ => new SecureRandom()
    }
  }

  def hex2bytes(hex: String): Array[Byte] = {
    hex.replaceAll("[^0-9A-Fa-f]", "").sliding(2, 2).toArray.map(Integer.parseInt(_, 16).toByte)
  }

  def bytes2hex(bytes: Array[Byte], sep: String = ""): String = bytes.map("%02x".format(_)).mkString(sep)

}

/**
  * Utility BaseEncoder used to execute CryptoEncoder utility functions in standalone test mode.
  */
object BaseEncoder extends CryptoEncoder {
  override  def encrypt(password: String, salt: Option[Array[Byte]] = None,
                        alg: Option[String] = None): String = throw new NotImplementedError()
  override def check_password(tagged_digest_salt: String, password: String): Boolean = throw new NotImplementedError()
}

