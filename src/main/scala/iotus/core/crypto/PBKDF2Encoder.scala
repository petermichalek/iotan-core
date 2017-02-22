package iotus.core.crypto

import java.security.MessageDigest
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.PBEKeySpec

/**
  * PBKDF2 Encoder
  */
object PBKDF2Encoder extends CryptoEncoder {

  val prefix = "pbkdf2(1000,20,sha512)"

  def encrypt(password: String, salt: Option[Array[Byte]] = None, alg: Option[String] = None): String = {

    // for now, only pbkdf2(1000,20,sha512) with that specific
    // number of iterations and size and algorithm is supported

    val passwordChars = password.toCharArray
    val realSalt = salt match {
      case Some(ba) => ba
      // salt size: 8, e.g.: bd8848decc977593
      case _ => bytes2hex(randomSalt(8)).getBytes
    }

    val iterations = 1000
    val key_length = 20

    val spec = new PBEKeySpec(
      passwordChars,
      realSalt,
      iterations,
      key_length * 8
    )
    val key = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512")
    val newdigest = key.generateSecret(spec).getEncoded()
    val out = "%s$%s$%s".format(prefix, new String(realSalt), bytes2hex(newdigest))
    out
  }


  /**
    * Example hash:
    * pbkdf2(1000,20,sha512)$bd8848decc977593$9fb52e83f7271355fb06d4fb86382b35f96b0a5e
    * @param tagged_digest_salt hashed password to verify
    * @param password
    * @return
    */
  override def check_password(tagged_digest_salt: String, password: String): Boolean = {
    if (!tagged_digest_salt.startsWith(prefix)) {
      false
    } else {
      val parts = tagged_digest_salt.split("\\$")
      if (parts.length != 3) {
        false
      } else {
        // extract hash from tagged hash
        // pbkdf2(1000,20,sha512)$bd8848decc977593$9fb52e83f7271355fb06d4fb86382b35f96b0a5e
        // assuming always iterations 1000, key length 20, alg sha512
        val passwordChars = password.toCharArray
        //val salt = parts(1)
        val digest = parts(2)
        val saltBytes = parts(1).getBytes
        val iterations = 1000
        val key_length = 20

        val spec = new PBEKeySpec(
          passwordChars,
          saltBytes,
          iterations,
          key_length * 8
        )
        // PBKDF2WithHmacSHA512 Vs. PBKDF2WithHmacSHA1
        // PBKDF2WithHmacSHA256
        val key = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512")
        val newdigest = key.generateSecret(spec).getEncoded()
        val newdigeststr = bytes2hex(newdigest)
        //bytes2hex(hashedPasswordBytes)
        // compare new and original digest
        newdigeststr sameElements digest
      }
    }
  }
}
