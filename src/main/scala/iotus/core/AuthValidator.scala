package iotus.core

/**
  * Trait (Interface) for authentication validation.
  */
trait AuthValidator {
  def validateCredentials(username: String, password: String): Boolean
}

