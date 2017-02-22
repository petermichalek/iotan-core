package iotus.core

import java.util.Properties
import javax.naming.{Context, NamingEnumeration}
import javax.naming.directory.{InitialDirContext, SearchControls, SearchResult}

import scala.util.{Failure, Success, Try}

/**
  * Ldap implementation of AuthValidator
  */
case class LdapAuthValidator(ldapUrl: String, adminUser: String, adminPassword: String, searchBase: String) extends AuthValidator {

  override def validateCredentials(username: String, password: String): Boolean = {
    doValidateCredentials(username, password)
  }

  private def doValidateCredentials(username: String, passcode: String): Boolean = {

    val result = Try {

      // todo move initial context retrieval to static function, then clone it for uid bind
      // see http://stackoverflow.com/questions/2522770/how-to-check-user-password-in-ldap-whith-java-with-given-ldapcontext
      var props = new Properties
      props.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
      props.put(Context.PROVIDER_URL, ldapUrl)
      props.put(Context.SECURITY_PRINCIPAL, s"cn=$adminUser,$searchBase")
      props.put(Context.SECURITY_CREDENTIALS, s"$adminPassword")

      var context: InitialDirContext = new InitialDirContext(props)

      val controls: SearchControls = new SearchControls
      //controls.setReturningAttributes(Array[String]("givenName", "sn", "memberOf", "cn"))
      // alternative to uid bind, user ssh encrypt and read userPassword
      // see http://gurolerdogan.blogspot.com/2010/03/ssha-encryption-with-java.html
      controls.setReturningAttributes(Array[String]("givenName", "sn", "memberOf", "cn", "userPassword"))
      controls.setSearchScope(SearchControls.SUBTREE_SCOPE)

      //val answers: NamingEnumeration[SearchResult] = context.search(searchBase, s"cn=$username", controls)
      val answers: NamingEnumeration[SearchResult] = context.search(searchBase, s"uid=$username", controls)
      val result: SearchResult = answers.nextElement

      val user: String = result.getNameInNamespace
      props = new Properties
      props.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
      props.put(Context.PROVIDER_URL, ldapUrl)
      props.put(Context.SECURITY_PRINCIPAL, user)
      props.put(Context.SECURITY_CREDENTIALS, passcode)
      context = new InitialDirContext(props)
    }
    result match {
      case Success(v) => true
      case Failure(v) => false
    }
  }
}
