package iotus.core

import java.io.{File, FileInputStream, InputStream}

import org.yaml.snakeyaml.Yaml
import scala.collection.JavaConversions._

/*
TODO: BETTER convert types: how to use implicits/type conversions

implicit def convert(javaMap: Hashtable[String, String]) = {
    Map.empty ++ new MapWrapper[String, String]() {
        def underlying = javaMap
    }
}


val hashtable: java.util.Hastable[String, String] = new java.util.Hashtable[String, String]()
hashtable.put("some_key", "some_value")

val map = hashtable.asInstanceOf[Map[String, String]]


 */
/**
  * Yaml configuration for IoTus system
  */
case class YamlConfig() {

  val yaml:Yaml  = new Yaml()
  val input:InputStream  = new FileInputStream(new File(YamlConfig.CONFIG_PATH))
  //var config:scala.collection.mutable.Map[String, Any] = yaml.load(input)
  //val config: java.util.Map[String, Object] = yaml.load(input)
  val config:java.util.Map[String, Object] = yaml.load(input).asInstanceOf[java.util.Map[String, Object]]
  val yamlConfig:Map[String, AnyRef] = mapAsScalaMap[String, Object](config).toMap.asInstanceOf[Map[String, AnyRef]]
  //val yamlConfig:scala.collection.mutable.Map[String, Object] = mapAsScalaMap[String, Object](config)
  //val ldapSection:Map[String, String] = mapAsScalaMap[String, Object](yamlConfig.getOrElse("ldap", null).asInstanceOf[java.util.Map[String, Object]])
  //asInstanceOf[Map[String, String]]
  //System.out.println("ldapSection=" + ldapSection)

  /**
    * Retrieve a string value from root of configuration (no subsection)
    * @param name
    * @return a String value
    */
  def getString(name: String): String = {
    yamlConfig.get(name).get.asInstanceOf[String]

    /*
    val result = Option(yamlConfig.get(name)) match {
      case Some(...) =>   ...
      case None => ...
    }
    */
  }

  /**
    * Retrieve a string value from subsection of configuration (no subsection)
    * @param name
    * @param section
    * @return a String value
    */
  def getString(name: String, section: String): String = {
    getSection(section).get(name).get
  }

  def getSection(name: String): Map[String, String] = {
    val obj = yamlConfig.get(name).get.asInstanceOf[java.util.LinkedHashMap[String, AnyRef]]
    val section = mapAsScalaMap[String, AnyRef](obj).toMap
    section.asInstanceOf[Map[String, String]]
  }

}

object YamlConfig {
  val CONFIG_PATH = "/opt/iotus/conf/iotus.yaml"
  //def apply(): YamlConfig = {
  //  return new YamlConfig()
  //}
}