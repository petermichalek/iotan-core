package iotus.core

import java.net.URI

case class CassandraConnectionUri(dbUrl: String) {

  private val uri = new URI(dbUrl)

  private val additionalHosts = Option(uri.getQuery) match {
    case Some(query) => query.split('&').map(_.split('=')).filter(param => param(0) == "host").map(param => param(1)).toSeq
    case None => Seq.empty
  }

  val host = uri.getHost
  val hosts = Seq(uri.getHost) ++ additionalHosts
  val port = uri.getPort
  val keyspace = uri.getPath.substring(1)

}

