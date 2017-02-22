package iotus.core

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.querybuilder.QueryBuilder._
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FunSpec, Matchers}

class ConnectionAndQuerySpec extends FunSpec with Matchers {

  val config:Config = ConfigFactory.load("ldaptest.conf")
  // "cassandra://localhost:9042/test"
  val dbUrl = config.getString("dburl")

  describe("Connecting and querying a Cassandra database") {
    it("should just work") {
      val uri = CassandraConnectionUri(dbUrl)
      //val uri = CassandraConnectionUri("cassandra://localhost:9042/cd")
      val session = Helper.createSessionAndInitKeyspace(uri)
      
      session.execute("CREATE TABLE IF NOT EXISTS things (id int, name text, PRIMARY KEY (id))")
      session.execute("INSERT INTO things (id, name) VALUES (1, 'foo');")

      val selectStmt = select().column("name")
        .from("things")
        .where(QueryBuilder.eq("id", 1))
        .limit(1)
      
      val resultSet = session.execute(selectStmt)
      val row = resultSet.one()
      row.getString("name") should be("foo")
      session.execute("DROP TABLE things;")
    }
  }

}
