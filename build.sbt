import AssemblyKeys._
assemblySettings

packSettings

// [Optional: Mappings from a program name to the corresponding Main class ]
//packMain := Map("hello" -> "myprog.Hello")

//val cassandraVersion = "3.1.1"
val cassandraVersion = "3.0.0"
val scalatestVersion = "2.2.0"
val argonautVersion = "6.0.4"
val saddleVersion = "1.3.+"
//val playJson = "2.3.4"
val jsonVersion = "1.0.4"
val snameyamlVersion = "1.9"
val typesafeConfigVersion = "1.3.1"
val scalaTimeVersion = "0.4.1"
val quillAsyncVersion = "0.8.0"
val quillCassVersion = "1.0.1-SNAPSHOT"

organization := "IoT Micro Services Project"

name := "iotus-core"

version := "0.1.01"

scalaVersion := "2.11.7"
// 2.12 for now doesn't work: there are unresolved problems in some libraries
//scalaVersion := "2.12.0"

resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"
resolvers += "Pellucid Bintray" at "http://dl.bintray.com/pellucid/maven"
resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
resolvers ++= Seq(
  Resolver.sonatypeRepo("snapshots")
)

//            "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
//"Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"


libraryDependencies ++= Seq(
    "org.scalaz.stream" %% "scalaz-stream" % "0.6a",

    // YAML
    // getting error here, therefore see lib/snakeyaml-1.9.jar
    //"org.yaml" %% "snakeyaml" % snameyamlVersion,

    //"org.mongodb" %% "casbah" % "3.1.1",
    // cassandra
    "com.datastax.cassandra" % "cassandra-driver-core" % cassandraVersion,
    "io.argonaut" %% "argonaut" % argonautVersion,

    // JSON
    "net.liftweb" %% "lift-json" % "2.6-M4",

    // JSON
    //"com.typesafe.play" %% "play-json" % playJson,
    "org.scala-lang.modules" %% "scala-parser-combinators" % jsonVersion,

    // getting error here, therefore see lib/config-1.3.1.jar
    // typesafe config used in tests
    // "com.typesafe" %% "config" % typesafeConfigVersion,

    // scala wrapper for java.time
    "codes.reactive" %% "scala-time" % scalaTimeVersion,

    // saddle - not used for now
    // "org.scala-saddle" %% "saddle-core" % saddleVersion,
    //"com.chrisomeara" % "pillar_2.11" % "2.0.1",

    // quill disabled for now
    //"io.getquill" %% "quill-async" % quillAsyncVersion,
    // getting error here, therefore may need to manually download to lib/quill-cassandra_2.11-1.0.1-SNAPSHOT.jar
    //"io.getquill" %% "quill-cassandra" % quillCassVersion,



    // framian
    //"com.pellucid" %% "framian" % "0.3.1",

    // csv support
    "com.github.tototoshi" %% "scala-csv" % "1.1.2",
    // light weight dataframe
    "com.github.martincooper" %% "scala-datatable" % "0.7.0",

// ioda not used for now
    //"joda-time" % "joda-time" % "2.3",
    //"org.joda" % "joda-convert" % "1.5",

    "org.scalatest" %% "scalatest" % scalatestVersion % "test"
)

//lazy val testDependencies = Seq (
//  "org.scalatest" %% "scalatest" % "2.2.0" % "test"
//)

