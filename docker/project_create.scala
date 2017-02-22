//package iotus.cli

import iotus.tools.ProjectBuilder

/**
  * Build simple project
*/
object Executor {
  val usage =
    """usage: project_create project_name timezone zincFile [skipProjectCreate]
      |e.g.:
      |project_create simple America/Log_Angeles ./src/main/resources/iotus-simple.zinc
      |""".stripMargin
  def main(args: Array[String]): Unit = {
    var ret = 1
    if (args.length < 3 || args.length > 4 ) {
      print(usage)

    } else {
      val (pid, tz, zincFile) = (args(0), args(1), args(2))
      val skipProjectCreate: Boolean = if (args.length == 4) {
        args (3).equals ("1") || args (3).equals ("true")
      } else {
        false
      }

      println(s"Creating project pid, tz, zincFile: $pid, $tz, $zincFile, skipProjectCreate=$skipProjectCreate")

      ProjectBuilder.createProject(pid, tz, zincFile, skipProjectCreate = skipProjectCreate) match {
        case Right(u) => println("Success")
          ret = 1
        case Left(s) => println(s"Problem executing createProject $s")
      }
    }
    // need to call exit, otherwise script hangs
    System.exit(ret)
  }
}


