package iotus.core

import org.projecthaystack.HGrid
import org.scalatest.{FunSpec, Matchers}

class BaseSpec(var verbose: Boolean=false) extends FunSpec with Matchers {
  verbose = false


  def dumpIfVerbose(s: String, grid: HGrid=null): Unit = {
    if (verbose) {
      println(s)
      if (grid != null)
        grid.dump()
    }
  }

}
