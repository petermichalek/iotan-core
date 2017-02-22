package iotus.tools

import org.projecthaystack._
import iotus.core.model.Project
import iotus.core._
import org.projecthaystack.io.HZincReader

import scala.collection.mutable
import scala.util.Left


/**
  * Utility object to create new projects from imported zinc files.
  */
object ProjectBuilder {

  val config: YamlConfig = Iotus.getConfig()
  val session: StoreSession = StoreSession.make(config.getString("db"))
  val projectRepo = new ProjectRepository("project", session)
  val adminUser = "admin@admin.org"

  val ORIGINAL_ID_NAME: String = "origId"
  var verbose = true

  /*
def modMap(postVals: Map[String, String]): Map[String, String] = {
   postVals map {case(k, v) => if(v == "a") (k -> "other value") else (k ->v)}


}
   */

  /**
    * Change references in a row/map, returning modified hrefs
    * @param row
    * @return
    */
  private def tweakRefs(row: Map[String, HVal], idMap: Map[String, HVal]): Map[String, HVal] = {
    // Map[String, Option[HVal]]
    var remapCount = 0
    row map { case(k, v) =>
        //println(s"k,v=$k,$v")
        if (k.endsWith("Ref")) {
          v match {
            case v: HRef =>
              val newRef = idMap.getOrElse(v.toString, null)
              if (newRef == null) {
                //println(s"Can't remap $k: $v since $v is not in id map in %s (%s)".format(row.get("id"), row.get(ORIGINAL_ID_NAME)))
                (k, v)
              } else {
                //println(s"Remapped $k -> $v to k -> $newRef")
                remapCount += 1
                k -> newRef
              }
            case _ =>
              println(s"failed for k,v=$k,$v which should be HRef type, but is ${v.getClass} ... skipping mapping")
              (k, v)
          }
        } else {
          (k, v)
        }
    }
  }


  /**
    * Resolves broken hrefs in grid, which result from
    * renaming ids when a project is duplicated/restored on
    * a different system.
    *
    * @param grid
    * @return Either error string or resulting transformed HGrid
    */
  def resolveHRefs(grid: GridWrap): Either[String, GridWrap] = {

    // create id map
    val idMap: Map[String, HVal] = grid.srows.map(
      x => x.get(ORIGINAL_ID_NAME).get.toString -> x.get("id").get).toMap
    //val idMap1: Map[String, HVal] = grid.srows.transform
      //(k, v) => k.get("origId").toString -> x.get("id").get) toMap
/*
    if (idMap.keys.toList.contains("RoscoeHighSchool")) {
      println("test passed")
    } else
      println("test failed")
*/
    var newrows: mutable.ListBuffer[Map[String, HVal]] = mutable.ListBuffer() // grid.srows.toArray:_*)
    //println(s"idMap: $idMap")

    // resolve hrefs
    for (row: Map[String, HVal] <- grid.srows) {
      newrows.append(tweakRefs(row, idMap))
    }

    // recreate grid from rows/cols
    var newwgrid: GridWrap = GridWrap.fromRows(grid.meta, grid.colNames, newrows.toList)
    Right(newwgrid)
  }

  /**
    * Tranfsform grid:
    *
    * @param wgrid
    * @return
    */
  def transformGrid(wgrid: GridWrap): Either[String, GridWrap] = {
      //wgrid.filterCols(Set("id", "oldId", "origId", "cxId", "equipRef", "siteRef", "bldgRef")).toGrid.dump
      //println(wgrid.srows.take(10))
      wgrid.renameCols(Map("id" -> ORIGINAL_ID_NAME), true) match {
        case Right(g) =>
          //println("g:")
          //g.filterCols(Set("id", "oldId", "origId", "cxId", "equipRef", "siteRef", "bldgRef")).toGrid.dump
          // fillup ids
          //g.generateIds()
          resolveHRefs(g)
        // left is already implicit?
        case Left(s) => Left(s)
      }

  }


  /**
    * Builds a small project with a specific
    * @param pid project short name/id
    * @param tz project timezone
    */
  def createProject(pid: String, tz: String, grid: HGrid, skipProjectCreate: Boolean): Either[String, Unit] = {
    // check validity of grid
    // val newgrid = transformGrid(grid)
    val newgrid: GridWrap = {
      transformGrid(GridWrap(grid)) match {
        case Left(s) =>
          return Left(s)
        case Right(g) =>
          //println("transformed:")
          //println(g.srows.take(10))
          g.filterCols(Set("id", "oldId", "origId", "cxId", "equipRef", "siteRef", "bldgRef")).toGrid.dump
          g
        case _ =>
          return Left("Internal error processing transformGrid results")
      }
    }

    println(s"Loaded grid: ${newgrid.numRows} rows")

    if (!skipProjectCreate) {
      val p = Project(pid, tz)
      val existingProject: Project = projectRepo.readByProp("pid", pid).getOrElse(null)
      if (existingProject != null) {
        return Left(s"Project $pid already exists")
      } else {
        projectRepo.save(p)
      }
    }
    try {
      val ctx = new IMContext(pid, adminUser)
      val frame = IMFrame(newgrid.toGrid, false)
      frame.toGrid.dump()
      ctx.commit(frame, "add")
      val resultGrid: HGrid = ctx.toGrid
      if (resultGrid.isEmpty) {
        Left(s"Error performing commit: ${resultGrid.meta.toString}")
      } else {
        Right()
      }
    } catch {
      case e:Exception => Left(s"Error creating context: ${e.toString}")
    }
  }

  /**
    * Build a demo project using given zincFile.
    * Zinc file used will be replicated on the new project with the following modifications:
    * - id column will be renamed to origId column. If origId column already existed in the original zinc file,
    *  it will be overwritten
    * - siteRefs, equipRefs and other *Refs will be changed so that they reflect the desired point/equi/site etc.
    *  hierarchy.
    *  (In addition to siteRef, equipRef some project may have fileds name bldgRef etc. all of those will be considered
    *   as long as they end with "Ref" and are of type HRef.)
    *
    * @param pid
    * @param tz
    * @param zincFile the zincFile may be based on exports from another project
    *                 It should contain all the nodes intended for the new project, such as
    *                 site, equip and point nodes
    * @return
    */
  def createProject(pid: String, tz: String, zincFile: String, skipProjectCreate: Boolean = false, verbose: Boolean = true): Either[String, Unit] = {
    val source = scala.io.Source.fromFile(zincFile)
    try {
      val data = source.mkString
      val grid = new HZincReader(data).readGrid()
      //grid.dump()
      createProject(pid, tz, grid, skipProjectCreate)
    } finally {
      source.close()
    }
  }

}
