package com.treasuredata.flow.lang.compiler

import com.treasuredata.flow.lang.compiler.SourceFile.NoSourceFile
import com.treasuredata.flow.lang.model.NodeLocation
import com.treasuredata.flow.lang.model.plan.FlowPlan
import wvlet.log.io.IOUtil

case class CompilationUnit(sourceFile: SourceFile):
  var untypedPlan: FlowPlan = null
  var typedPlan: FlowPlan   = null

  def toSourceLocation(nodeLocation: Option[NodeLocation]) =
    SourceLocation(this, nodeLocation)

object CompilationUnit:
  val empty: CompilationUnit = CompilationUnit(NoSourceFile)

  def fromFile(path: String) = CompilationUnit(SourceFile.fromFile(path))

  def fromPath(path: String): List[CompilationUnit] =
    // List all *.flow files under the path
    val files = listFiles(path)
    val units = files.map { file =>
      CompilationUnit(SourceFile.fromFile(file))
    }.toList
    units

  private def listFiles(path: String): Seq[String] =
    val f = new java.io.File(path)
    if f.isDirectory then
      f.listFiles().flatMap { file =>
        listFiles(file.getPath)
      }
    else if f.isFile && f.getName.endsWith(".flow") then Seq(f.getPath)
    else Seq.empty

object SourceFile:
  object NoSourceFile extends SourceFile("<empty>", _ => "")
  def fromFile(file: String): SourceFile = SourceFile(file, IOUtil.readAsString)

class SourceFile(val file: String, readContent: (file: String) => String):
  def toCompileUnit: CompilationUnit = CompilationUnit(this)
  lazy val content: String           = readContent(file)
