package com.treasuredata.flow.lang.compiler

import com.treasuredata.flow.lang.compiler.SourceFile.NoSourceFile
import com.treasuredata.flow.lang.model.NodeLocation
import com.treasuredata.flow.lang.model.plan.LogicalPlan
import wvlet.airframe.ulid.ULID
import wvlet.log.io.IOUtil

/**
  * Represents a unit for compilation (= source file) and records intermediate data (e.g., plan trees) for the source
  * file
  * @param sourceFile
  */
case class CompilationUnit(sourceFile: SourceFile):
  // Untyped plan tree
  var unresolvedPlan: LogicalPlan = LogicalPlan.empty
  // Fully-typed plan tree
  var resolvedPlan: LogicalPlan = LogicalPlan.empty

  // Plans generated for subscriptions
  var subscriptionPlan: LogicalPlan = LogicalPlan.empty

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
  def fromFile(file: String): SourceFile      = SourceFile(file, IOUtil.readAsString)
  def fromString(content: String): SourceFile = SourceFile(s"${ULID.newULIDString}.flow", _ => content)

class SourceFile(val file: String, readContent: (file: String) => String):
  def toCompileUnit: CompilationUnit = CompilationUnit(this)
  lazy val content: String           = readContent(file)
