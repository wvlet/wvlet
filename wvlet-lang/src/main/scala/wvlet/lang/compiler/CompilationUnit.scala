/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.compiler

import wvlet.lang.api.LinePosition
import wvlet.lang.api.SourceLocation
import wvlet.lang.api.Span
import wvlet.lang.compiler
import wvlet.lang.compiler.SourceFile.NoSourceFile
import wvlet.lang.compiler.analyzer.DependencyDAG
import wvlet.lang.model.plan.ExecutionPlan
import wvlet.lang.model.plan.LogicalPlan
import wvlet.lang.model.plan.NamedRelation
import wvlet.lang.model.plan.Relation
import wvlet.lang.stdlib.StdLib
import wvlet.uni.log.LogSupport

import java.util.concurrent.ConcurrentHashMap

/**
  * Represents a unit for compilation (= source file) and records intermediate data (e.g., plan
  * trees) for the source file
  * @param sourceFile
  */
case class CompilationUnit(sourceFile: SourceFile, isPreset: Boolean = false)
    extends LogSupport
    with Ordered[CompilationUnit]:

  export sourceFile.isSQL
  export sourceFile.isWv
  export sourceFile.relativeFilePath
  export sourceFile.fileName

  // Untyped plan tree
  var unresolvedPlan: LogicalPlan = LogicalPlan.empty

  // Cached after parsing; symbol lookup reads this on a hot path
  private var cachedPackageName: String = null

  /**
    * The package this unit belongs to, or an empty string when the unit has no package declaration
    * (the shared default package)
    */
  def packageName: String =
    if cachedPackageName != null then
      cachedPackageName
    else
      unresolvedPlan match
        case p: wvlet.lang.model.plan.PackageDef =>
          val name =
            if p.name.nonEmpty then
              p.name.fullName
            else
              ""
          cachedPackageName = name
          name
        case _ =>
          // The unit is not parsed yet, so do not cache
          ""

  // Fully-typed plan tree
  var resolvedPlan: LogicalPlan        = LogicalPlan.empty
  var modelDependencies: DependencyDAG = DependencyDAG.empty
  var executionPlan: ExecutionPlan     = ExecutionPlan.empty

  var knownSymbols: List[Symbol] = List.empty

  var lastError: Option[Throwable] = None

  // Type errors collected by the Typer phase (reported as diagnostics, not compile failures)
  var typerErrors: List[wvlet.lang.compiler.typer.TyperError] = Nil

  // Plans generated for subscriptions
  var subscriptionPlans: List[LogicalPlan] = List.empty[LogicalPlan]

  private var finishedPhases: Set[String]  = Set.empty
  private var lastCompiledAt: Option[Long] = None

  def isEmpty: Boolean  = this eq CompilationUnit.empty
  def isFailed: Boolean = lastError.isDefined

  def isFinished(phase: Phase): Boolean = finishedPhases.contains(phase.name)
  def setFinished(phase: Phase): Unit   =
    finishedPhases += phase.name
    lastCompiledAt = Some(System.currentTimeMillis())

  def setFailed(e: Throwable): Unit =
    lastError = Some(e)
    lastCompiledAt = Some(System.currentTimeMillis())

  def needsRecompile: Boolean = lastCompiledAt.exists(sourceFile.lastUpdatedAt > _)

  def reload(): CompilationUnit =
    sourceFile.reload()
    finishedPhases = Set.empty
    lastError = None
    lastCompiledAt = None
    // The unit will be re-parsed, so the package declaration may change
    cachedPackageName = null
    this

  def text(span: Span): String =
    // Extract the text in the span range
    sourceFile.getContent.slice(span.start, span.end).mkString

  def enter(symbol: Symbol): Unit = knownSymbols = symbol :: knownSymbols

  def toSourceLocation(nodeLocation: LinePosition) =
    val codeLineAt: String = nodeLocation
      .map { loc =>
        val line = sourceFile.sourceLine(loc.line)
        line
      }
      .getOrElse("")

    SourceLocation(sourceFile.relativeFilePath, sourceFile.fileName, codeLineAt, nodeLocation)

  def findRelationRef(name: String): Option[LogicalPlan] =
    var result: Option[Relation] = None
    resolvedPlan.traverse {
      case r: NamedRelation if r.name.leafName == name =>
        result = Some(r)
    }
    result

  override def compare(that: CompilationUnit): Int = sourceFile.file.compare(that.sourceFile.file)

end CompilationUnit

class CompilationUnitCache:
  import scala.jdk.CollectionConverters.*
  private var cache = ConcurrentHashMap[String, CompilationUnit]().asScala

  def getOrElseUpdate(path: String, factory: => CompilationUnit): CompilationUnit = cache
    .getOrElseUpdate(path, factory)

end CompilationUnitCache

object CompilationUnit extends LogSupport:
  val empty: CompilationUnit = CompilationUnit(NoSourceFile)

  def fromWvletString(text: String) = CompilationUnit(SourceFile.fromWvletString(text))
  def fromSqlString(text: String)   = CompilationUnit(SourceFile.fromSqlString(text))

  def fromFile(path: String) = CompilationUnit(SourceFile.fromFile(path))

  def fromPath(
      path: String,
      cache: CompilationUnitCache = CompilationUnitCache()
  ): List[CompilationUnit] =
    // List all *.wv and .sql files under the path
    val files = SourceIO.listSourceFiles(path)
    val units =
      files
        .map { file =>
          cache.getOrElseUpdate(
            file.path,
            CompilationUnit(SourceFile.fromFile(file), isPreset = false)
          )
        }
        .toList
    units.sorted

  def stdLib: List[CompilationUnit] =
    StdLib
      .allFiles
      .map { case (file, content) =>
        CompilationUnit(SourceFile.fromString(file, content), isPreset = true)
      }
      .toList

end CompilationUnit
