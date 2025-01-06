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

import wvlet.lang.api.{LinePosition, SourceLocation, Span}
import wvlet.lang.compiler
import wvlet.lang.compiler.SourceFile.NoSourceFile
import wvlet.lang.compiler.analyzer.DependencyDAG
import wvlet.lang.model.plan.{ExecutionPlan, LogicalPlan, NamedRelation, Relation}
import wvlet.lang.stdlib.StdLib
import wvlet.log.LogSupport
import wvlet.log.io.{IOUtil, Resource}

import java.io.File
import java.net.URLClassLoader
import java.net.{URI, URL}
import java.util.jar.JarFile

/**
  * Represents a unit for compilation (= source file) and records intermediate data (e.g., plan
  * trees) for the source file
  * @param sourceFile
  */
case class CompilationUnit(sourceFile: SourceFile, isPreset: Boolean = false) extends LogSupport:
  // Untyped plan tree
  var unresolvedPlan: LogicalPlan = LogicalPlan.empty
  // Fully-typed plan tree
  var resolvedPlan: LogicalPlan        = LogicalPlan.empty
  var modelDependencies: DependencyDAG = DependencyDAG.empty
  var executionPlan: ExecutionPlan     = ExecutionPlan.empty

  var knownSymbols: List[Symbol] = List.empty

  var lastError: Option[Throwable] = None

  // Plans generated for subscriptions
  var subscriptionPlans: List[LogicalPlan] = List.empty[LogicalPlan]

  private var finishedPhases: Set[String]  = Set.empty
  private var lastCompiledAt: Option[Long] = None

  def isEmpty: Boolean  = this eq CompilationUnit.empty
  def isFailed: Boolean = lastError.isDefined

  def isFinished(phase: Phase): Boolean = finishedPhases.contains(phase.name)
  def setFinished(phase: Phase): Unit =
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

end CompilationUnit

object CompilationUnit extends LogSupport:
  val empty: CompilationUnit = CompilationUnit(NoSourceFile)

  def fromWvletString(text: String) = CompilationUnit(SourceFile.fromWvletString(text))
  def fromSqlString(text: String)   = CompilationUnit(SourceFile.fromSqlString(text))

  def fromFile(path: String) = CompilationUnit(SourceFile.fromFile(path))

  def fromPath(path: String): List[CompilationUnit] =
    // List all *.wv and .sql files under the path
    val files = SourceIO.listSourceFiles(path)
    val units =
      files
        .map { file =>
          CompilationUnit(SourceFile.fromFile(file), isPreset = false)
        }
        .toList
    units

  def fromResourcePath(path: String, isPreset: Boolean): List[CompilationUnit] =
    val resources = SourceIO.listResources(path)
    resources
      .filter(_.isSourceFile)
      .map { r =>
        CompilationUnit(SourceFile.fromFile(r), isPreset = isPreset)
      }

  def stdLib: List[CompilationUnit] =
    StdLib
      .allFiles
      .map { case (file, content) =>
        CompilationUnit(SourceFile.fromString(file, content), isPreset = true)
      }
      .toList

end CompilationUnit
