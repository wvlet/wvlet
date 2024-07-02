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
package wvlet.lang.compiler.core

import wvlet.airframe.ulid.ULID
import wvlet.lang.compiler.ast.Tree
import wvlet.log.io.IOUtil

import java.io.File

/**
  * Represents a unit for compilation (= source file) and records intermediate data (e.g., plan trees) for the source
  * file
  *
  * @param sourceFile
  */
case class CompilationUnit(sourceFile: SourceFile):
  // Untyped plan tree
  var unresolvedPlan: Tree = Tree.EmptyTree
  // Fully-typed plan tree
  var resolvedPlan: Tree = Tree.EmptyTree

  // Plans generated for subscriptions
  var subscriptionPlans: List[Tree] = List.empty[Tree]

object CompilationUnit:
  val empty: CompilationUnit = CompilationUnit(SourceFile.NoSourceFile)

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
    else if f.isFile && f.getName.endsWith(SourceFile.fileExt) then Seq(f.getPath)
    else Seq.empty

