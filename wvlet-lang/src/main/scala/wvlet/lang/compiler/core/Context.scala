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

import wvlet.log.LogSupport

/**
  * Context conveys the current state of the compilation, including defined types, table definitions, and the current
  * compilation unit.
  *
  * Context and Scope are mutable, and the compiler will update them as it processes the source code.
  */
case class Context(
    compileUnit: CompilationUnit = CompilationUnit.empty,
    phase: Phase = Phase.NoPhase,
    owner: Symbol = Symbol.NoSymbol,
    scope: Scope = Scope(),
    sourceFolders: List[String] = List.empty
) extends LogSupport:

  def nestingLevel: Int = scope.nestingLevel

  def withPhase(newPhase: Phase): Context =
    this.copy(phase = newPhase)
  
  def withCompilationUnit[U](newCompileUnit: CompilationUnit): Context =
    this.copy(compileUnit = newCompileUnit)

  def withOwner(newOwner: Symbol): Context =
    this.copy(owner = newOwner)
