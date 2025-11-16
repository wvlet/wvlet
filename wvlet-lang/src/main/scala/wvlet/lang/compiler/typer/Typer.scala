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
package wvlet.lang.compiler.typer

import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.Phase
import wvlet.lang.model.plan.LogicalPlan
import wvlet.lang.model.Type
import wvlet.lang.model.Type.NoType
import wvlet.log.LogSupport

/**
  * New typer implementation using bottom-up traversal and tpe field. This is designed to replace
  * the current TypeResolver with a more efficient single-pass approach.
  *
  * Currently, this is a stub that delegates to the existing TypeResolver for gradual migration.
  */
object Typer extends Phase("typer") with LogSupport:

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    trace(s"Running new typer on ${unit.sourceFile.fileName}")

    // Type the plan bottom-up
    given typerCtx: TyperContext = TyperContext.from(context, unit)
    val typed                    = typePlan(unit.unresolvedPlan)

    // TODO: Report errors collected in TyperContext
    if typerCtx.hasErrors then
      warn(s"Typing errors in ${unit.sourceFile.fileName}:")
      typerCtx
        .errors
        .reverse
        .foreach { err =>
          warn(s"  ${err.message} at ${err.sourceLocation(using context)}")
        }

    // For now, just return the unit unchanged
    // TODO: Store typed plan in CompilationUnit once we have a field for it
    unit

  /**
    * Main typing entry point - will type a plan bottom-up
    */
  private def typePlan(plan: LogicalPlan)(using ctx: TyperContext): LogicalPlan =
    // Bottom-up: type children first, then type the node itself
    val withTypedChildren = plan.mapChildren(typePlan)

    // Type current node
    typeNode(withTypedChildren)

  /**
    * Type a single node using composable rules
    */
  private def typeNode(plan: LogicalPlan)(using ctx: TyperContext): LogicalPlan =
    // Apply typing rules
    val typed = TyperRules.allRules.applyOrElse(plan, identity[LogicalPlan])

    // Ensure type is set
    if typed.tpe == NoType then
      typed.tpe = inferType(typed)

    typed

  /**
    * Fallback type inference for nodes not covered by rules
    */
  private def inferType(plan: LogicalPlan)(using ctx: TyperContext): Type =
    // For now, return NoType
    // This will be expanded as we implement specific typing logic
    NoType

end Typer
