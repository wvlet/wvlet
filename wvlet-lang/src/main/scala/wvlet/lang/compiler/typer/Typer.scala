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
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.Phase
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.model.expr.Expression
import wvlet.lang.model.plan.*
import wvlet.lang.model.Type
import wvlet.lang.model.Type.NoType
import wvlet.log.LogSupport

/**
  * New typer implementation using bottom-up traversal and tpe field. This is designed to replace
  * the current TypeResolver with a more efficient single-pass approach.
  *
  * Context carries TyperState for typing-specific state (following Scala 3 pattern).
  *
  * The typer operates in two phases:
  *   1. Pre-scan: Register symbols (TypeDef, ModelDef) before typing so they can be referenced
  *   2. Type: Bottom-up traversal with context-aware scope management
  */
object Typer extends Phase("typer") with LogSupport:

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    trace(s"Running new typer on ${unit.sourceFile.fileName}")

    // Phase 1: Pre-scan to register symbols
    val preScanCtx = preScan(unit.unresolvedPlan)(using context)

    // Phase 2: Type the plan bottom-up with context-aware scope management
    val typed = typePlan(unit.unresolvedPlan)(using preScanCtx)

    // Report any typing errors
    if preScanCtx.hasTyperErrors then
      warn(s"Typing errors in ${unit.sourceFile.fileName}:")
      preScanCtx
        .typerErrors
        .foreach { err =>
          warn(s"  ${err.message} at ${err.sourceLocation(using context)}")
        }

    // For now, just return the unit unchanged
    // TODO: Store typed plan in CompilationUnit once we have a field for it
    unit

  // ============================================
  // Phase 1: Pre-scanning for symbol registration
  // ============================================

  /**
    * Pre-scan the plan to register symbols before typing. This ensures that forward references can
    * be resolved during the typing phase.
    */
  private def preScan(plan: LogicalPlan)(using ctx: Context): Context =
    plan match
      case p: PackageDef =>
        // Enter package scope and pre-scan statements
        val packageCtx = ctx.newContext(p.symbol)
        p.statements.foldLeft(packageCtx)(preScanStatement)
      case r: Relation =>
        preScanRelation(r)
      case _ =>
        ctx

  /**
    * Pre-scan a statement to register its symbol
    */
  private def preScanStatement(ctx: Context, stmt: LogicalPlan): Context =
    stmt match
      case t: TypeDef =>
        ctx.enter(t.symbol)
        ctx
      case m: ModelDef =>
        ctx.enter(m.symbol)
        ctx
      case i: Import =>
        ctx.withImport(i)
      case r: Relation =>
        preScanRelation(r)(using ctx)
      case _ =>
        ctx

  /**
    * Pre-scan a relation to register table/file symbols
    */
  private def preScanRelation(r: Relation)(using ctx: Context): Context =
    r.traverse { case s: HasTableOrFileName =>
      ctx.enter(s.symbol)
    }
    ctx

  // ============================================
  // Phase 2: Context-aware typing
  // ============================================

  /**
    * Main typing entry point - types a plan with context-aware scope management
    */
  private def typePlan(plan: LogicalPlan)(using ctx: Context): LogicalPlan =
    plan match
      // Handle PackageDef - creates new scope for statements
      case p: PackageDef =>
        val packageCtx      = ctx.newContext(p.symbol)
        var currentCtx      = packageCtx
        val typedStatements = p
          .statements
          .map { stmt =>
            val typedStmt = typePlan(stmt)(using currentCtx)
            // Update context for imports
            currentCtx = updateContextForStatement(typedStmt, currentCtx)
            typedStmt
          }
        p.copy(statements = typedStatements)

      // Handle TypeDef - enters type scope for methods
      case t: TypeDef =>
        val typeCtx = ctx.newContext(t.symbol)
        // Set input type from the type's schema for field access
        val inputType  = t.symbol.dataType
        val bodyCtx    = typeCtx.withInputType(inputType)
        val typedElems = t
          .elems
          .map { elem =>
            typeTypeElem(elem)(using bodyCtx)
          }
        t.copy(elems = typedElems)

      // Handle ModelDef - enters model scope with parameters
      case m: ModelDef =>
        val modelCtx = ctx.newContext(m.symbol)
        // Register parameters in input type
        val paramTypes = m.params.map(p => NamedType(p.name, p.dataType))
        val inputType = SchemaType(
          parent = None,
          typeName = Name.typeName(s"${m.name.name}_params"),
          columnTypes = paramTypes
        )
        val bodyCtx    = modelCtx.withInputType(inputType)
        val typedChild = typePlan(m.child)(using bodyCtx)
        typedChild match
          case q: Query =>
            m.copy(child = q)
          case _ =>
            // Should not happen for well-formed ModelDef
            m

      // Handle Relation - propagate input type through relational operators
      case r: Relation =>
        typeRelation(r)

      // Default: bottom-up typing for other nodes
      case other =>
        val withTypedChildren = other.mapChildren(typePlan)
        typeNode(withTypedChildren)

  /**
    * Update context based on a typed statement (e.g., adding imports)
    */
  private def updateContextForStatement(stmt: LogicalPlan, ctx: Context): Context =
    stmt match
      case i: Import =>
        ctx.withImport(i)
      case _ =>
        ctx

  /**
    * Type a type element (method or field definition)
    */
  private def typeTypeElem(elem: TypeElem)(using ctx: Context): TypeElem =
    elem match
      case f: FunctionDef =>
        // Add function args to input type
        val argTypes = f.args.map(arg => NamedType(arg.name, arg.dataType))
        val inputWithArgs =
          ctx.inputType match
            case st: SchemaType =>
              st.copy(columnTypes = st.columnTypes ++ argTypes)
            case _ =>
              SchemaType(
                parent = None,
                typeName = Name.NoTypeName,
                columnTypes = argTypes
              )
        val funcCtx   = ctx.withInputType(inputWithArgs)
        val typedExpr = f.expr.map(e => typeExpression(e)(using funcCtx))
        f.copy(expr = typedExpr)

      case fd: FieldDef =>
        // Type field body if present
        val typedBody = fd.body.map(e => typeExpression(e)(using ctx))
        fd.copy(body = typedBody)

  /**
    * Type a relation with input type propagation
    */
  private def typeRelation(r: Relation)(using ctx: Context): Relation =
    // First type children (bottom-up)
    val withTypedChildren = r.mapChildren {
      case child: Relation =>
        typeRelation(child)
      case child: LogicalPlan =>
        typePlan(child)
    }

    // Get input type from children
    val inputType = withTypedChildren.inputRelationType

    // Type expressions with the input type context
    val exprCtx              = ctx.withInputType(inputType)
    val withTypedExpressions = withTypedChildren.transformChildExpressions { expr =>
      typeExpression(expr)(using exprCtx)
    }

    // Apply typing rules to the relation itself
    typeNode(withTypedExpressions).asInstanceOf[Relation]

  /**
    * Type an expression using TyperRules
    */
  private def typeExpression(expr: Expression)(using ctx: Context): Expression =
    // Bottom-up: type children first
    val withTypedChildren = expr.transformChildExpressions(typeExpression)

    // Apply expression rules
    TyperRules.exprRules.applyOrElse(withTypedChildren, identity[Expression])

  /**
    * Type a single node using composable rules
    */
  private def typeNode(plan: LogicalPlan)(using ctx: Context): LogicalPlan =
    // Apply typing rules
    val typed = TyperRules.allRules.applyOrElse(plan, identity[LogicalPlan])

    // Ensure type is set
    if typed.tpe == NoType then
      typed.tpe = inferType(typed)

    typed

  /**
    * Fallback type inference for nodes not covered by rules
    */
  private def inferType(plan: LogicalPlan)(using ctx: Context): Type =
    // For now, return NoType
    // This will be expanded as we implement specific typing logic
    NoType

end Typer
