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
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.model.expr.Expression
import wvlet.lang.model.plan.*
import wvlet.lang.model.Type
import wvlet.lang.model.Type.NoType
import wvlet.lang.model.Type.FunctionType
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

    // Store the typed plan in CompilationUnit
    unit.resolvedPlan = typed
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
        val typedPackage = p.copy(statements = typedStatements)
        TyperRules.typeStatement(typedPackage)
        typedPackage
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
        val typedTypeDef = t.copy(elems = typedElems)
        TyperRules.typeStatement(typedTypeDef)
        typedTypeDef
      // Handle ModelDef - enters model scope with parameters
      case m: ModelDef =>
        val modelCtx = ctx.newContext(m.symbol)
        // Register parameters in input type
        val paramTypes = m.params.map(p => NamedType(p.name, p.dataType))
        val inputType  = SchemaType(
          parent = None,
          typeName = Name.typeName(s"${m.name.name}_params"),
          columnTypes = paramTypes
        )
        val bodyCtx       = modelCtx.withInputType(inputType)
        val typedChild    = typePlan(m.child)(using bodyCtx)
        val typedModelDef =
          typedChild match
            case q: Query =>
              m.copy(child = q)
            case _ =>
              // Should not happen for well-formed ModelDef
              m
        TyperRules.typeStatement(typedModelDef)
        typedModelDef
      // Handle Relation - propagate input type through relational operators
      // Returns potentially transformed relation (e.g., TableRef -> TableScan)
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
        val argTypes      = f.args.map(arg => NamedType(arg.name, arg.dataType))
        val inputWithArgs =
          ctx.inputType match
            case st: SchemaType =>
              st.copy(columnTypes = st.columnTypes ++ argTypes)
            case _ =>
              SchemaType(parent = None, typeName = Name.NoTypeName, columnTypes = argTypes)
        val funcCtx   = ctx.withInputType(inputWithArgs)
        val typedExpr = f.expr.map(e => typeExpression(e)(using funcCtx))
        val typedFunc = f.copy(expr = typedExpr)
        // Set FunctionType
        val retType = f.retType.getOrElse(typedExpr.map(_.dataType).getOrElse(DataType.UnknownType))
        val contextNames = f.defContexts.flatMap(_.name.map(n => Name.termName(n.leafName)))
        typedFunc.tpe = Type.FunctionType(f.name, argTypes, retType, contextNames)
        typedFunc
      case fd: FieldDef =>
        // Type field body if present
        val typedBody  = fd.body.map(e => typeExpression(e)(using ctx))
        val typedField = fd.copy(body = typedBody)
        // Set field type from symbol
        typedField.tpe = fd.symbol.dataType
        typedField

  /**
    * Type a relation with input type propagation. Uses in-place traversal for efficiency - only
    * updates mutable tpe fields without creating new tree instances.
    *
    * For table references (TableRef, FileRef, TableFunctionCall), applies tableRefRules to resolve
    * them to concrete scans (TableScan, FileScan, ModelScan) via type definition lookup.
    */
  private def typeRelation(r: Relation)(using ctx: Context): Relation =
    // First transform and type children (bottom-up)
    // Use mapChildren to properly replace child relations that get transformed
    val withTypedChildren = r.mapChildren { child =>
      child match
        case childRel: Relation =>
          typeRelation(childRel)
        case other =>
          typePlan(other)
    }

    // Work with the tree that has typed children
    val relation =
      withTypedChildren match
        case rel: Relation =>
          rel
        case other =>
          r // Shouldn't happen, but fallback to original

    // Get input type from children (now typed)
    val inputType = relation.inputRelationType

    // Type direct child expressions with the input type context - in place
    val exprCtx = ctx.withInputType(inputType)
    relation
      .childExpressions
      .foreach { expr =>
        typeExpression(expr)(using exprCtx)
      }

    // Apply relation rules (includes tableRefRules for resolving table references)
    // This handles TableRef -> TableScan, FileRef -> FileScan, etc.
    val resolved = TyperRules.relationRules.applyOrElse(relation, identity[Relation])
    resolved

  end typeRelation

  /**
    * Type an expression using TyperRules. Uses in-place traversal for efficiency - only updates
    * mutable tpe fields without creating new tree instances.
    */
  private def typeExpression(expr: Expression)(using ctx: Context): Expression =
    // Bottom-up: type children first - in place
    expr.children.foreach(typeExpression)

    // Apply expression rules - modifies tpe field in place, returns same instance
    TyperRules.exprRules.applyOrElse(expr, identity[Expression])

  /**
    * Type a single node (fallback for nodes not handled by specific typing methods)
    */
  private def typeNode(plan: LogicalPlan)(using ctx: Context): LogicalPlan =
    // Ensure type is set
    if plan.tpe == NoType then
      plan.tpe = inferType(plan)
    plan

  /**
    * Fallback type inference for nodes not covered by rules
    */
  private def inferType(plan: LogicalPlan)(using ctx: Context): Type =
    // For now, return NoType
    // This will be expanded as we implement specific typing logic
    NoType

end Typer
