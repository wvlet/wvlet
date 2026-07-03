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

import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.Phase
import wvlet.lang.compiler.analyzer.AggregationResolver
import wvlet.lang.compiler.analyzer.FunctionInliner
import wvlet.lang.compiler.analyzer.RelationRefResolver
import wvlet.lang.model.expr.ContextInputRef
import wvlet.lang.model.expr.DotRef
import wvlet.lang.model.expr.FunctionApply
import wvlet.lang.model.expr.Identifier
import wvlet.lang.model.expr.InRelation
import wvlet.lang.model.expr.NotInRelation
import wvlet.lang.model.expr.SubQueryExpression
import wvlet.lang.model.expr.TupleInRelation
import wvlet.lang.model.expr.TupleNotInRelation
import wvlet.lang.model.DataType
import wvlet.lang.model.RelationType
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.model.expr.Expression
import wvlet.lang.model.plan.*
import wvlet.lang.model.Type
import wvlet.lang.model.Type.NoType
import wvlet.lang.model.Type.FunctionType
import wvlet.uni.log.LogSupport

/**
  * The type resolution phase, using bottom-up traversal and in-place tpe assignment.
  *
  * Context carries TyperState for typing-specific state (following Scala 3 pattern).
  *
  * The typer operates in two phases:
  *   1. Pre-scan: Register symbols (TypeDef, ModelDef) before typing so they can be referenced
  *   2. Type: Bottom-up traversal with context-aware scope management
  */
object Typer extends Phase("typer") with LogSupport:

  /**
    * The maximum number of function-resolution rounds per relation. Each round resolves at least
    * one link of a chained method call, so chains longer than this are exceedingly unlikely
    */
  private inline val maxFunctionResolutionRounds = 10

  /**
    * Returns true if the expression already carries a resolved data type in either the dataType or
    * tpe field. An ErrorType in tpe does not count, so a later pass may still resolve it
    */
  private def hasResolvedType(e: Expression): Boolean =
    e.dataType.isResolved || (
      e.tpe match
        case dt: DataType if dt.isResolved =>
          true
        case _ =>
          false
    )

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
            // Point the symbol to the typed tree so later references (e.g. ModelScan) see it
            typedStmt match
              case m: ModelDef =>
                m.symbol.tree = m
              case t: TypeDef =>
                t.symbol.tree = t
              case _ =>
            // Re-point relation alias symbols (select as ...) to the rewritten child relation so
            // later references expand to the resolved query
            typedStmt.traverse { case s: SelectAsAlias =>
              s.symbol.tree = s.child
            }
            typedStmt
          }
        val typedPackage = p.copy(statements = typedStatements)
        // A case-class copy does not carry over the mutable symbol/comment fields
        typedPackage.copyMetadataFrom(p)
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
        // A case-class copy does not carry over the mutable symbol/comment fields
        typedTypeDef.copyMetadataFrom(t)
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
              val copied = m.copy(child = q)
              // A case-class copy does not carry over the mutable symbol/comment fields
              copied.copyMetadataFrom(m)
              copied
            case _ =>
              // Should not happen for well-formed ModelDef
              m
        TyperRules.typeStatement(typedModelDef)
        typedModelDef
      // Handle FlowDef - resolve stage bodies with previously-defined stages in scope
      case f: FlowDef =>
        resolveFlowDef(f)
      // Handle Relation - propagate input type through relational operators
      case r: Relation =>
        resolveRelation(r)
      // Handle ValDef - resolve native function references in the bound expression so it can be
      // evaluated once at execution time (e.g. val id = ulid_string)
      case v: ValDef =>
        val newExpr = v
          .expr
          .transformUpExpression {
            case id: Identifier if id.unresolved && id.nonEmpty && !hasResolvedType(id) =>
              FunctionInliner.findNativeFunction(ctx, id.fullName).getOrElse(id)
          }
        val typedValDef =
          if newExpr eq v.expr then
            v
          else
            val copied = v.copy(expr = newExpr)
            // A case-class copy does not carry over the mutable symbol/comment fields
            copied.copyMetadataFrom(v)
            copied
        TyperRules.typeStatement(typedValDef)
        typedValDef
      // Default: bottom-up typing for other nodes
      case other =>
        val withTypedChildren = other.mapChildren(typePlan)
        typeNode(withTypedChildren)

  private def hasUnresolvedUnderscore(r: Relation): Boolean =
    var found = false
    r.childExpressions
      .foreach { e =>
        e.traverseExpressions {
          case c: ContextInputRef if !c.dataType.isResolved =>
            found = true
        }
      }
    found

  /**
    * Resolve a relation tree: table/model/file references, partial-query and function inlining,
    * aggregation expressions, and in-place typing. Also used by GenSQL to re-resolve model bodies
    * after model expansion
    */
  def resolveRelation(r: Relation)(using ctx: Context): Relation =
    // Resolve structural references (tables, models, files, partial queries, underscores) and
    // type each node's expressions in a single bottom-up pass. Interleaving resolution and
    // typing matters: several relation nodes memoize their relationType on first access, so a
    // parent must not read a child's relation type before the child's expressions are typed
    val prepared = prepareRelation(r)
    // Inline function calls and aggregation expressions only when candidates are present.
    // Chained method calls (e.g. x.to_double.round(1)) resolve one link per round, so iterate
    // to a fixpoint
    var current = prepared
    if needsFunctionResolution(current) then
      var continue = true
      var rounds   = 0
      while continue && rounds < maxFunctionResolutionRounds do
        rounds += 1
        // Inline function applications, bare member references, and native function
        // references in a single bottom-up pass. Only no-arg methods are inlined from a bare
        // DotRef: a DotRef with declared arguments is the base of an enclosing FunctionApply
        // that must bind them first (possibly in a later round)
        val inlineRule: PartialFunction[Expression, Expression] =
          case f: FunctionApply =>
            FunctionInliner.resolveFunctionApply(f)
          case d: DotRef =>
            FunctionInliner.findFunctionDef(d) match
              case Some(m) if m.ft.args.isEmpty =>
                FunctionInliner.inlineFunctionBody(d, m, Nil)
              case _ =>
                d
          case id: Identifier if id.unresolved && id.nonEmpty && !hasResolvedType(id) =>
            // Replace references to native (compile-time evaluated) functions
            FunctionInliner.findNativeFunction(ctx, id.fullName).getOrElse(id)
        val fnInlined = current
          .transformUp {
            // Test expressions form an assertion DSL evaluated by the test runner, so keep
            // them as written (the aggregation resolver skips ShouldExpr for the same reason)
            case t: TestRelation =>
              t
            case r: Relation =>
              r.transformChildExpressions { case e: Expression =>
                e.transformUpExpression(inlineRule)
              }
          }
          .asInstanceOf[Relation]
        // Inline aggregation functions applied via dot syntax (e.g. expr.sum) and expand
        // grouping-key indexes (_1, _2, ...) in select clauses
        val aggResolved = AggregationResolver.resolveAggregations(fnInlined)
        if aggResolved eq current then
          continue = false
        else
          // Re-type so the inlined bodies get tpe assigned for the next round
          typeRelation(aggResolved)
          current = aggResolved
      end while
    end if
    current

  end resolveRelation

  /**
    * A single bottom-up pass that resolves structural references and types each relation node's own
    * expressions. Children are processed before their parents (including plans nested inside
    * expressions), so a node's input relation type is fully typed by the time it is read
    */
  private def prepareRelation(r: Relation)(using ctx: Context): Relation = prepare(r, Map.empty)
    .asInstanceOf[Relation]

  /**
    * Recursive worker for prepareRelation. The scope argument carries locally-defined relation
    * names (CTE aliases from `with ... as`, flow stage names) that are visible only within the
    * enclosing query, so references to them are typed in place without symbol registration
    */
  private def prepare(plan: LogicalPlan, scope: Map[String, RelationType])(using
      ctx: Context
  ): LogicalPlan =
    plan match
      case w: WithQuery =>
        // Each CTE definition sees the aliases defined before it; the query body sees them all
        var cteScope = scope
        var changed  = false
        val newDefs  = w
          .queryDefs
          .map { d =>
            val resolved = prepare(d, cteScope).asInstanceOf[AliasedRelation]
            if !(resolved eq d) then
              changed = true
            cteScope += resolved.alias.fullName -> resolved.relationType
            resolved
          }
        val newBody      = prepare(w.queryBody, cteScope).asInstanceOf[Relation]
        val resolvedWith =
          if changed || !(newBody eq w.queryBody) then
            val neww = w.copy(queryDefs = newDefs, queryBody = newBody)
            neww.copyMetadataFrom(w)
            neww
          else
            w
        typeRelationNode(resolvedWith)
        resolvedWith
      case ref: TableRef if scope.contains(ref.name.fullName) =>
        // A reference to a locally-defined relation (CTE alias or flow stage). The node stays a
        // TableRef so that generated SQL keeps referencing the alias; only its type is assigned
        ref.tpe = scope(ref.name.fullName)
        typeRelationNode(ref)
        ref
      case other =>
        val withChildren = other.mapChildren(c => prepare(c, scope))
        val resolved     = structuralResolutionRule.applyOrElse(withChildren, identity[LogicalPlan])
        resolved match
          case rel: Relation =>
            typeRelationNode(rel)
            rel
          case o =>
            o

  /**
    * Resolve and type the body of each flow stage, making previously-defined stage names resolvable
    * from subsequent stages (e.g. `stage output = from entry | ...`)
    */
  private def resolveFlowDef(f: FlowDef)(using ctx: Context): FlowDef =
    var stageScope = Map.empty[String, RelationType]
    var changed    = false
    val newStages  = f
      .stages
      .map { s =>
        val newBody     = s.body.map(b => prepare(b, stageScope).asInstanceOf[Relation])
        val bodyChanged =
          (newBody, s.body) match
            case (Some(a), Some(b)) =>
              !(a eq b)
            case _ =>
              false
        val newStage =
          if !bodyChanged then
            s
          else
            changed = true
            val ns = s.copy(body = newBody)
            ns.copyMetadataFrom(s)
            ns
        stageScope += newStage.name.name -> newStage.relationType
        newStage
      }
    if changed then
      val nf = f.copy(stages = newStages)
      nf.copyMetadataFrom(f)
      nf
    else
      f

  end resolveFlowDef

  /**
    * Type the direct child expressions of a single relation node (the node's children are expected
    * to be typed already) and assign its own tpe
    */
  private def typeRelationNode(rel: Relation)(using ctx: Context): Unit =
    val exprCtx = ctx.withInputType(rel.inputRelationType)
    rel match
      case t: TestRelation =>
        // Test expressions are an assertion DSL over the tested query's result
        typeTestExpression(t.testExpr)(using exprCtx)
      case _ =>
        rel
          .childExpressions
          .foreach { expr =>
            typeExpression(expr)(using exprCtx)
          }
    rel.tpe = rel.relationType
    // Type name expressions of aliases and local references as the relation type they denote
    rel match
      case a: AliasedRelation if a.relationType.isResolved =>
        a.alias.tpe = a.relationType
      case n: NamedRelation if n.relationType.isResolved =>
        n.name.tpe = n.relationType
      case t: TableRef if t.relationType.isResolved =>
        t.name.tpe = t.relationType
      case _ =>
        ()

  /**
    * A single bottom-up rewrite that resolves table/model/file references into concrete scan nodes,
    * inlines partial query applications (with cycle detection), and resolves underscore (_)
    * references from the input relation of the enclosing operator
    */
  private def structuralResolutionRule(using
      ctx: Context
  ): PartialFunction[LogicalPlan, LogicalPlan] =
    case ref: TableRef if !ref.relationType.isResolved =>
      RelationRefResolver.resolveTableRef(ref)
    case ref: TableFunctionCall if !ref.relationType.isResolved =>
      RelationRefResolver.resolveTableFunctionCall(ref)
    case m: ModelScan if !m.resolved =>
      RelationRefResolver.resolveModelScan(m)
    case f: FileRef if f.filePath.endsWith(".wv") || f.filePath.endsWith(".sql") =>
      resolveQueryFileRef(f)
    case f: FileRef if RelationRefResolver.isDataFilePath(f.filePath) =>
      RelationRefResolver.resolveDataFileRef(f).getOrElse(f)
    case p: PartialQueryApply =>
      // The inlined body is spliced above the already-resolved child and is not revisited by
      // the enclosing bottom-up traversal, so resolve and type the body recursively
      val inlined = FunctionInliner.resolvePartialQuery(p)
      inlined match
        case rel: Relation if !(inlined eq p) =>
          prepareRelation(rel)
        case _ =>
          p
    case u: UnaryRelation if hasUnresolvedUnderscore(u) =>
      val contextType = u.inputRelation.relationType
      u.transformChildExpressions { case expr: Expression =>
        expr.transformExpression {
          case ref: ContextInputRef if !ref.dataType.isResolved =>
            ContextInputRef(dataType = contextType, ref.span)
        }
      }

  end structuralResolutionRule

  /**
    * Returns true if the relation tree may contain function applications, method or native function
    * references, aggregation shorthands, or grouping-key indexes that require the inlining
    * fixpoint. This scan keeps simple queries to a single typing pass
    */
  private def needsFunctionResolution(r: Relation): Boolean =
    var needs = false
    r.traverseExpressions {
      case _: FunctionApply =>
        needs = true
      case _: DotRef =>
        needs = true
      case id: Identifier if id.unresolved && id.nonEmpty && !hasResolvedType(id) =>
        needs = true
    }
    if !needs then
      r.traverse { case p: AggSelect =>
        if p.selectItems.exists(_.nameExpr.isGroupingKeyIndex) then
          needs = true
      }
    needs

  /**
    * Import a query from another .wv or .sql file by compiling the referenced unit with this phase
    * and replacing the reference with its resolved single query
    */
  private def resolveQueryFileRef(r: FileRef)(using ctx: Context): Relation =
    ctx.findCompilationUnit(r.filePath) match
      case None =>
        throw StatusCode.FILE_NOT_FOUND.newException(s"${r.path} is not found")
      case Some(unit) =>
        // compile the query
        val compiledUnit =
          if unit.isFinished(Typer) then
            unit
          else
            run(unit, ctx.withCompilationUnit(unit))
        // Replace with the resolved plan
        compiledUnit.resolvedPlan match
          case PackageDef(_, List(rel: Relation), _, _) =>
            BracedRelation(rel, r.span)
          case other =>
            throw StatusCode
              .SYNTAX_ERROR
              .newException(s"${unit.sourceFile} is not a single query file")

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
    */
  private def typeRelation(r: Relation)(using ctx: Context): Unit =
    // First type children (bottom-up) - in place
    r.children
      .foreach {
        case child: Relation =>
          typeRelation(child)
        case child: LogicalPlan =>
          typePlan(child)
      }

    // Get input type from children (now typed)
    val inputType = r.inputRelationType

    // Type direct child expressions with the input type context - in place
    val exprCtx = ctx.withInputType(inputType)
    r match
      case t: TestRelation =>
        // Test expressions are an assertion DSL over the tested query's result
        typeTestExpression(t.testExpr)(using exprCtx)
      case _ =>
        r.childExpressions
          .foreach { expr =>
            typeExpression(expr)(using exprCtx)
          }

    // Set tpe from relationType
    r.tpe = r.relationType

  /**
    * Type a test expression. Tests inspect the result of the tested query through pseudo members of
    * the underscore (_.size, _.columns, _.rows, _.output, _.json) that are evaluated by the test
    * runner, so they are typed here rather than through the regular member lookup
    */
  private def typeTestExpression(e: Expression)(using ctx: Context): Unit =
    e.children.foreach(typeTestExpression)
    e match
      case d @ DotRef(q: Identifier, name, _, _) if q.fullName == "_" =>
        name.leafName match
          case "size" =>
            d.tpe = DataType.LongType
          case "columns" =>
            d.tpe = DataType.ArrayType(DataType.StringType)
          case "rows" =>
            d.tpe = DataType.ArrayType(DataType.ArrayType(DataType.AnyType))
          case "output" | "json" =>
            d.tpe = DataType.StringType
          case _ =>
            TyperRules.exprRules.applyOrElse(d, identity[Expression])
      case i: Identifier if i.fullName == "_" && !hasResolvedType(i) =>
        // The underscore denotes the full result of the tested query
        ctx.inputType match
          case dt: DataType if dt.isResolved =>
            i.tpe = dt
          case _ =>
            ()
      case other =>
        TyperRules.exprRules.applyOrElse(other, identity[Expression])

  /**
    * Type an expression using TyperRules. Uses in-place traversal for efficiency - only updates
    * mutable tpe fields without creating new tree instances.
    */
  private def typeExpression(expr: Expression)(using ctx: Context): Expression =
    // Type relations nested inside this expression (e.g. subquery expressions) so their
    // inner expressions resolve against their own input relations
    expr match
      case s: SubQueryExpression =>
        typeRelation(s.query)
      case i: InRelation =>
        typeRelation(i.in)
      case i: NotInRelation =>
        typeRelation(i.in)
      case i: TupleInRelation =>
        typeRelation(i.in)
      case i: TupleNotInRelation =>
        typeRelation(i.in)
      case _ =>
        ()

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
