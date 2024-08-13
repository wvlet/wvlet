package com.treasuredata.flow.lang.compiler.analyzer

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.compiler.RewriteRule.PlanRewriter
import com.treasuredata.flow.lang.compiler.{
  CompilationUnit,
  Context,
  MethodSymbolInfo,
  ModelSymbolInfo,
  MultipleSymbolInfo,
  Name,
  Phase,
  RewriteRule,
  Symbol,
  TermName,
  TypeName,
  TypeSymbolInfo
}
import com.treasuredata.flow.lang.model.DataType.{
  NamedType,
  PrimitiveType,
  SchemaType,
  UnresolvedType,
  VarArgType
}
import com.treasuredata.flow.lang.model.Type.FunctionType
import com.treasuredata.flow.lang.model.expr.*
import com.treasuredata.flow.lang.model.plan.*
import com.treasuredata.flow.lang.model.{DataType, RelationType, RelationTypeList}
import wvlet.log.LogSupport

object TypeResolver extends Phase("type-resolver") with LogSupport:

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    // resolve plans
    val resolvedPlan: LogicalPlan = TypeResolver.resolve(unit.unresolvedPlan, context)
    unit.resolvedPlan = resolvedPlan
    unit

  def defaultRules: List[RewriteRule] =
    resolveTypeDef ::                 // resolve known types in TypeDef
      resolveLocalFileScan ::         // resolve local file scans for DuckDb
      resolveTableRef ::              // resolve table reference (model or schema) types
      resolveModelDef ::              // resolve ModelDef
      resolveTransformItem ::         // resolve transform items prefixed with column name
      resolveSelectItem ::            // resolve select items (projected columns)
      resolveGroupingKey ::           // resolve Aggregate keys
      resolveGroupingKeyIndexes ::    // resolve grouping key indexes, _1, _2, .. in select clauses
      resolveRelation ::              // resolve expressions inside relation nodes
      resolveUnderscore ::            // resolve underscore in relation nodes
      resolveThis ::                  // resolve `this` in type definitions
      resolveFunctionBodyInTypeDef :: //
      resolveFunctionApply ::         // Resolve function args
      resolveInlineRef ::             // Resolve inline-expression expansion
      resolveAggregationFunctions ::  // Resolve aggregation expression without group by
      resolveModelDef ::              // Resolve models again to use the updated types
      Nil

  private def lookupType(name: Name, context: Context): Option[Symbol] =
    context.scope.lookupSymbol(name) match
      case Some(s) =>
        Some(s)
      case None =>
        var foundSym: Option[Symbol] = None
        context
          .importDefs
          .collectFirst {
            case i: Import if i.importRef.leafName == name.name =>
              // trace(s"Found import ${i}")
              for
                ctx <- context.global.getAllContexts
                if foundSym.isEmpty
              do
                // trace(s"Lookup ${name} in ${ctx.compilationUnit.sourceFile.fileName}")
                ctx
                  .compilationUnit
                  .knownSymbols
                  .collectFirst {
                    case s: Symbol if s.name == name =>
                      trace(s"Found ${s.name} in ${ctx.compilationUnit}")
                      foundSym = Some(s)
                  }
          }

        if foundSym.isEmpty then
          // Search for global and preset contexts
          for
            // TODO Search global scope
            ctx <- context
              .global
              .getAllContexts
              .filter(_.isGlobalContext) // preset libraries or global symbols
            if foundSym.isEmpty
          do
            // trace(
            //  s"Searching ${name} in ${ctx.compilationUnit.sourceFile.fileName}\n${ctx.compilationUnit.knownSymbols} ${ctx.hashCode()}"
            // )
            ctx
              .compilationUnit
              .knownSymbols
              .collectFirst {
                case s: Symbol if s.name == name =>
                  trace(s"Found ${s.name} in ${ctx.compilationUnit}")
                  foundSym = Some(s)
              }

        foundSym

  def resolve(plan: LogicalPlan, context: Context): LogicalPlan =

    def preScan(p: LogicalPlan, ctx: Context): Context =
      p match
        case t: TypeDef =>
          ctx.enter(t.symbol)
          ctx
        case m: ModelDef =>
          ctx.enter(m.symbol)
          ctx
        case other =>
          ctx

    def nextContext(p: LogicalPlan, ctx: Context): Context =
      p match
        case i: Import =>
          ctx.withImport(i)
        case _ =>
          ctx

    plan match
      case p: PackageDef =>
        val packageCtx = context.newContext(p.symbol)

        // Load symbols defined in the compilation unit
        p.statements
          .foldLeft(packageCtx) { (prevContext, stmt) =>
            preScan(stmt, prevContext)
          }

        // Rewrite individual statement while maintaining Import contexts
        var ctx: Context = packageCtx
        val stmts        = List.newBuilder[LogicalPlan]
        p.statements
          .foreach { stmt =>
            val newStmt = RewriteRule.rewrite(stmt, defaultRules, ctx)
            stmts += newStmt

            if !ctx.compilationUnit.isPreset then
              trace(newStmt.pp)

            // Update the tree reference from the symbol
            newStmt match
              case m: ModelDef =>
                m.symbol.tree = m
              case t: TypeDef =>
                t.symbol.tree = t
              case _ =>

            ctx = nextContext(newStmt, ctx)
          }
        p.copy(statements = stmts.result())
      case other =>
        throw StatusCode
          .UNEXPECTED_STATE
          .newException(
            s"Unexpected plan type: ${other.getClass.getName}",
            other.sourceLocation(using context.compilationUnit)
          )
    end match

  end resolve

  // Resolve the type of TypeDef
  private object resolveTypeDef extends RewriteRule:
    override def apply(context: Context): PlanRewriter =
      case t: TypeDef if !t.symbol.dataType.isResolved =>
        t.symbol.dataType match
          case s: SchemaType =>
            var updated = false
            val newCols = s
              .columnTypes
              .map { ct =>
                if ct.dataType.isResolved then
                  ct
                else
                  lookupType(ct.dataType.typeName, context) match
                    case Some(sym) =>
                      val si = sym.symbolInfo
                      // trace(s"Resolved ${ct.dataType} as ${si.dataType}")
                      updated = true
                      ct.copy(dataType = si.dataType)
                    case None =>
                      warn(s"Cannot resolve type: ${ct.dataType.typeName}")
                      ct
              }
            if updated then
              val newType = s.copy(columnTypes = newCols)
              trace(s"Resolved ${t.name} as ${newType}")
              t.symbol.symbolInfo.dataType = newType
              newType
            else
              s
          case other =>
            warn(
              f"Unresolved type ${t}[${t.params}]: ${other} (${t.locationString(using context)})"
            )
        end match
        // No tree rewrite is required
        t

    end apply

  end resolveTypeDef

  /**
    * Resolve schema of local file scans (e.g., JSON, Parquet)
    */
  private object resolveLocalFileScan extends RewriteRule:
    override def apply(context: Context): PlanRewriter =
      case r: FileScan if r.path.endsWith(".json") =>
        val file             = context.getDataFile(r.path)
        val jsonRelationType = JSONAnalyzer.analyzeJSONFile(file)
        val cols             = jsonRelationType.fields
        JSONFileScan(file, jsonRelationType, cols, r.nodeLocation)
      case r: FileScan if r.path.endsWith(".parquet") =>
        val file                = context.dataFilePath(r.path)
        val parquetRelationType = ParquetAnalyzer.guessSchema(file)
        val cols                = parquetRelationType.fields
        ParquetFileScan(file, parquetRelationType, cols, r.nodeLocation)

  private object resolveModelDef extends RewriteRule:
    override def apply(context: Context): PlanRewriter = {
      case m: ModelDef if m.givenRelationType.isEmpty && !m.symbol.dataType.isResolved =>
        m.relationType match
          case r: RelationType if r.isResolved =>
            // given model type is already resolved
            m.symbol.symbolInfo match
              case t: ModelSymbolInfo =>
                t.dataType = r
                debug(s"Resolved ${t}")
              case other =>
          case other =>
            trace(
              s"Unresolved model type for ${m.name}: ${other} (${m.locationString(using context)})"
            )
        m
      case m: ModelDef if m.givenRelationType.isDefined && !m.symbol.dataType.isResolved =>
        val modelType = m.givenRelationType.get
        lookupType(modelType.typeName, context) match
          case Some(sym) =>
            val si = sym.symbolInfo
            trace(s"${modelType.typeName} -> ${m.symbol.id} -> ${m.symbol.symbolInfo}")
            val modelSymbolInfo = m.symbol.symbolInfo
            si.dataType match
              case r: RelationType =>
                trace(s"Resolved model type: ${m.name} as ${r}")
                modelSymbolInfo.dataType = r
                // TODO Develop safe copy or embed Symbol as Plan parameter
                val newModel = m.copy(givenRelationType = Some(r))
                newModel.symbol = m.symbol
                newModel
              case other =>
                warn(s"Unexpected model type: ${other} ${m.locationString(using context)}")
                m
          case None =>
            warn(s"Cannot resolve model type: ${modelType.typeName}")
            m
    }

  end resolveModelDef

  /**
    * Resolve TableRefs with concrete TableScans using the table schema in the catalog.
    */
  private object resolveTableRef extends RewriteRule:
    private def lookup(qName: NameExpr, context: Context): Option[Symbol] =
      qName match
        case i: Identifier =>
          lookupType(i.toTermName, context)
        case d: DotRef =>
          // TODO
          warn(s"TODO: resolve ${d}")
          None
        case _ =>
          None

    override def apply(context: Context): PlanRewriter =
      case ref: TableRef if !ref.relationType.isResolved =>
        lookup(ref.name, context) match
          case Some(sym) =>
            val si = sym.symbolInfo
            si.tpe match
              case r: RelationType =>
                debug(s"resolved ${sym} ${ref.locationString(using context)}")
                ModelScan(sym.name, Nil, r, r.fields, ref.nodeLocation)
              case _ =>
                ref
          case None =>
            // Lookup known types
            val tblType = Name.typeName(ref.name.leafName)
            lookupType(tblType, context).map(_.symbolInfo.dataType) match
              case Some(tpe: SchemaType) =>
                trace(s"Found a table type for ${tblType}: ${tpe}")
                TableScan(tblType.toTermName, tpe, tpe.fields, ref.nodeLocation)
              case _ =>
                warn(s"Unresolved table ref: ${ref.name.fullName}: ${context.scope.getAllEntries}")
                ref
      case ref: ModelRef if !ref.relationType.isResolved =>
        lookup(ref.name, context) match
          case Some(sym) =>
            val si = sym.symbolInfo
            si.tpe match
              case r: RelationType =>
                trace(s"Resolved model ref: ${ref.name.fullName} as ${r}")
                ModelScan(sym.name, ref.args, r, r.fields, ref.nodeLocation)
              case _ =>
                ref
          case None =>
            trace(s"Unresolved model ref: ${ref.name.fullName}")
            ref

    end apply

  end resolveTableRef

  private object resolveTransformItem extends RewriteRule:
    override def apply(context: Context): PlanRewriter = { case t: Transform =>
      val newItems: Seq[Attribute] = t
        .transformItems
        .map {
          case s: SingleColumn =>
            // resolve only the body expression
            s.copy(expr = s.expr.transformExpression(resolveExpression(t.relationType, context)))
          case x: Attribute =>
            x.transformExpression(resolveExpression(t.relationType, context))
              .asInstanceOf[Attribute]
        }
      t.copy(transformItems = newItems)
    }

    /**
      * Resolve select items (projected attributes) in Project nodes
      */

  private object resolveSelectItem extends RewriteRule:
    def apply(context: Context): PlanRewriter = { case p: Project =>
      val resolvedChild = p.child.transform(resolveRelation(context)).asInstanceOf[Relation]
      val resolvedColumns: Seq[Attribute] = p
        .selectItems
        .map {
          case s: SingleColumn =>
            val resolvedExpr: Expression = s
              .expr
              .transformExpression(resolveExpression(p.child.relationType, context))
            s.copy(expr = resolvedExpr)

          case x: Attribute =>
            x.transformExpression(resolveExpression(p.child.relationType, context))
              .asInstanceOf[Attribute]
        }
      Project(resolvedChild, resolvedColumns, p.nodeLocation)
    }

  private object resolveGroupingKey extends RewriteRule:
    override def apply(context: Context): PlanRewriter = { case g: Aggregate =>
      val newKeys = g
        .groupingKeys
        .map { k =>
          k.transformExpression(resolveExpression(g.child.relationType, context))
            .asInstanceOf[GroupingKey]
        }
      g.copy(groupingKeys = newKeys)
    }

  /**
    * Resolve grouping key indexes _1, _2, .... in select clauses
    */
  private object resolveGroupingKeyIndexes extends RewriteRule:
    // Find the first Aggregate node
    private def findAggregate(r: Relation): Option[Aggregate] =
      r match
        case a: Aggregate =>
          Some(a)
        case f: Filter =>
          findAggregate(f.child)
        case p: Project =>
          findAggregate(p.child)
        case _ =>
          None

    override def apply(context: Context): PlanRewriter = {
      case p: Project if p.selectItems.exists(_.nameExpr.isGroupingKeyIndex) =>
        findAggregate(p.child) match
          case Some(agg) =>
            p.transformChildExpressions {
              case attr: SingleColumn if attr.nameExpr.isGroupingKeyIndex =>
                val index = attr.nameExpr.fullName.stripPrefix("_").toInt - 1
                if index >= agg.groupingKeys.length then
                  throw StatusCode
                    .SYNTAX_ERROR
                    .newException(
                      s"Invalid grouping key index: ${attr.nameExpr}",
                      attr.nodeLocation
                    )(using context)

                val referencedGroupingKey = agg.groupingKeys(index)
                SingleColumn(
                  referencedGroupingKey.name,
                  expr = referencedGroupingKey,
                  attr.nodeLocation
                )
            }
          case None =>
            p
    }

  end resolveGroupingKeyIndexes

  /**
    * Resolve expression in relation nodes
    */
  private object resolveRelation extends RewriteRule:
    override def apply(context: Context): PlanRewriter = {
      case r: Relation => // Regular relation and Filter etc.
        r.transformUpExpressions(resolveExpression(r.relationType, context))
    }

  /**
    * Resolve underscore (_) from the parent relation node
    */
  private object resolveUnderscore extends RewriteRule:
    private def hasUnderscore(r: Relation): Boolean =
      var found = false
      r.childExpressions
        .map { e =>
          e.traverseExpressions { case c: ContextInputRef =>
            found = true
          }
        }
      found

    override def apply(context: Context): PlanRewriter = {
      case u: UnaryRelation if hasUnderscore(u) =>
        val contextType = u.inputRelation.relationType
        trace(s"Resolved underscore (_) as ${contextType} in ${u.locationString(using context)}")
        val updated = u.transformChildExpressions { case expr: Expression =>
          expr.transformExpression {
            case ref: ContextInputRef if !ref.dataType.isResolved =>
              val c = ContextInputRef(dataType = contextType, ref.nodeLocation)
              c
          }
        }
        updated
    }

  /**
    * Resolve the type of `this` in the type definition
    */
  private object resolveThis extends RewriteRule:
    override def apply(context: Context): PlanRewriter = { case t: TypeDef =>
      val enclosing = context.scope.lookupSymbol(t.name)
      enclosing match
        case Some(s: Symbol) =>
          // TODO Handle nested definition (e.g., nested type definition)
          val r = s.symbolInfo.dataType
          t.transformUpExpressions { case th: This =>
            val newThis = th.copy(dataType = r)
            // trace(s"Resolved this: ${th} as ${newThis}")
            newThis
          }
        case _ =>
          t
    }

  /**
    * Find a corresponding MethodSymbolInfo for the given function expression
    * @param f
    * @param context
    * @return
    */
  private def resolveFunction(f: Expression, knownArgs: List[FunctionArg] = Nil)(using
      context: Context
  ): Option[MethodSymbolInfo] =
    f match
      case fa: FunctionApply =>
        resolveFunction(fa.base, fa.args)
      case i: Identifier =>
        lookupType(i.toTermName, context)
          .map(_.symbolInfo)
          .collect {
            case m: MethodSymbolInfo =>
              m
            case m: MultipleSymbolInfo =>
              // TODO resolve one of the function type
              throw StatusCode
                .SYNTAX_ERROR
                .newException(s"Ambiguous function call for ${i}", i.nodeLocation)
          }
      case d @ DotRef(qual, method: Identifier, _, _) =>
        val methodName = method.toTermName
        if qual.dataType.isResolved then
          lookupType(qual.dataType.typeName, context)
            .map(_.symbolInfo.findMember(methodName).symbolInfo)
            .collect {
              case m: MethodSymbolInfo =>
                m
              case m: MultipleSymbolInfo =>
                // TODO resolve one of the function type
                throw StatusCode
                  .SYNTAX_ERROR
                  .newException(s"Ambiguous function call for ${method}", d.nodeLocation)
            }
        else
          trace(s"Failed to find function `${methodName}` for ${qual}")
          None
      case _ =>
        trace(s"Failed to find function definition for ${f}")
        None
  end resolveFunction

  /**
    * Evaluate FunctionApply nodes with the given function definition
    */
  private object resolveFunctionApply extends RewriteRule:

    override def apply(context: Context): PlanRewriter = { case q: Query =>
      q.transformUpExpressions { case f: FunctionApply =>
        resolveFunApply(f)(using context)
      }
    }

    private def resolveFunApply(f: FunctionApply)(using ctx: Context): Expression =
      resolveFunction(f) match
        case Some(m: MethodSymbolInfo) =>
          val functionArgTypes = m.ft.args
          // Mapping function arguments aligned to the function definition
          var index        = 0
          val resolvedArgs = List.newBuilder[(TermName, Expression)]

          def mapArg(args: List[FunctionArg]): Unit =
            if !args.isEmpty then
              args.head match
                case FunctionArg(None, expr, loc) =>
                  if index >= functionArgTypes.length then
                    throw StatusCode.SYNTAX_ERROR.newException("Too many arguments", loc)
                  val argType = functionArgTypes(index)
                  argType.dataType match
                    case VarArgType(elemType) =>
                      index += 1
                      resolvedArgs += argType.name -> ListExpr(args, loc)
                    // all args are consumed
                    case _ =>
                      index += 1
                      resolvedArgs += argType.name -> expr
                      mapArg(args.tail)
                case FunctionArg(Some(argName), expr, loc) =>
                  functionArgTypes.find(_.name == argName) match
                    case Some(argType) =>
                      argType.dataType match
                        case VarArgType(elemType) =>
                          resolvedArgs += argName -> ListExpr(args, expr.nodeLocation)
                        // all args are consumed
                        case _ =>
                          resolvedArgs += argName -> expr
                          mapArg(args.tail)
                    case None =>
                      throw StatusCode
                        .SYNTAX_ERROR
                        .newException("Unknown argument name: ${argName}", loc)

          // Resolve function arguments
          mapArg(f.args)

          trace(s"Resolved args for ${m.name}: ${resolvedArgs.result()}")
          // Resolve identifiers in the function body with the given function arguments
          val expr = m
            .body
            .map {
              _.transformUpExpression:
                case th: This =>
                  f.base match
                    case d: DotRef =>
                      d.qualifier
                    case _ =>
                      th
                case i: Identifier =>
                  resolvedArgs.result().find(_._1 == i.toTermName) match
                    case Some((_, expr)) =>
                      expr
                    case None =>
                      i
            }
            .getOrElse(f)
          expr match
            case i: InterpolatedString =>
              // TODO Support adding DataType to arbitrary expressions
              // Resolve interpolated string from function argument type
              i.copy(dataType = m.ft.returnType)
            case _ =>
              expr
        case _ =>
          f

  end resolveFunctionApply

  private object resolveInlineRef extends RewriteRule:
    private def resolveRef(using ctx: Context): PartialFunction[Expression, Expression] =
      case ref: DotRef =>
        resolveFunction(ref) match
          case Some(m: MethodSymbolInfo) =>
            // Replace {this} -> {qual}
            m.body
              .map { body =>
                body.transformExpression { case th: This =>
                  ref.qualifier
                }
              }
              .getOrElse(ref)
          case _ =>
            ref
    end resolveRef

    override def apply(context: Context): PlanRewriter = { case r: Query =>
      r.transformUpExpressions(resolveRef(using context))
    }

  end resolveInlineRef

  private object resolveFunctionBodyInTypeDef extends RewriteRule:
    override def apply(context: Context): PlanRewriter = { case td: TypeDef =>
      // Collect fields defined in the type
      val fields: List[NamedType] =
        td.symbol.dataType match
          case r: RelationType =>
            r.fields.toList
          case _ =>
            throw StatusCode
              .UNEXPECTED_STATE
              .newException(s"TypeDef ${td.name} is not resolved with RelationType")

      val newElems: List[TypeElem] = td
        .elems
        .map {
          case f: FunctionDef =>
            val retType = f
              .retType
              .map { t =>
                context.scope.lookupSymbol(t.typeName) match
                  case Some(sym) =>
                    sym.dataType
                  case None =>
                    t
              }
            // Function arguments that will be used inside the expression
            val argFields: List[NamedType] = f
              .args
              .map { arg =>
                NamedType(arg.name, arg.dataType)
              }
            // create a type that includes function arguments
            val knownFields = fields ++ argFields
            val inputType   = SchemaType(None, Name.NoTypeName, knownFields)
            // trace(s"resolve function body: ${f.name} using ${inputType}")
            val newF = f.copy(
              retType = retType,
              expr = f.expr.map(x => x.transformUpExpression(resolveExpression(inputType, context)))
            )
            newF
          case other =>
            other
        }
      val newTypeDef = td.copy(elems = newElems)
      // TODO Embed Symbol to TypeDef param
      newTypeDef.symbol = td.symbol
      newTypeDef
    }

  end resolveFunctionBodyInTypeDef

  /**
    * For aggregation, which has no Aggregate plan node, a simple expression like
    * {{{(l_extendedprice * l_discount)}}} can be an aggregation expression if aggregation function
    * is applied like {{{(l_extendedprice * l_discount).sum}}}.
    */
  private object resolveAggregationFunctions extends RewriteRule:

    private var aggregationFunctions: List[Symbol] = Nil

    private def init(ctx: Context): Unit =
      // TODO Support adding more methods to the array type
      if aggregationFunctions.isEmpty then
        aggregationFunctions = lookupType(Name.typeName("array"), ctx)
          .map(_.symbolInfo)
          .collect { case t: TypeSymbolInfo =>
            t.members
          }
          .getOrElse(Nil)

    private def resolveAggregationExpr(using Context): PartialFunction[Expression, Expression] =
      case d @ DotRef(qual, name: Identifier, _, _) =>
        val nme = name.toTermName
        aggregationFunctions
          .find(_.name == nme)
          .map(_.symbolInfo)
          .collect { case m: MethodSymbolInfo =>
            m
          }
          .map { m =>
            // inline aggregation function body
            m.body
              .map { body =>
                body.transformUpExpression {
                  case th: This =>
                    qual
                  case i: InterpolatedString =>
                    // Resolve interpolated string from function argument type
                    i.copy(dataType = m.ft.returnType)
                }
              }
              .getOrElse(d)
          }
          .getOrElse(d)
    end resolveAggregationExpr

    override def apply(context: Context): PlanRewriter = { case q: Query =>
      init(context)
      q.transformUpExpressions(resolveAggregationExpr(using context))
    }

  end resolveAggregationFunctions

  /**
    * Resolve the given expression type using the input attributes from child plan nodes
    *
    * @param expr
    * @param knownAttributes
    */
  private def resolveExpression(
      inputRelationType: RelationType,
      context: Context
  ): PartialFunction[Expression, Expression] =
//    case s: SubQueryExpression =>
//      // Resolve subquery expression
//      val rt = RelationTypeList(
//        Name.typeName(RelationType.newRelationTypeName),
//        List(inputRelationType, s.query.relationType)
//      )
//      s.transformUpExpression {
//        resolveExpression(rt, context)
//      }
    case a: Attribute if !a.dataType.isResolved && !a.nameExpr.isEmpty =>
      val name = a.fullName
      trace(s"Find ${name} in ${inputRelationType.fields} ${a.locationString(using context)}")
      inputRelationType.find(x => x.name == name) match
        case Some(tpe) =>
          a // a.withDataType(tpe.dataType)
        case None =>
          a
    case ref: DotRef =>
      val refName      = Name.termName(ref.name.leafName)
      val resolvedQual = resolveExpression(ref.qualifier, inputRelationType, context)
      // Resolve types after following . (dot)
      resolvedQual.dataType match
        case t: SchemaType =>
          trace(s"Find reference from ${t} -> ${ref.name}")
          t.columnTypes.find(_.name.name == ref.name.leafName) match
            case Some(col) =>
              trace(s"${t}.${col.name} is a column")
              ResolvedAttribute(
                Name.termName(ref.name.leafName),
                col.dataType,
                None,
                ref.nodeLocation
              )
            case None =>
              // Lookup functions
              lookupType(t.typeName, context).map(_.symbolInfo) match
                case Some(tpe: TypeSymbolInfo) =>
                  tpe.declScope.lookupSymbol(refName).map(_.symbolInfo) match
                    case Some(method: MethodSymbolInfo) =>
                      trace(s"Resolved ${t}.${ref.name.fullName} as a function")
                      ref.copy(dataType = method.ft.returnType)
                    case _ =>
                      warn(s"${t}.${ref.name.fullName} is not found")
                      ref
                case _ =>
                  warn(s"${t}.${ref.name.fullName} is not found")
                  ref
        case other =>
          // TODO Support multiple context-specific functions
          // warn(s"qualifier's type name: ${other.typeName}: ${ref.qualifier}")
          lookupType(other.typeName, context).map(_.symbolInfo) match
            case Some(tpe: TypeSymbolInfo) =>
              tpe.declScope.lookupSymbol(refName).map(_.symbolInfo) match
                case Some(method: MethodSymbolInfo) =>
                  trace(s"Resolved ${ref} as ${method.ft}")
                  ref.copy(dataType = method.ft.returnType)
                case _ =>
                  trace(s"Failed to resolve ${ref} as ${other}")
                  ref
            case _ =>
              trace(s"TODO: resolve ref: ${ref.fullName} as ${other}")
              ref
      end match
    case i: InterpolatedString if i.prefix.fullName == "sql" =>
      // Ignore it because embedded SQL expressions have no static type
      i
    case i: Identifier if !i.dataType.isResolved =>
      inputRelationType.find(x => x.name == i.fullName) match
        case Some(attr) =>
          val ri = i.toResolved(attr.dataType)
          trace(s"Resolved identifier: ${ri}")
          ri
        case None =>
          trace(
            s"Failed to resolve identifier: ${i} (${i.locationString(using context)}) from ${inputRelationType.fields}"
          )
          i
  end resolveExpression

  private def resolveExpression(
      expr: Expression,
      inputRelationType: RelationType,
      context: Context
  ): Expression = resolveExpression(inputRelationType, context)
    .applyOrElse(expr, identity[Expression])

  /**
    * Resolve the given list of attribute types using known attributes from the child plan nodes as
    * hints
    *
    * @param attributes
    * @param knownAttributes
    * @param context
    * @return
    */
  private def resolveAttributes(
      attributes: Seq[Attribute],
      inputRelationType: RelationType,
      context: Context
  ): Seq[Attribute] = attributes.map { a =>
    a.transformExpression(resolveExpression(inputRelationType, context)).asInstanceOf[Attribute]
  }

end TypeResolver
