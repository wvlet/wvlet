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
package wvlet.lang.compiler.analyzer

import wvlet.airframe.msgpack.spi.MsgPack
import wvlet.lang.api.{Span, StatusCode}
import wvlet.lang.catalog.Catalog.TableName
import wvlet.lang.compiler.RewriteRule.PlanRewriter
import wvlet.lang.compiler.{
  CompilationUnit,
  Context,
  ContextLogSupport,
  MethodSymbolInfo,
  ModelSymbolInfo,
  MultipleSymbolInfo,
  Name,
  Phase,
  RelationAliasSymbolInfo,
  RewriteRule,
  Symbol,
  TermName,
  TypeName,
  TypeSymbolInfo
}
import wvlet.lang.model.DataType.{
  AnyType,
  NamedType,
  SchemaType,
  TypeParameter,
  TypeVariable,
  UnknownType,
  VarArgType
}
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.lang.model.{DataType, RelationType}
import wvlet.log.LogSupport
import wvlet.lang.compiler.ContextUtil.*

import scala.util.Try

object TypeResolver extends Phase("type-resolver") with ContextLogSupport:

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
      resolveSelectItem ::            // resolve select items (projected columns)
      resolveGroupingKey ::           // resolve Aggregate keys
      resolveGroupingKeyIndexes ::    // resolve grouping key indexes, _1, _2, .. in select clauses
      resolveRelation ::              // resolve expressions inside relation nodes
      resolveUnderscore ::            // resolve underscore in relation nodes
      resolveThis ::                  // resolve `this` in type definitions
      resolveFunctionBodyInTypeDef :: //
      resolveFunctionApply ::         // Resolve function args
      resolveInlineRef ::             // Resolve inline-expression expansion
      resolveNativeExpressions ::     // Resolve native expressions
      resolveNoGroupByAggregations :: // Resolve aggregation expression without group by
      resolveModelDef ::              // Resolve models again to use the updated types
      resolveModelScan ::             // Resolve scanned model types
      Nil

  private def lookupType(name: Name, context: Context): Option[Symbol] = context.findSymbolByName(
    name
  )

  def resolve(plan: LogicalPlan, context: Context): LogicalPlan =

    def preScan(p: LogicalPlan, ctx: Context): Context =
      p match
        case t: TypeDef =>
          ctx.enter(t.symbol)
          ctx
        case m: ModelDef =>
          ctx.enter(m.symbol)
          ctx
        case q: Relation =>
          q.traverseOnce { case s: HasTableOrFileName =>
            ctx.enter(s.symbol)
            s match
              case u: UnaryRelation =>
                preScan(u.child, ctx)
          }
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

            ctx.logTrace(s"Untyped plan:\n${stmt.pp}")

            var newStmt = RewriteRule.rewrite(stmt, defaultRules, ctx)
            // Resolve again if the statement is not resolved
            if !newStmt.resolved then
              ctx.logTrace(s"Trying to resolve unresolved plan again:\n${newStmt.pp}")
              newStmt = RewriteRule.rewriteUnresolved(newStmt, defaultRules, ctx)
            stmts += newStmt

            if !ctx.compilationUnit.isPreset then
              ctx.logDebug(newStmt.pp)

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
      case r: Relation =>
        RewriteRule.rewrite(r, defaultRules, context)
      case other =>
        throw StatusCode
          .UNEXPECTED_STATE
          .newException(
            s"Unexpected plan type: ${other.getClass.getName}",
            other.sourceLocation(using context)
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
              t.symbol.symbolInfo.withType(newType)
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
    * Resolve schema of local file scans (e.g., JSON, Parquet).
    *
    * TODO: Introduce lazy evaluation of the schema to avoid unnecessary schema resolution
    */
  private object resolveLocalFileScan extends RewriteRule:
    override def apply(context: Context): PlanRewriter =
      case r: FileRef if r.filePath.endsWith(".wv") || r.filePath.endsWith(".sql") =>
        // import a query from another .wv or .sql file
        context.findCompilationUnit(r.filePath) match
          case None =>
            throw StatusCode.FILE_NOT_FOUND.newException(s"${r.path} is not found")
          case Some(unit) =>
            // compile the query
            val compiledUnit =
              if unit.isFinished(TypeResolver) then
                unit
              else
                run(unit, context.withCompilationUnit(unit))
            // Replace with the resolved plan
            compiledUnit.resolvedPlan match
              case PackageDef(_, List(rel: Relation), _, _) =>
                BracedRelation(rel, r.span)
              case other =>
                throw StatusCode
                  .SYNTAX_ERROR
                  .newException(s"${unit.sourceFile} is not a single query file")
      case f: FileRef if f.filePath.endsWith(".json") || f.filePath.endsWith(".json.gz") =>
        val file             = context.getDataFile(f.filePath)
        val jsonRelationType = JSONAnalyzer.analyzeJSONFile(file)
        val cols             = jsonRelationType.fields
        FileScan(SingleQuoteString(file, f.span), jsonRelationType, cols, f.span)
      case f: FileRef if f.filePath.endsWith(".parquet") || f.filePath.endsWith(".csv") =>
        val file         = context.dataFilePath(f.filePath)
        val relationType = DuckDBAnalyzer.guessSchema(file)
        val cols         = relationType.fields
        FileScan(SingleQuoteString(file, f.span), relationType, cols, f.span)

    end apply

  end resolveLocalFileScan

  private object resolveModelDef extends RewriteRule:
    override def apply(context: Context): PlanRewriter = {
      case m: ModelDef if m.givenRelationType.isEmpty && !m.symbol.dataType.isResolved =>
        m.relationType match
          case r: RelationType if r.isResolved =>
            // given model type is already resolved
            m.symbol.symbolInfo match
              case t: ModelSymbolInfo =>
                t.withDataType(r)
              case other =>
          case other =>
            context.logTrace(
              s"Unresolved model type for ${m.name}: ${other} (${m.locationString(using context)})"
            )
        m
      case m: ModelDef if m.givenRelationType.isDefined && !m.symbol.dataType.isResolved =>
        val modelType = m.givenRelationType.get
        lookupType(modelType.typeName, context) match
          case Some(sym) =>
            val si = sym.symbolInfo
            context.logTrace(s"${modelType.typeName} -> ${m.symbol.id} -> ${m.symbol.symbolInfo}")
            val modelSymbolInfo = m.symbol.symbolInfo
            si.dataType match
              case r: RelationType =>
                context.logTrace(s"Resolved model type: ${m.name} as ${r}")
                modelSymbolInfo.withDataType(r)
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

  private object resolveModelScan extends RewriteRule:
    override def apply(context: Context): PlanRewriter =
      case m: ModelScan if !m.resolved =>
        context.findTermSymbolByName(m.name.fullName) match
          case Some(sym) if sym.isModelDef =>
            sym.tree match
              case md: ModelDef =>
                val newModelScan = m.copy(schema = md.relationType)
                newModelScan.symbol = md.child.symbol
                newModelScan
              case _ =>
                m
          case _ =>
            m

  end resolveModelScan

  /**
    * Resolve TableRefs with concrete TableScans using the table schema in the catalog.
    */
  private object resolveTableRef extends RewriteRule:
    private def lookup(qName: NameExpr, context: Context): Option[Symbol] =
      def getTableName(d: DotRef): Option[TableName] = Try(TableName(d.fullName)).toOption

      qName match
        case i: Identifier =>
          lookupType(i.toTermName, context)
        case d: DotRef =>
          // TODO Load table schema from the given qualified name
//          getTableName(d).flatMap { tbl =>
//            context.catalog.getTable(tbl)
//          }
          None
        case _ =>
          None

    override def apply(context: Context): PlanRewriter =
      case ref: TableRef if !ref.relationType.isResolved =>
        lookup(ref.name, context) match
          case Some(sym) =>
            sym.tree match
              case m: ModelDef =>
                val r = m.child.relationType
                ModelScan(TableName(ref.name.fullName), Nil, r, ref.span)
              case v: ValDef =>
                // Check if this is a table value constant by looking at the dataType
                v.dataType match
                  case schemaType: DataType.SchemaType =>
                    // Handle table value constants: val t1(id, val) = [[...]]
                    // The expression is an ArrayConstructor containing rows (each row is also an ArrayConstructor)
                    v.expr match
                      case arr: ArrayConstructor =>
                        // arr.values contains the rows - use them directly
                        val values = Values(arr.values, schemaType, arr.span)
                        values
                      case other =>
                        ref
                  case _ =>
                    // Regular val definition, not a table value constant
                    ref
              case _ =>
                val si = sym.symbolInfo
                si match
                  case relAlias: RelationAliasSymbolInfo =>
                    // Replace alias to the referenced query
                    sym.tree.asInstanceOf[Relation]
                  case _ =>
                    ref
          case None =>
            // Lookup known types
            val tblType = Name.typeName(ref.name.leafName)
            lookupType(tblType, context).map(_.symbolInfo.dataType) match
              case Some(tpe: SchemaType) =>
                context.logTrace(s"Found a table type for ${tblType}: ${tpe}")
                val tableName = TableName.parse(tblType.toTermName.name)
                TableScan(tableName, tpe, tpe.fields, ref.span)
              case _ =>
                val tableName = TableName.parse(ref.name.fullName)
                context
                  .catalog
                  .findTable(
                    tableName.schema.getOrElse(context.defaultSchema),
                    tableName.name
                  ) match
                  case Some(tbl) =>
                    TableScan(tableName, tbl.schemaType, tbl.schemaType.fields, ref.span)
                  case None =>
                    context
                      .workEnv
                      .errorLogger
                      .debug(
                        s"Unresolved table ref: ${ref.name.fullName}: ${context
                            .scope
                            .getAllEntries}"
                      )
                    ref
      case ref: TableFunctionCall if !ref.relationType.isResolved =>
        lookup(ref.name, context) match
          case Some(sym) =>
            val si = sym.symbolInfo
            si.tpe match
              case r: RelationType =>
                context.logTrace(s"Resolved model ref: ${ref.name.fullName} as ${r}")
                ModelScan(TableName(sym.name.name), ref.args, r, ref.span)
              case _ =>
                ref
          case None =>
            context.logTrace(s"Unresolved model ref: ${ref.name.fullName}")
            ref

    end apply

  end resolveTableRef

  /**
    * Resolve select items (projected attributes) in Project nodes
    */
  private object resolveSelectItem extends RewriteRule:
    def apply(context: Context): PlanRewriter = { case p: Project =>
      val resolvedChild = p.child.transform(resolveRelation(context)).asInstanceOf[Relation]
      val resolvedColumns: List[Attribute] = p
        .selectItems
        .map {
          case s: SingleColumn =>
            s.transformChildExpressions(resolveExpression(p.inputRelationType, context))
          case x: Attribute =>
            x.transformChildExpressions(resolveExpression(p.inputRelationType, context))
        }
      Project(resolvedChild, resolvedColumns, p.span)
    }

  private object resolveGroupingKey extends RewriteRule:
    override def apply(context: Context): PlanRewriter = { case g: GroupBy =>
      val newKeys = g
        .groupingKeys
        .map { k =>
          k.transformChildExpressions(resolveExpression(g.child.relationType, context))
        }
      g.copy(groupingKeys = newKeys)
    }

  /**
    * Resolve grouping key indexes _1, _2, .... in select clauses
    */
  private object resolveGroupingKeyIndexes extends RewriteRule:
    // Find the first Aggregate node
    private def findAggregate(r: Relation): Option[GroupBy] =
      r match
        case a: GroupBy =>
          Some(a)
        case f: FilteringRelation =>
          findAggregate(f.child)
        case p: Project =>
          findAggregate(p.child)
        case _ =>
          None

    override def apply(context: Context): PlanRewriter = {
      case p: AggSelect if p.selectItems.exists(_.nameExpr.isGroupingKeyIndex) =>
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
                      context.sourceLocationAt(attr.span)
                    )

                val referencedGroupingKey = agg.groupingKeys(index)
                SingleColumn(referencedGroupingKey.name, expr = referencedGroupingKey, attr.span)
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
        // context.logWarn(s"Resolving relation: ${r} with ${r.inputRelationType}")
        val newRelation = r.transformChildExpressions(
          resolveExpression(r.inputRelationType, context)
        )
        newRelation
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
        context.logTrace(
          s"Resolved underscore (_) as ${contextType} in ${u.locationString(using context)}"
        )
        val updated = u.transformChildExpressions { case expr: Expression =>
          expr.transformExpression {
            case ref: ContextInputRef if !ref.dataType.isResolved =>
              val c = ContextInputRef(dataType = contextType, ref.span)
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
  private def findFunctionDef(f: Expression, knownArgs: List[FunctionArg] = Nil)(using
      context: Context
  ): Option[MethodSymbolInfo] =

    f match
      case fa: FunctionApply =>
        findFunctionDef(fa.base, fa.args)
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
                .newException(s"Ambiguous function call for ${i}", context.sourceLocationAt(i.span))
          }
      case d @ DotRef(qual, method: Identifier, _, _) =>
        val methodName = method.toTermName
        val qualType =
          if qual.dataType.isResolved then
            qual.dataType
          else
            DataType.AnyType

        val m: Option[MethodSymbolInfo] = lookupType(qualType.typeName, context)
          .map { sym =>
            // TODO: Resolve member with different arg types
            sym.symbolInfo.findMember(methodName).symbolInfo
          }
          .collect {
            case m: MethodSymbolInfo =>
              m
            case m: MultipleSymbolInfo =>
              // TODO resolve one of the function type
              throw StatusCode
                .SYNTAX_ERROR
                .newException(
                  s"Ambiguous function call for ${method}",
                  context.sourceLocationAt(d.span)
                )
          }
          .map { mi =>
            // Find the owner type of the method to build generic type mappings
            lookupType(mi.owner.dataType.typeName, context).map(_.symbolInfo) match
              case Some(ownerType: TypeSymbolInfo) =>
                val typeMap: Map[TypeName, DataType] = ownerType
                  .typeParams
                  .zipAll(qualType.typeParams, UnknownType, UnknownType)
                  .collect { case (t1: TypeParameter, t2) =>
                    t1.typeName -> t2
                  }
                  .toMap[TypeName, DataType]
                if typeMap.isEmpty then
                  mi
                else
                  val bounded = mi.bind(typeMap)
                  context.logTrace(s"Bind ${mi} with typeMap: ${typeMap} => ${bounded}")
                  bounded
              case _ =>
                mi
          }
          .map { m =>
            context.logTrace(s"Resolved method ${qualType}.${methodName} => ${m}")
            m
          }

        if m.isEmpty then
          context.logTrace(s"Failed to find function `${methodName}` for ${qual}:${qual.dataType}")

        m
      case _ =>
        trace(s"Failed to find function definition for ${f}")
        None
    end match
  end findFunctionDef

  /**
    * Evaluate FunctionApply nodes with the given function definition
    */
  private object resolveFunctionApply extends RewriteRule:
    override def apply(context: Context): PlanRewriter = { case q: Query =>
      q.transformUpExpressions { case f: FunctionApply =>
        resolveFunctionApply(f)(using context)
      }
    }

  end resolveFunctionApply

  private def inlineFunctionBody(
      base: Expression,
      m: MethodSymbolInfo,
      resolvedArgs: List[(TermName, Expression)]
  )(using context: Context): Expression =
    val newExpr: Expression =
      m.body match
        case Some(methodBody) =>
          methodBody.transformUpExpression {
            case th: This =>
              base match
                case d: DotRef =>
                  // Replace ${this} with ${qual} in DotRef(qual, method)
                  d.qualifier
                case _ =>
                  th
            case i: Identifier =>
              // Replace function arguments to the resolved args
              // TODO Handle nested function calls
              resolvedArgs.find(_._1 == i.toTermName) match
                case Some((_, expr)) =>
                  expr
                case None =>
                  i
          }
        case None =>
          // If the function definition has no body expression, return the original expression
          base
    context.logTrace(s"Resolving ${base} => ${newExpr}: ${m.ft.returnType}")
    if newExpr.resolved || !m.ft.returnType.isResolved then
      newExpr
    else
      newExpr.withDataType(m.ft.returnType)

  end inlineFunctionBody

  private def resolveFunctionApply(f: FunctionApply)(using context: Context) =
    findFunctionDef(f) match
      case Some(m: MethodSymbolInfo) =>
        val functionArgTypes = m.ft.args
        // Mapping function arguments aligned to the function definition
        var index        = 0
        val resolvedArgs = List.newBuilder[(TermName, Expression)]

        def mapArg(args: List[FunctionArg]): Unit =
          if !args.isEmpty then
            args.head match
              case FunctionArg(None, expr, _, _, span) =>
                if index >= functionArgTypes.length then
                  throw StatusCode
                    .SYNTAX_ERROR
                    .newException("Too many arguments", context.sourceLocationAt(span))
                val argType = functionArgTypes(index)
                argType.dataType match
                  case VarArgType(elemType) =>
                    index += 1
                    resolvedArgs += argType.name -> ListExpr(args, span)
                  // all args are consumed
                  case _ =>
                    index += 1
                    resolvedArgs += argType.name -> expr
                    mapArg(args.tail)
              case FunctionArg(Some(argName), expr, _, _, span) =>
                functionArgTypes.find(_.name == argName) match
                  case Some(argType) =>
                    argType.dataType match
                      case VarArgType(elemType) =>
                        resolvedArgs += argName -> ListExpr(args, expr.span)
                      // all args are consumed
                      case _ =>
                        resolvedArgs += argName -> expr
                        mapArg(args.tail)
                  case None =>
                    throw StatusCode
                      .SYNTAX_ERROR
                      .newException(
                        "Unknown argument name: ${argName}",
                        context.sourceLocationAt(span)
                      )

        // Resolve function arguments
        mapArg(f.args)

        // Resolve identifiers in the function body with the given function arguments
        // context.logWarn(s"Resolved function args: ${f.base}")
        val expr = inlineFunctionBody(f.base, m, resolvedArgs.result())

        f.window match
          case Some(w) =>
            WindowApply(expr.withDataType(m.ft.returnType), w, expr.span)
          case None =>
            expr.withDataType(m.ft.returnType)
      case _ =>
        f
    end match
  end resolveFunctionApply

  /**
    * Resolve `this` expression inside sql"...${this}..." interpolated strings, etc.
    */
  private object resolveInlineRef extends RewriteRule:
    private def resolveRef(using ctx: Context): PartialFunction[Expression, Expression] =
      case ref: DotRef =>
        findFunctionDef(ref) match
          case Some(m: MethodSymbolInfo) =>
            // Replace {this} -> {qual}
            inlineFunctionBody(ref, m, Nil)
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

  private object resolveNativeExpressions extends RewriteRule:

    private def findNativeFunction(context: Context, name: String): Option[NativeExpression] =
      context
        .findTermSymbolByName(name)
        .map(_.symbolInfo)
        .collect {
          case m: MethodSymbolInfo if m.body.isDefined =>
            m.body.get
        }
        .collect { case n: NativeExpression =>
          n
        }

    def apply(context: Context): PlanRewriter = { case q: TopLevelStatement =>
      q.transformUpExpressions {
        case id: Identifier if id.unresolved && id.nonEmpty =>
          // Replace the id with the referenced native expression
          val expr = findNativeFunction(context, id.fullName).getOrElse(id)
          expr
      }
    }

  end resolveNativeExpressions

  /**
    * For aggregation, which has no Aggregate plan node, a simple expression like
    * {{{(l_extendedprice * l_discount)}}} can be an aggregation expression if aggregation function
    * is applied like {{{(l_extendedprice * l_discount).sum}}}.
    */
  private object resolveNoGroupByAggregations extends RewriteRule:

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

    private def resolveAggregationExpr(using
        ctx: Context
    ): PartialFunction[Expression, Expression] =
      case e: ShouldExpr =>
        // do not resolve aggregation expr in test expressions
        e
      case d: DotRef =>
        val dd  = d.transformChildExpressions(resolveAggregationExpr)
        val nme = dd.name.toTermName
        aggregationFunctions
          .find(_.name == nme)
          .map(_.symbolInfo)
          .collect { case m: MethodSymbolInfo =>
            inlineFunctionBody(d, m, Nil)
          }
          .getOrElse(dd)
      case other =>
        other.transformChildExpressions(resolveAggregationExpr)
    end resolveAggregationExpr

    override def apply(context: Context): PlanRewriter = { case q: Query =>
      init(context)
      q.transformUp { case s: GeneralSelection =>
        s.transformChildExpressions(resolveAggregationExpr(using context))
      }
    }

  end resolveNoGroupByAggregations

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
    case f: FunctionApply if !f.resolved =>
      val resolvedF = f.transformChildExpressions(resolveExpression(inputRelationType, context))
      resolveFunctionApply(resolvedF)(using context)
    case ref: DotRef if !ref.resolved =>
      val resolvedRef = ref.transformChildExpressions(resolveExpression(inputRelationType, context))
      val refName     = Name.termName(resolvedRef.name.leafName)

      // Resolve types after following . (dot)
      resolvedRef.qualifier.dataType match
        case t: SchemaType =>
          context.logTrace(s"Find reference from ${t} -> ${resolvedRef.name}")
          t.columnTypes.find(_.name.name == resolvedRef.name.leafName) match
            case Some(col) =>
              context.logTrace(s"${t}.${col.name} is a column")
              ResolvedAttribute(
                Name.termName(resolvedRef.name.leafName),
                col.dataType,
                None,
                resolvedRef.span
              )
            case None =>
              // Lookup functions
              lookupType(t.typeName, context).map(_.symbolInfo) match
                case Some(tpe: TypeSymbolInfo) =>
                  tpe.declScope.lookupSymbol(refName).map(_.symbolInfo) match
                    case Some(method: MethodSymbolInfo) =>
                      context.logTrace(s"Resolved ${t}.${resolvedRef.name.fullName} as a function")
                      resolvedRef.copy(dataType = method.ft.returnType)
                    case _ =>
                      context.logDebug(s"${t}.${resolvedRef.name.fullName} is not found")
                      resolvedRef
                case _ =>
                  context.logDebug(s"${t}.${resolvedRef.name.fullName} is not found")
                  resolvedRef
        case refDataType =>
          // TODO Support multiple context-specific functions
          lookupType(refDataType.typeName, context).map(_.symbolInfo) match
            case Some(functionBaseType: TypeSymbolInfo) =>
              functionBaseType.declScope.lookupSymbol(refName).map(_.symbolInfo) match
                case Some(method: MethodSymbolInfo) =>
                  // Resolve generic types
                  val typeMap: Map[TypeName, DataType] =
                    if refDataType.typeParams.isEmpty then
                      Map.empty
                    else
                      functionBaseType
                        .typeParams
                        .zipAll(refDataType.typeParams, DataType.UnknownType, DataType.UnknownType)
                        .map { (a, b) =>
                          a.typeName -> b
                        }
                        .toMap

                  val boundedFunctionType = method.bind(typeMap)
                  // val newExpr = inlineFunctionBody(resolvedRef, boundedFunctionType, Nil)
                  // newExpr
                  val retType = boundedFunctionType.ft.returnType
                  resolvedRef.copy(dataType = retType)
                case _ =>
                  context.logTrace(s"Failed to resolve ${resolvedRef} as ${refDataType}")
                  resolvedRef
            case _ =>
              context.logTrace(s"TODO: ${refDataType.typeName} <- resolve ref: ${ref.fullName}")
              resolvedRef
      end match
    case i: Identifier if !i.resolved =>
      inputRelationType.find(x => x.name == i.fullName) match
        case Some(attr) =>
          val ri = i.toResolved(attr.dataType)
          context.logTrace(s"Resolved identifier: ${ri} from ${inputRelationType}")
          ri
        case None =>
          i
    case other if !other.resolved =>
      val expr = other.transformChildExpressions(resolveExpression(inputRelationType, context))
      expr
  end resolveExpression

  private def resolveExpression(
      expr: Expression,
      inputRelationType: RelationType,
      context: Context
  ): Expression = resolveExpression(inputRelationType, context).applyOrElse(
    expr,
    identity[Expression]
  )

end TypeResolver
