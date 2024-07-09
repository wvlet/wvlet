package com.treasuredata.flow.lang.compiler.analyzer

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.compiler.RewriteRule.PlanRewriter
import com.treasuredata.flow.lang.compiler.{
  CompilationUnit,
  Context,
  ModelSymbolInfo,
  Name,
  Phase,
  RewriteRule,
  Symbol,
  TypeSymbolInfo
}
import com.treasuredata.flow.lang.model.DataType.{NamedType, PrimitiveType, SchemaType}
import com.treasuredata.flow.lang.model.Type.FunctionType
import com.treasuredata.flow.lang.model.expr.*
import com.treasuredata.flow.lang.model.plan.*
import com.treasuredata.flow.lang.model.{DataType, RelationType}
import wvlet.log.LogSupport

object TypeResolver extends Phase("type-resolver") with LogSupport:

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    // resolve plans
    var resolvedPlan: LogicalPlan = TypeResolver.resolve(unit.unresolvedPlan, context)
    // resolve again to resolve unresolved relation types
    resolvedPlan = TypeResolver.resolve(resolvedPlan, context)
    unit.resolvedPlan = resolvedPlan
    unit

  def defaultRules: List[RewriteRule] =
    resolveLocalFileScan ::   // resolve local file scans for DuckDb
      resolveTableRef ::      // resolve table reference (model or schema) types
      resolveModelDef ::      // resolve ModelDef
      resolveTransformItem :: // resolve transform items prefixed with column name
      resolveSelectItem ::    // resolve select items (projected columns)
      resolveGroupingKey ::   // resolve Aggregate keys
      resolveRelation ::      // resolve expressions inside relation nodes
      resolveUnderscore ::    // resolve underscore in relation nodes
      resolveThis ::          // resolve `this` in type definitions
      // resolveFunctionBodyInTypeDef :: //
      resolveModelDef :: // Resolve models again to use the updated types
      Nil

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
            debug(newStmt.pp)

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

  end resolve

//  def resolveRelation(plan: LogicalPlan, context: Context): Relation =
//    val resolvedPlan = resolve(plan, context)
//    resolvedPlan match
//      case r: Relation =>
//        r
//      case _ =>
//        throw StatusCode.NOT_A_RELATION.newException(s"Not a relation:\n${resolvedPlan.pp}")

  /**
    * Resolve schema of local file scans (e.g., JSON, Parquet)
    */
  object resolveLocalFileScan extends RewriteRule:
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

  object resolveModelDef extends RewriteRule:
    override def apply(context: Context): PlanRewriter = { case m: ModelDef =>
      m.relationType match
        case r: RelationType if r.isResolved =>
          // given model type is already resolved
          m.symbol.symbolInfo(using context) match
            case t: ModelSymbolInfo =>
              t.dataType = r
              debug(s"Resolved ${t}")
            case other =>
          m
        case _ =>
          m
    }

  /**
    * Resolve TableRefs with concrete TableScans using the table schema in the catalog.
    */
  object resolveTableRef extends RewriteRule:
    private def lookup(qName: NameExpr, context: Context): Option[Symbol] =
      qName match
        case i: Identifier =>
          context.scope.lookupSymbol(Name.termName(i.leafName))
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
            val si = sym.symbolInfo(using context)
            si.tpe match
              case r: RelationType =>
                debug(s"resolved ${sym} ${ref.locationString(using context)}")
                RelScan(sym.name(using context), r, r.fields, ref.nodeLocation)
              case _ =>
                ref
          case None =>
            trace(s"Unresolved table ref: ${ref.name.fullName}")
            ref

  object resolveTransformItem extends RewriteRule:
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

  object resolveSelectItem extends RewriteRule:
    def apply(context: Context): PlanRewriter = { case p: Project =>
      val resolvedChild = p.child.transform(resolveRelation(context)).asInstanceOf[Relation]
      val resolvedColumns: Seq[Attribute] = p
        .selectItems
        .map {
          case s: SingleColumn =>
            s.copy(expr =
              s.expr.transformExpression(resolveExpression(p.child.relationType, context))
            )
          case x: Attribute =>
            x.transformExpression(resolveExpression(p.child.relationType, context))
              .asInstanceOf[Attribute]
        }
      Project(resolvedChild, resolvedColumns, p.nodeLocation)
    }

  object resolveGroupingKey extends RewriteRule:
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
    * Resolve expression in relation nodes
    */
  object resolveRelation extends RewriteRule:
    override def apply(context: Context): PlanRewriter = {
      case r: Relation => // Regular relation and Filter etc.
        r.transformExpressions(resolveExpression(r.inputRelationType, context))
    }

  /**
    * Resolve underscore (_) from the parent relation node
    */
  object resolveUnderscore extends RewriteRule:
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
  object resolveThis extends RewriteRule:
    override def apply(context: Context): PlanRewriter = { case t: TypeDef =>
      val enclosing = context.scope.lookupSymbol(Name.typeName(t.name.leafName))
      enclosing match
        case Some(s: Symbol) =>
          // TODO Handle nested definition (e.g., nested type definition)
          val r = s.symbolInfo(using context).dataType
          t.transformUpExpressions { case th: This =>
            val newThis = th.copy(dataType = r)
            trace(s"Resolved this: ${th} as ${newThis}")
            newThis
          }
        case _ =>
          t
    }

//
//  object resolveFunctionBodyInTypeDef extends RewriteRule:
//    override def apply(context: Context): PlanRewriter = { case td: TypeDef =>
//      val attrs = td
//        .elems
//        .collect { case v: TypeValDef =>
//          val name = v.name.fullName
//          context.scope.resolveType(v.tpe.fullName) match
//            case Some(resolvedType) =>
//              ResolvedAttribute(v.name, resolvedType, None, v.nodeLocation)
//            case None =>
//              UnresolvedAttribute(v.name, v.nodeLocation)
//        }
//
//      val newElems: List[TypeElem] = td
//        .elems
//        .map {
//          case f: FunctionDef =>
//            val retType = f
//              .retType
//              .map { t =>
//                context.scope.resolveType(t.typeName) match
//                  case Some(resolvedType) =>
//                    resolvedType
//                  case None =>
//                    t
//              }
//            // Function arguments that will be used inside the expression
//            val argAttrs = f
//              .args
//              .map { arg =>
//                ResolvedAttribute(arg.name, arg.dataType, None, arg.nodeLocation)
//              }
//            // vals and function args
//            val attrList = AttributeList(attrs ++ argAttrs)
//            // warn(s"resolve function body: ${f.expr} using ${attrList}")
//            val newF = f.copy(
//              retType = retType,
//              expr = f.expr.map(x => x.transformUpExpression(resolveExpression(attrList, context)))
//            )
//            newF
//          case other =>
//            other
//        }
//      td.copy(elems = newElems)
//    }
//
//  end resolveFunctionBodyInTypeDef
//
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
    case a: Attribute if !a.dataType.isResolved && !a.nameExpr.isEmpty =>
      val name = a.fullName
      debug(s"Find ${name} in ${inputRelationType.fields} ${a.locationString(using context)}")
      inputRelationType.find(x => x.name == name) match
        case Some(tpe) =>
          a // a.withDataType(tpe.dataType)
        case None =>
          a
    case ref: DotRef =>
      // Resolve types after following . (dot)
      ref.qualifier.dataType match
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
              t.defs.find(_.name.name == ref.name.fullName) match
                case Some(f: FunctionType) =>
                  trace(s"Resolved ${t}.${ref.name.fullName} as a function")
                  ref.copy(dataType = f.returnType)
                case _ =>
                  warn(s"${t}.${ref.name.fullName} is not found")
                  ref
//        case p: PrimitiveType =>
//          trace(s"Find reference from ${p} -> ${ref.name}")
//          context.scope.findType(p.typeName) match
//            case Some(pt: SchemaType) =>
//              pt.defs.find(_.name == ref.fullName) match
//                case Some(m: FunctionType) =>
//                  // TODO Handling context-specific methods
//                  trace(s"Resolved ${p}.${ref.name.fullName} as a primitive function")
//                  ref.copy(dataType = m.returnType)
//                case _ =>
//                  trace(s"Failed to resolve ${p}.${ref.name.fullName}")
//                  ref
//            case _ =>
//              trace(s"Failed to resolve ${p}.${ref.name.fullName}")
//              ref
        case other =>
          // trace(s"TODO: resolve ref: ${ref.fullName} as ${other.getClass}")
          ref
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
//    case other: Expression if !other.dataType.isResolved =>
//      trace(s"TODO: resolve expression: ${other} using ${knownAttributes}")
//      other

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
