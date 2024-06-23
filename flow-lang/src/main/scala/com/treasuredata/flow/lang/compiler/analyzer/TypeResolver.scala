package com.treasuredata.flow.lang.compiler.analyzer

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.compiler.RewriteRule.PlanRewriter
import com.treasuredata.flow.lang.compiler.{CompilationUnit, Context, Phase, RewriteRule}
import com.treasuredata.flow.lang.model.DataType.{NamedType, SchemaType}
import com.treasuredata.flow.lang.model.expr.{Attribute, AttributeList, Expression}
import com.treasuredata.flow.lang.model.plan.*
import com.treasuredata.flow.lang.model.{DataType, RelationType}
import wvlet.log.LogSupport

object TypeResolver extends Phase("type-resolver") with LogSupport:

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    trace(context.scope.getAllTypes.map(t => s"[${t._1}]: ${t._2}").mkString("\n"))

    // resolve plans
    var resolvedPlan: LogicalPlan = TypeResolver.resolve(unit.unresolvedPlan, context)
    // resolve again to resolve unresolved relation types
    resolvedPlan = TypeResolver.resolve(resolvedPlan, context)
    unit.resolvedPlan = resolvedPlan
    unit

  def defaultRules: List[RewriteRule] =
    resolveLocalFileScan :: resolveTableRef :: resolveRelation :: resolveProjectedColumns ::
      resolveModelDef :: Nil

  def resolve(plan: LogicalPlan, context: Context): LogicalPlan = RewriteRule
    .rewrite(plan, defaultRules, context)

  def resolveRelation(plan: LogicalPlan, context: Context): Relation =
    val resolvedPlan = resolve(plan, context)
    resolvedPlan match
      case r: Relation =>
        r
      case _ =>
        throw StatusCode.NOT_A_RELATION.newException(s"Not a relation:\n${resolvedPlan.pp}")

  /**
    * Resolve schema of local file scans (e.g., JSON, Parquet)
    */
  object resolveLocalFileScan extends RewriteRule:
    override def apply(context: Context): PlanRewriter =
      case r: FileScan if r.path.endsWith(".json") =>
        val file             = context.getDataFile(r.path)
        val jsonRelationType = JSONAnalyzer.analyzeJSONFile(file)
        val cols = jsonRelationType
          .typeParams
          .collect { case n: NamedType =>
            n
          }
        JSONFileScan(file, jsonRelationType, cols, r.nodeLocation)
      case r: FileScan if r.path.endsWith(".parquet") =>
        val file                = context.dataFilePath(r.path)
        val parquetRelationType = ParquetAnalyzer.guessSchema(file)
        val cols = parquetRelationType
          .typeParams
          .collect { case n: NamedType =>
            n
          }
        ParquetFileScan(file, parquetRelationType, cols, r.nodeLocation)

  /**
    * Resolve TableRefs with concrete TableScans using the table schema in the catalog.
    */
  object resolveTableRef extends RewriteRule:
    override def apply(context: Context): PlanRewriter =
      case ref: TableRef =>
        context.scope.findType(ref.name.fullName) match
          case Some(tpe: RelationType) =>
            // warn(s"Resolved table ref: ${ref} -> ${tpe.typeName}")
            context.scope.resolveType(tpe.typeName) match
              case Some(schema: SchemaType) =>
                context.scope.getTableDef(ref.name) match
                  case Some(tbl) =>
                    TableScan(ref.name.fullName, tpe, schema.columnTypes, ref.nodeLocation)
                  case None =>
                    RelScan(ref.name.fullName, tpe, schema.columnTypes, ref.nodeLocation)
              case other =>
                // warn(s"Unresolved table ref: ${ref} -> ${other}")
                ref
          case _ =>
            trace(s"Unresolved table ref: ${ref}")
            ref

  /**
    * Resolve expression in relation nodes
    */
  object resolveRelation extends RewriteRule:
    override def apply(context: Context): PlanRewriter = {
      case q: Query =>
        q.copy(body = resolveRelation(q.body, context))
      case r: Relation => // Regular relation and Filter etc.
        r.transformUpExpressions { case x: Expression =>
          resolveExpression(x, r.inputAttributeList, context)
        }
    }

  /**
    * Resolve select items (projected attributes) in Project nodes
    */
  object resolveProjectedColumns extends RewriteRule:
    def apply(context: Context): PlanRewriter = { case p: Project =>
      val resolvedChild = resolveRelation(p.child, context)
      val resolvedColumns: Seq[Attribute] = resolveAttributes(
        p.selectItems,
        resolvedChild.outputAttributeList,
        context
      )
      Project(resolvedChild, resolvedColumns, p.nodeLocation)
    }

  /**
    * Resolve the given list of attribute types using known attributes from the child plan nodes as
    * hints
    * @param attributes
    * @param knownAttributes
    * @param context
    * @return
    */
  private def resolveAttributes(
      attributes: Seq[Attribute],
      knownAttributes: AttributeList,
      context: Context
  ): Seq[Attribute] = attributes.map { a =>
    val resolvedExpr = resolveExpression(a, knownAttributes, context)
    a
  }

  /**
    * Resolve the given expression type using the input attributes from child plan nodes
    * @param expr
    * @param knownAttributes
    */
  private def resolveExpression(
      expr: Expression,
      knownAttributes: AttributeList,
      context: Context
  ): Expression =
    trace(s"resolve ${expr} using ${knownAttributes}")
    expr

  object resolveModelDef extends RewriteRule:
    override def apply(context: Context): PlanRewriter = { case m: ModelDef =>
      context.scope.resolveType(m.relationType.typeName) match
        case Some(r: RelationType) =>
          m.copy(relationType = r)
        case _ =>
          m
    }
