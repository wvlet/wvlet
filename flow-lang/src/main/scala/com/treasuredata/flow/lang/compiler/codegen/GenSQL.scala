package com.treasuredata.flow.lang.compiler.codegen

import com.treasuredata.flow.lang.compiler.{
  BoundedSymbolInfo,
  CompilationUnit,
  Context,
  ModelSymbolInfo,
  Name,
  Phase,
  Symbol,
  TermName
}
import com.treasuredata.flow.lang.model.expr.*
import com.treasuredata.flow.lang.model.plan.*
import com.treasuredata.flow.lang.model.plan.JoinType.*

import scala.collection.immutable.ListMap

case class GeneratedSQL(sql: String, plan: Relation)

object GenSQL extends Phase("generate-sql"):
  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    // Generate SQL from the resolved plan
    // generateSQL(unit.resolvedPlan, context)
    // Attach the generated SQL to the CompilationUnit
    unit

  def generateSQL(q: Query, ctx: Context): GeneratedSQL =
    val expanded = expand(q, ctx)
    // val sql      = SQLGenerator.toSQL(expanded)
    val sql = printRelation(expanded, ctx, 0)
    trace(s"[plan]\n${expanded.pp}\n[SQL]\n${sql}")
    GeneratedSQL(sql, expanded)

  def expand(q: Relation, ctx: Context): Relation =
    // expand referenced models

    def transformExpr(r: Relation, ctx: Context): Relation = r
      .transformUpExpressions { case i: Identifier =>
        val nme = Name.termName(i.leafName)
        ctx.scope.lookupSymbol(nme) match
          case Some(sym) =>
            sym.symbolInfo(using ctx) match
              case b: BoundedSymbolInfo =>
                // Replace to the bounded expression
                b.expr
              case _ =>
                i
          case None =>
            i
      }
      .asInstanceOf[Relation]

    def transformModelScan(m: ModelScan, sym: Symbol): Relation =
      sym.tree match
        case md: ModelDef =>
          val newCtx = ctx.newContext(sym)
          // TODO add model args to the context sco
          m.modelArgs
            .zipWithIndex
            .foreach { (arg, index) =>
              val argName: TermName = arg.name.getOrElse(md.params(index).name)
              val argValue          = arg.value

              // Register function arguments to the current scope
              val argSym    = Symbol(ctx.global.newSymbolId)
              given Context = ctx
              argSym.symbolInfo = BoundedSymbolInfo(argSym, argName, argValue.dataType, argValue)
              newCtx.scope.add(argName, argSym)
              argSym
            }

          // Replace function argument references in the model body with the actual expressions
          val modelBody = transformExpr(md.child, newCtx)
          expand(modelBody, newCtx)
        case other =>
          warn(s"Unknown model tree for ${m.name}: ${other}")
          m

    // TODO expand expressions and inline macros as well
    q.transformUp {
        case m: ModelScan =>
          lookupType(m.name, ctx) match
            case Some(sym) =>
              transformModelScan(m, sym)
            case None =>
              warn(s"unknown model: ${m.name}")
              m
        case q: Query =>
          q.child
      }
      .asInstanceOf[Relation]

  end expand

  private def lookupType(name: Name, ctx: Context): Option[Symbol] = ctx
    .scope
    .lookupSymbol(name)
    .orElse {
      var result: Option[Symbol] = None
      for
        c <- ctx.global.getAllContexts
        if result.isEmpty
      do
        result = c.compilationUnit.knownSymbols.find(_.name(using c) == name)
      result
    }

  def printRelation(r: Relation, ctx: Context, nestingLevel: Int): String =
    def indent(s: String): String =
      if nestingLevel == 0 then
        s
      else
        val str = s.split("\n").map(x => s"  ${x}").mkString("\n")
        s"\n${str}"

    def toAggregateSelect(agg: AggregateSelect, r: Relation): AggregateSelect =
      def collectFilter(plan: Relation): (List[Filter], Relation) =
        plan match
          case f: Filter =>
            val (filters, child) = collectFilter(f.inputRelation)
            (f :: filters, child)
          case other =>
            (Nil, other)
      end collectFilter

      r match
        case f: Filter =>
          val (filters, lastNode) = collectFilter(f)
          lastNode match
            case a: Aggregate =>
              agg.copy(child = a.child, groupingKeys = a.groupingKeys, having = filters)
            case other =>
              agg.copy(child = other, filters = filters)
        case p: Project =>
          agg
        case a: Aggregate =>
          agg.copy(child = a.child, groupingKeys = a.groupingKeys)
        case _ =>
          agg

    end toAggregateSelect

    def printAggregate(a: Aggregate): String =
      // Aggregation without any projection (select)
      val agg = toAggregateSelect(
        AggregateSelect(a.child, Nil, a.groupingKeys, Nil, Nil, a.nodeLocation),
        a.child
      )
      val s           = Seq.newBuilder[String]
      val selectItems = Seq.newBuilder[String]
      selectItems ++= agg.groupingKeys.map(x => printExpression(x, ctx))
      selectItems ++=
        a.inputRelationType
          .fields
          .map { f =>
            // TODO: This should generate a nested relation, but use arbitrary(expr) for efficiency
            s"arbitrary(${f.name})"
          }

      s += s"select ${selectItems.result().mkString(", ")}"
      s += s"from ${printRelation(agg.child, ctx, nestingLevel + 1)}"
      s += s"group by ${agg.groupingKeys.map(x => printExpression(x, ctx)).mkString(", ")}"
      s.result().mkString("\n")

    r match
      case p: Project =>
        // pull-up filter nodes to build where clause
        // pull-up an Aggregate node to build group by clause
        val agg = toAggregateSelect(
          AggregateSelect(p.child, p.selectItems.toList, Nil, Nil, Nil, p.nodeLocation),
          p.child
        )
        val selectItems = agg.selectItems.map(x => printExpression(x, ctx)).mkString(", ")
        val s           = Seq.newBuilder[String]
        s += s"select ${selectItems}"
        s += s"from ${printRelation(agg.child, ctx, nestingLevel + 1)}"
        if agg.filters.nonEmpty then
          val filterExpr = Expression.concatWithAnd(agg.filters.map(x => x.filterExpr))
          s += s"where ${printExpression(filterExpr, ctx)}"
        if agg.groupingKeys.nonEmpty then
          s += s"group by ${agg.groupingKeys.map(x => printExpression(x, ctx)).mkString(", ")}"
        if agg.having.nonEmpty then
          s += s"having ${agg.having.map(x => printExpression(x.filterExpr, ctx)).mkString(", ")}"

        indent(s"(${s.result().mkString("\n")})")
      case a: Aggregate =>
        indent(s"(${printAggregate(a)})")
      case j: Join =>
        def printRel(r: Relation): String =
          r match
            case t: TableScan =>
              t.name.name
            case other =>
              printRelation(other, ctx, nestingLevel + 1)

        val l = printRel(j.left)
        val r = printRel(j.right)
        val c =
          j.cond match
            case NoJoinCriteria =>
              ""
            case NaturalJoin(_) =>
              ""
            case JoinUsing(columns, _) =>
              s" using(${columns.map(_.fullName).mkString(", ")})"
            case ResolvedJoinUsing(columns, _) =>
              s" using(${columns.map(_.fullName).mkString(", ")})"
            case JoinOn(expr, _) =>
              s" on ${printExpression(expr, ctx)}"
            case JoinOnEq(keys, _) =>
              s" on ${printExpression(Expression.concatWithEq(keys), ctx)}"
        j.joinType match
          case InnerJoin =>
            s"${l} join ${r}${c}"
          case LeftOuterJoin =>
            s"${l} left join ${r}${c}"
          case RightOuterJoin =>
            s"${l} right join ${r}${c}"
          case FullOuterJoin =>
            s"${l} full outer join ${r}${c}"
          case CrossJoin =>
            s"${l} cross join p${r}${c}"
          case ImplicitJoin =>
            s"${l}, ${r}${c}"
      case j: JSONFileScan =>
        indent(s"(select * from '${j.path}')")
      case f: Filter =>
        f.child match
          case a: Aggregate =>
            val body = List(
              s"${printAggregate(a)}",
              s"having ${printExpression(f.filterExpr, ctx)}"
            ).mkString("\n")
            indent(s"(${body})")
          case _ =>
            indent(
              s"""(select * from ${printRelation(
                  f.inputRelation,
                  ctx,
                  nestingLevel + 1
                )}\nwhere ${printExpression(f.filterExpr, ctx)})"""
            )
      case a: AliasedRelation =>
        indent(
          s"${printRelation(a.inputRelation, ctx, nestingLevel + 1)} as ${printExpression(a.alias, ctx)}"
        )
      case p: ParenthesizedRelation =>
        indent(s"(${printRelation(p.child, ctx, nestingLevel + 1)})")
      case t: TestRelation =>
        printRelation(t.inputRelation, ctx, nestingLevel + 1)
      case l: Limit =>
        val input = printRelation(l.inputRelation, ctx, nestingLevel + 1)
        indent(s"""(select * from ${input}\nlimit ${l.limit.stringValue})""")
      case q: Query =>
        printRelation(q.body, ctx, nestingLevel)
      case s: Sort =>
        val input = printRelation(s.inputRelation, ctx, nestingLevel + 1)
        indent(
          s"""(select * from ${input}\norder by ${s
              .orderBy
              .map(e => printExpression(e, ctx))
              .mkString(", ")})"""
        )
      case t: Transform =>
        val transformItems = t.transformItems.map(x => printExpression(x, ctx)).mkString(", ")
        indent(
          s"""(select ${transformItems}, * from ${printRelation(
              t.inputRelation,
              ctx,
              nestingLevel + 1
            )})"""
        )
      case t: TableRef =>
        indent(s"""(select * from ${t.name.fullName})""")
      case t: TableScan =>
        indent(s"""(select * from ${t.name})""")
      case t: ParquetFileScan =>
        indent(s"""(select * from '${t.path}')""")
      case s: Show if s.showType == ShowType.models =>
        val models: Seq[ListMap[String, Any]] = ctx
          .global
          .getAllContexts
          .flatMap { ctx =>
            ctx
              .compilationUnit
              .knownSymbols
              .map(_.symbolInfo(using ctx))
              .collect { case m: ModelSymbolInfo =>
                m
              }
              .map { m =>
                val e = ListMap.newBuilder[String, Any]
                e += "name" -> s"'${m.name.name}'"

                // model argument types
                m.symbol.tree match
                  case md: ModelDef if md.params.nonEmpty =>
                    val args = md.params.map(arg => s"${arg.name}:${arg.dataType.typeDescription}")
                    e += "args" -> args.mkString("'", ", ", "'")
                  case _ =>
                    e += "args" -> "cast(null as varchar)"

                // package_name
                if !m.owner.isNoSymbol && m.owner != ctx.global.defs.RootPackage then
                  e += "package_name" -> s"'${m.owner.name(using ctx).name}'"
                else
                  e += "package_name" -> "cast(null as varchar)"

                e.result()
              }
          }

        val modelValues = models
          .map { x =>
            List(x("name"), x.getOrElse("args", ""), x.getOrElse("package_name", "")).mkString(",")
          }
          .map(x => s"(${x})")
        if modelValues.isEmpty then
          indent(
            "(select null::varchar as name, null::varchar as args, null::varchar as package_name limit 0)"
          )
        else
          indent(
            s"(select * from values ${indent(modelValues.mkString(", "))} as __models(name, args, package_name))"
          )
      case other =>
        warn(s"unknown relation type: ${other}")
        other.toString

    end match

  end printRelation

  def printExpression(expression: Expression, context: Context): String =
    expression match
      case g: UnresolvedGroupingKey =>
        printExpression(g.child, context)
      case f: FunctionApply =>
        val base = printExpression(f.base, context)
        val args = f.args.map(x => printExpression(x, context)).mkString(", ")
        s"${base}(${args})"
      case f: FunctionArg =>
        // TODO handle arg name mapping
        printExpression(f.value, context)
      case b: BinaryExpression =>
        s"${printExpression(b.left, context)} ${b.operatorName} ${printExpression(b.right, context)}"
      case s: StringPart =>
        s.stringValue
      case s: StringLiteral =>
        s"'${s.stringValue}'"
      case l: Literal =>
        l.stringValue
      case i: Identifier =>
        i.strExpr
      case s: SortItem =>
        s"${printExpression(s.sortKey, context)}${s.ordering.map(x => s" ${x.expr}").getOrElse("")}"
      case s: SingleColumn =>
        if s.nameExpr.isEmpty then
          printExpression(s.expr, context)
        else
          s"${printExpression(s.expr, context)} as ${s.nameExpr.fullName}"
      case a: Attribute =>
        a.fullName
      case p: ParenthesizedExpression =>
        s"(${printExpression(p.child, context)})"
      case i: InterpolatedString =>
        i.parts
          .map { e =>
            printExpression(e, context)
          }
          .mkString
      case s: SubQueryExpression =>
        s"(${printRelation(s.query, context, 0)})"
      case i: IfExpr =>
        s"if(${printExpression(i.cond, context)}, ${printExpression(i.onTrue, context)}, ${printExpression(i.onFalse, context)})"
      case i: Wildcard =>
        i.strExpr
      case n: Not =>
        s"not ${printExpression(n.child, context)}"
      case l: ListExpr =>
        l.exprs.map(x => printExpression(x, context)).mkString(", ")
      case d @ DotRef(qual: Expression, name: NameExpr, _, _) =>
        s"${printExpression(qual, context)}.${printExpression(name, context)}"
      case other =>
        warn(s"unknown expression type: ${other}")
        other.toString

end GenSQL
