package com.treasuredata.flow.lang.compiler.codegen

import com.treasuredata.flow.lang.compiler.{CompilationUnit, Context, Name, Phase, Symbol}
import com.treasuredata.flow.lang.model.expr.*
import com.treasuredata.flow.lang.model.plan.*
import com.treasuredata.flow.lang.model.sql.SQLGenerator

import scala.annotation.tailrec

object GenSQL extends Phase("generate-sql"):
  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    // Generate SQL from the resolved plan
    // generateSQL(unit.resolvedPlan, context)
    // Attach the generated SQL to the CompilationUnit
    unit

  def generateSQL(q: Query, ctx: Context): String =
    val expanded = expand(q, ctx)
    // val sql      = SQLGenerator.toSQL(expanded)
    val sql = printRelation(expanded, ctx)
    trace(s"[plan]\n${expanded.pp}\n[SQL]\n${sql}")
    sql

  def expand(q: Query, ctx: Context): Query =
    // expand referenced models
    // TODO expand expressions and inline macros as well
    q.transformUp { case m: ModelScan =>
        lookupType(m.name, ctx) match
          case Some(sym) =>
            sym.symbolInfo(using ctx).plan match
              case m: ModelDef =>
                m.child
              case _ =>
                m
          case _ =>
            m
      }
      .asInstanceOf[Query]

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

  def printRelation(r: Relation, ctx: Context): String =
    r match
      case p: Project =>
        val selectItems = p.selectItems.map(x => printExpression(x, ctx)).mkString(", ")
        s"""(select ${selectItems} from ${printRelation(p.inputRelation, ctx)})"""
      case j: JSONFileScan =>
        s"(select * from '${j.path}')"
      case f: Filter =>
        s"""(select * from ${printRelation(f.inputRelation, ctx)}
           |where ${printExpression(f.filterExpr, ctx)})""".stripMargin
      case t: TestRelation =>
        printRelation(t.inputRelation, ctx)
      case l: Limit =>
        val input = printRelation(l.inputRelation, ctx)
        s"""(select * from ${input}
           |limit ${l.limit.stringValue})""".stripMargin
      case q: Query =>
        printRelation(q.body, ctx)
      case s: Sort =>
        val input = printRelation(s.inputRelation, ctx)
        s"""(select * from ${input}
           |order by ${s.orderBy.map(e => printExpression(e, ctx)).mkString(", ")})""".stripMargin
      case other =>
        warn(s"unknown relaation type: ${other}")
        other.toString

  def printExpression(expression: Expression, context: Context): String =
    expression match
      case b: BinaryExpression =>
        s"${printExpression(b.left, context)} ${b.operatorName} ${printExpression(b.right, context)}"
      case l: Literal =>
        l.stringValue
      case i: Identifier =>
        i.strExpr
      case s: SortItem =>
        s"${printExpression(s.sortKey, context)} ${s.ordering.map(_.expr).getOrElse("")}"
      case s: SingleColumn =>
        if s.nameExpr.isEmpty then
          printExpression(s.expr, context)
        else
          s"${printExpression(s.expr, context)} AS ${s.nameExpr.fullName}"
      case a: Attribute =>
        a.fullName
      case other =>
        warn(s"unknown expression type: ${other}")
        other.toString

end GenSQL
