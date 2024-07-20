package com.treasuredata.flow.lang.compiler.codegen

import com.treasuredata.flow.lang.compiler.{
  BoundedSymbolInfo,
  CompilationUnit,
  Context,
  Name,
  Phase,
  Symbol,
  TermName
}
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
    val sql = printRelation(expanded, ctx, 0)
    trace(s"[plan]\n${expanded.pp}\n[SQL]\n${sql}")
    sql

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

    r match
      case p: Project =>
        val selectItems = p.selectItems.map(x => printExpression(x, ctx)).mkString(", ")
        indent(
          s"""(select ${selectItems} from ${printRelation(
              p.inputRelation,
              ctx,
              nestingLevel + 1
            )})"""
        )
      case j: JSONFileScan =>
        indent(s"(select * from '${j.path}')")
      case f: Filter =>
        indent(
          s"""(select * from ${printRelation(
              f.inputRelation,
              ctx,
              nestingLevel + 1
            )} where ${printExpression(f.filterExpr, ctx)})"""
        )
      case t: TestRelation =>
        printRelation(t.inputRelation, ctx, nestingLevel + 1)
      case l: Limit =>
        val input = printRelation(l.inputRelation, ctx, nestingLevel + 1)
        indent(s"""(select * from ${input} limit ${l.limit.stringValue})""")
      case q: Query =>
        printRelation(q.body, ctx, nestingLevel)
      case s: Sort =>
        val input = printRelation(s.inputRelation, ctx, nestingLevel + 1)
        indent(
          s"""(select * from ${input} order by ${s
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
      case other =>
        warn(s"unknown relation type: ${other}")
        other.toString

    end match

  end printRelation

  def printExpression(expression: Expression, context: Context): String =
    expression match
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
          s"${printExpression(s.expr, context)} AS ${s.nameExpr.fullName}"
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
      case other =>
        warn(s"unknown expression type: ${other}")
        other.toString

end GenSQL
