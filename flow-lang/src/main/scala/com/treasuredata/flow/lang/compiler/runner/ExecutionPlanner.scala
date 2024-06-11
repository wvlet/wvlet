package com.treasuredata.flow.lang.compiler.runner

import com.treasuredata.flow.lang.StatusCode
import com.treasuredata.flow.lang.compiler.CompilationUnit
import com.treasuredata.flow.lang.connector.DBContext
import com.treasuredata.flow.lang.model.DataType
import com.treasuredata.flow.lang.model.expr.{BinaryExprType, Expression, *}
import com.treasuredata.flow.lang.model.plan.*
import com.treasuredata.flow.lang.model.sql.{RawSqlExpr, SQLGenerator, SqlExpr, SqlExprList, sql}
import wvlet.log.LogSupport

class ExecutionPlanner(using compilationUnit: CompilationUnit, ctx: DBContext) extends LogSupport:
  def plan(l: LogicalPlan): SqlExpr =
    l match
      case p: PackageDef =>
        statements(p.statements)
      case other =>
        unknown(other)

  inline private def unknown(l: LogicalPlan): Nothing =
    throw StatusCode.SYNTAX_ERROR.newException(s"Unknown logical plan: ${l} (${l.getClass.getName})", l.sourceLocation)

  inline private def unknown(e: Expression): Nothing =
    throw StatusCode.SYNTAX_ERROR.newException(s"Unknown expression: ${e} (${e.getClass.getName})", e.sourceLocation)

  def statements(statements: Seq[LogicalPlan]): SqlExprList =
    val exprs: List[SqlExpr] = statements
      .map: stmt =>
        statement(stmt)
      .toList
    SqlExprList(exprs)

  def statement(stmt: LogicalPlan): SqlExpr =
    stmt match
      case q: Query =>
        relation(q.body)
      case _ =>
        unknown(stmt)

  def relation(r: Relation): SqlExpr =
    r match
      case p: Project =>
        SqlExpr.join(
          sql"select",
          SqlExprList(p.selectItems.map(attribute)),
          relation(p.child)
        )
      case e: EmptyRelation =>
        sql""
      case other => unknown(other)

  def attribute(a: Attribute): SqlExpr =
    a match
      case s: SingleColumn =>
        if s.name == "?" then expression(s.expr)
        else sql"${expression(s.expr)} as ${s.fullName}"
      case a: AttributeRef =>
        sql"${a.name}"
      case _ =>
        unknown(a)

  def expression(e: Expression): SqlExpr =
    e match
      case a: ArithmeticBinaryExpr =>
        val left  = expression(a.left)
        val right = expression(a.right)
        a.left.dataType match
          case DataType.StringType =>
            a.exprType match
              case BinaryExprType.Add =>
                left.asString.+(right)
              case _ =>
                unknown(a)
          case _ =>
            sql"${left} ${a.exprType.symbol} ${right}"
      case s: StringLiteral =>
        sql"'${s.stringValue}'"
      case l: Literal =>
        sql"${l.stringValue}"
      case other: Expression =>
        unknown(other)

//      case f: FileScan =>
//        fileScan(f)
//      case j: JSONFileScan =>
//        jsonFileScan(j)
