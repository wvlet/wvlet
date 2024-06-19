package com.treasuredata.flow.lang.model.sql

import com.treasuredata.flow.lang.connector.{DBContext, QueryScope}
import com.treasuredata.flow.lang.model.DataType
import com.treasuredata.flow.lang.model.plan.{
  Distinct,
  Filter,
  JoinType,
  Limit,
  Selection,
  SetOperation,
  Sort
}
import com.treasuredata.flow.lang.model.sql.SqlExpr.IString

/**
  * An abstraction of expressions for mapping AST to SQL expressions
  *
  * @param str
  * @tparam A
  */
abstract class SqlExpr:
  override def toString: String = toSQL
  def toSQL: String             = ???
  def stripMargin: SqlExpr      = RawSqlExpr(this.toSQL.stripMargin)

  def asString(using ctx: DBContext): IString = ctx.withSelf(this).IString

case class RawSqlExpr(sql: String) extends SqlExpr:
  override def toSQL: String = sql

class SqlExprList(exprs: Seq[SqlExpr], separator: String = ", ") extends SqlExpr:
  override def toSQL: String = exprs.map(_.toSQL).mkString(separator)

extension (sc: StringContext)
  def sql(args: Any*): SqlExpr =
    new SqlExpr:
      override def toSQL: String =
        sc.parts
          .zipAll(args, "", "")
          .foldLeft("") { case (acc, (part, arg)) =>
            acc + part + arg
          }

object SqlExpr:

  def join(exprs: SqlExpr*): SqlExpr = SqlExprList(
    exprs.filterNot(_.toSQL.isEmpty),
    separator = " "
  )

  class IRelation(using ctx: DBContext) extends SqlExpr:
    export ctx.self

    private def needToWrapSQL: Boolean =
      ctx.plan match
        case s: Selection =>
          true
        case s: SetOperation =>
          true
        case l: Limit =>
          true
        case f: Filter =>
          true
        case s: Sort =>
          true
        case d: Distinct =>
          true
        case _ =>
          false

    override def toSQL: String = ???

    def filter(cond: SqlExpr) = sql"""select * from ${self} where ${cond}"""

    def limit(n: SqlExpr)         = sql"select * from ${self} limit ${n}"
    def select(cols: SqlExprList) = sql"""select ${cols} from ${self}"""

    def join(joinType: JoinType, right: IRelation, on: SqlExpr) =
      sql"""select * from ${self} ${joinType} join ${right} on ${on}"""

    def groupBy(keys: SqlExprList, attrs: SqlExprList) =
      sql"""select ${attrs} from ${self} group by ${keys}"""

  end IRelation

  trait IAttribute(using ctx: DBContext) extends SqlExpr:
    export ctx.self

  end IAttribute

  abstract trait IString(using ctx: DBContext) extends SqlExpr:
    export ctx.self

    // override def toSQL: String = self.toSQL

    def toInt     = sql"cast(${self} as bigint)"
    def toLong    = sql"cast(${self} as bigint)"
    def toFloat   = sql"cast(${self} as float)"
    def toDouble  = sql"cast(${self} as double)"
    def toBoolean = sql"cast(${self} as boolean)"

    def orElse(s: SqlExpr) = sql"coalesce(${self}, ${s})"

    def +(s: SqlExpr) = sql"${self} || ${s}"

    def `=`(s: SqlExpr) = sql"${self} = ${s}"
    def !=(s: SqlExpr)  = sql"${self} != ${s}"
    def <(s: SqlExpr)   = sql"${self} < ${s}"
    def <=(s: SqlExpr)  = sql"${self} <= ${s}"
    def >(s: SqlExpr)   = sql"${self} > ${s}"
    def >=(s: SqlExpr)  = sql"${self} >= ${s}"

    def length = sql"length(${self})"
    def substring(start: SqlExpr): SqlExpr
    def substring(start: SqlExpr, end: SqlExpr): SqlExpr
    def regexpContains(pattern: SqlExpr): SqlExpr
  end IString

  abstract trait IBoolean(using ctx: DBContext) extends SqlExpr:
    export ctx.self

    def toStr    = sql"if(${self}, 'true', 'false')"
    def toInt    = sql"if(${self}, 1, 0)"
    def toLong   = sql"if(${self}, 1, 0)"
    def toFloat  = sql"if(${self}, 1.0, 0.0)"
    def toDouble = sql"if(${self}, 1.0, 0.0)"

    // !x
    def unary_!        = sql"not ${self}"
    def ==(x: SqlExpr) = sql"${self} = ${x}"
    def !=(x: SqlExpr) = sql"${self} != ${x}"

    def and(x: SqlExpr) = sql"${self} and ${x}"
    def or(x: SqlExpr)  = sql"${self} or ${x}"

    def &&(x: SqlExpr) = and(x)
    def ||(x: SqlExpr) = or(x)

  sealed trait INumeric extends SqlExpr

  abstract trait IInt(using ctx: DBContext) extends INumeric:
    export ctx.self

    def toBoolean = sql"cast(${self} as boolean)"
    def toStr     = sql"cast(${self} as varchar)"
    def toLong    = sql"cast(${self} as bigint)"
    def toFloat   = sql"cast(${self} as float)"
    def toDouble  = sql"cast(${self} as double)"

    def +(x: SqlExpr) = sql"${self} + ${x}"
    def -(x: SqlExpr) = sql"${self} - ${x}"
    def *(x: SqlExpr) = sql"${self} * ${x}"
    def /(x: SqlExpr) = sql"${self} / ${x}"
    def %(x: SqlExpr) = sql"${self} % ${x}"

  abstract trait ILong(using ctx: DBContext) extends INumeric:
    export ctx.self

    def toBoolean     = sql"cast(${self} as boolean)"
    def toStr         = sql"cast(${self} as varchar)"
    def toInt         = sql"cast(${self} as integer)"
    def toFloat       = sql"cast(${self} as float)"
    def toDouble      = sql"cast(${self} as double)"
    def +(x: SqlExpr) = sql"${self} + ${x}"
    def -(x: SqlExpr) = sql"${self} - ${x}"
    def *(x: SqlExpr) = sql"${self} * ${x}"
    def /(x: SqlExpr) = sql"${self} / ${x}"
    def %(x: SqlExpr) = sql"${self} % ${x}"

  abstract trait IFloat(using ctx: DBContext) extends INumeric:
    export ctx.self

    def toBoolean     = sql"cast(${self} as boolean)"
    def toStr         = sql"cast(${self} as varchar)"
    def toInt         = sql"cast(${self} as integer)"
    def toLong        = sql"cast(${self} as bigint)"
    def toDouble      = sql"cast(${self} as double)"
    def +(x: SqlExpr) = sql"${self} + ${x}"
    def -(x: SqlExpr) = sql"${self} - ${x}"
    def *(x: SqlExpr) = sql"${self} * ${x}"
    def /(x: SqlExpr) = sql"${self} / ${x}"
    def %(x: SqlExpr) = sql"${self} % ${x}"

  abstract trait IDouble(using ctx: DBContext) extends INumeric:
    export ctx.self

    def toBoolean = sql"cast(${self} as boolean)"
    def toStr     = sql"cast(${self} as varchar)"
    def toInt     = sql"cast(${self} as integer)"
    def toLong    = sql"cast(${self} as bigint)"
    def toFloat   = sql"cast(${self} as float)"

    def +(x: SqlExpr) = sql"${self} + ${x}"
    def -(x: SqlExpr) = sql"${self} - ${x}"
    def *(x: SqlExpr) = sql"${self} * ${x}"
    def /(x: SqlExpr) = sql"${self} / ${x}"
    def %(x: SqlExpr) = sql"${self} % ${x}"
