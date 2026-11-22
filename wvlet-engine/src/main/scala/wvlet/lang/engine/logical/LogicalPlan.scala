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
package wvlet.lang.engine.logical

import wvlet.lang.engine.catalog.TableSource
import wvlet.lang.engine.exec.JoinKind
import wvlet.lang.engine.exec.Operator
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.VExprs
import wvlet.lang.engine.vector.Batch
import wvlet.lang.engine.vector.Field
import wvlet.lang.engine.vector.PType
import wvlet.lang.engine.vector.Schema

import scala.collection.mutable

/** The identity of a column in a logical plan, unique within a query */
case class ColumnId(value: Int):
  override def toString: String = s"#${value}"

/** An output column of a logical plan node */
case class Column(id: ColumnId, name: String, ptype: PType, qualifiers: List[String] = Nil):
  def toField: Field            = Field(name, ptype, qualifiers)
  override def toString: String = s"${name}${id}"

/**
  * A bound expression: an executable [[VExpr]] whose column references ([[ColRef]]) hold
  * [[ColumnId]] values instead of input positions, so that it stays valid when the columns of its
  * input move. [[Lowering]] maps the ids back to positions.
  */
case class BExpr(expr: VExpr):
  def ptype: PType = expr.ptype

  /** The columns the expression reads */
  def refs: Set[ColumnId] =
    val acc = mutable.BitSet.empty
    VExprs.collectRefs(expr, acc)
    acc.iterator.map(ColumnId(_)).toSet

  /** The column references, with their types, for validation */
  def colRefs: Seq[ColRef] =
    val b                    = Seq.newBuilder[ColRef]
    def loop(e: VExpr): Unit =
      e match
        case c: ColRef =>
          b += c
        case other =>
          other.children.foreach(loop)
    loop(expr)
    b.result()

  /** The executable expression over an input whose column of id `i` is at `position(i)` */
  def lower(position: Array[Int]): VExpr = VExprs.remap(expr, position)

  override def toString: String = expr.sig

object BExpr:
  /** Bind an expression over input positions to the ids of the columns at those positions */
  def of(e: VExpr, input: IndexedSeq[Column]): BExpr = BExpr(
    VExprs.remap(e, input.map(_.id.value).toArray)
  )

  /** A reference to a column */
  def ref(c: Column): BExpr = BExpr(ColRef(c.id.value, c.ptype, c.name))

/** A sort key of a logical [[LogicalPlan.Sort]] */
case class OrderKey(expr: BExpr, ascending: Boolean, nullsFirst: Boolean)

/**
  * The engine's logical plan: relations over identified columns ([[ColumnId]]) and bound
  * expressions ([[BExpr]]), between the compiler's relations and the physical operators.
  * [[LogicalBinder]] builds it and [[Lowering]] turns it into operators. Relations the binder
  * doesn't cover yet are [[LogicalPlan.Legacy]] leaves, already planned by the recursive planner.
  *
  * Passes over the plan match its cases without a wildcard default, so that a new case fails the
  * build (non-exhaustive matches are errors in wvlet-engine) until every pass handles it.
  */
enum LogicalPlan:
  /** Read a table source; `columns` are the source columns, possibly renamed or requalified */
  case Scan(source: TableSource, columns: IndexedSeq[Column])

  /** Rows evaluated at plan time */
  case Values(columns: IndexedSeq[Column], rows: Batch)

  /** Rows of `child` for which `cond` is true */
  case Filter(child: LogicalPlan, cond: BExpr)

  /** One output column per expression */
  case Project(child: LogicalPlan, exprs: IndexedSeq[BExpr], columns: IndexedSeq[Column])

  /** Sort by `keys`; with a limit only the top rows are kept */
  case Sort(child: LogicalPlan, keys: IndexedSeq[OrderKey], limit: Option[Long])

  /** Skip `offset` rows, then return at most `limit` rows */
  case Limit(child: LogicalPlan, limit: Long, offset: Long)

  /**
    * A join whose output is the columns of `left`, then those of `right`. With keys it is a hash
    * join on `leftKeys` (over `left`) = `rightKeys` (over `right`), building on `right`, or on
    * `left` when `buildLeft` is set; without keys it is a nested loop join. The residual reads the
    * columns of both inputs
    */
  case Join(
      left: LogicalPlan,
      right: LogicalPlan,
      kind: JoinKind,
      leftKeys: IndexedSeq[BExpr],
      rightKeys: IndexedSeq[BExpr],
      residual: Option[BExpr],
      buildLeft: Boolean
  )

  /** A relation planned by the recursive planner, whose operator schema gives the columns */
  case Legacy(op: Operator, columns: IndexedSeq[Column])

  /** The output columns, in order */
  def output: IndexedSeq[Column] =
    this match
      case Scan(_, columns) =>
        columns
      case Values(columns, _) =>
        columns
      case Filter(child, _) =>
        child.output
      case Project(_, _, columns) =>
        columns
      case Sort(child, _, _) =>
        child.output
      case Limit(child, _, _) =>
        child.output
      case Join(left, right, _, _, _, _, _) =>
        left.output ++ right.output
      case Legacy(_, columns) =>
        columns

  def children: Seq[LogicalPlan] =
    this match
      case Scan(_, _) | Values(_, _) | Legacy(_, _) =>
        Nil
      case Filter(child, _) =>
        Seq(child)
      case Project(child, _, _) =>
        Seq(child)
      case Sort(child, _, _) =>
        Seq(child)
      case Limit(child, _, _) =>
        Seq(child)
      case Join(left, right, _, _, _, _, _) =>
        Seq(left, right)

  /** The output as an operator schema */
  def schema: Schema = Schema(output.map(_.toField))

  /**
    * The plan as text, source first, like `Operator.explain`: each line is a node with its output
    * columns (`name#id`). `↓` marks the node consuming the line above; inputs after the first are
    * indented, and `↙` marks the node consuming them
    */
  def explain: String =
    val lines                                  = Seq.newBuilder[String]
    def loop(p: LogicalPlan, depth: Int): Unit =
      p.children.headOption.foreach(loop(_, depth))
      p.children.drop(1).foreach(loop(_, depth + 1))
      val marker =
        if p.children.size > 1 then
          "↙ "
        else if p.children.nonEmpty then
          "↓ "
        else
          ""
      lines += s"${"  " * depth}${marker}${p.nodeText} -> [${p.output.mkString(", ")}]"
    loop(this, 0)
    lines.result().mkString("\n")

  private def nodeText: String =
    this match
      case Scan(source, _) =>
        s"Scan[${source.name}]"
      case Values(_, rows) =>
        s"Values(rows=${rows.size})"
      case Filter(_, cond) =>
        s"Filter(${cond})"
      case Project(_, exprs, _) =>
        s"Project(${exprs.mkString(", ")})"
      case Sort(_, keys, limit) =>
        val k = keys.map { k =>
          val dir =
            if k.ascending then
              "asc"
            else
              "desc"
          s"${k.expr} ${dir}"
        }
        s"Sort(${k.mkString(", ")}${limit.map(n => s", limit=${n}").getOrElse("")})"
      case Limit(_, limit, offset) =>
        s"Limit(${limit}, offset=${offset})"
      case Join(_, _, kind, lk, rk, residual, buildLeft) =>
        val keys = lk.zip(rk).map((l, r) => s"${l} = ${r}")
        val args =
          keys ++ residual.map(r => s"residual=${r}") ++ Option.when(buildLeft)("build=left")
        s"Join[${kind}](${args.mkString(", ")})"
      case Legacy(op, _) =>
        s"Legacy[${op.name}]"

end LogicalPlan
