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
package wvlet.lang.engine.runtime

import wvlet.lang.engine.catalog.TableSource
import wvlet.lang.engine.exec.AggCall
import wvlet.lang.engine.exec.FrameSpec
import wvlet.lang.engine.exec.JoinKind
import wvlet.lang.engine.exec.Operator
import wvlet.lang.engine.exec.SortKey
import wvlet.lang.engine.exec.WindowCall
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.VExprs
import wvlet.lang.engine.vector.Batch
import wvlet.lang.engine.vector.Field
import wvlet.lang.engine.vector.PType
import wvlet.lang.engine.vector.Schema

import scala.collection.mutable

/** The identity of a column in a runtime plan, unique within a query */
case class ColumnId(value: Int):
  override def toString: String = s"#${value}"

/** An output column of a runtime plan node */
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

/** An aggregate call of a [[RuntimePlan.Aggregate]], over the ids of its input */
case class AggregateCall(
    name: String,
    args: Seq[BExpr],
    distinct: Boolean,
    filter: Option[BExpr],
    ptype: PType
):
  /** The expressions the call reads */
  def exprs: Seq[BExpr] = args ++ filter

  /** The executable call over an input whose column of id `i` is at `position(i)` */
  def lower(position: Array[Int]): AggCall = AggCall(
    name,
    args.map(_.lower(position)),
    distinct,
    filter.map(_.lower(position)),
    ptype
  )

  override def toString: String =
    val d =
      if distinct then
        "distinct "
      else
        ""
    s"${name}(${d}${args.mkString(", ")})${filter.map(f => s" filter ${f}").getOrElse("")}"

object AggregateCall:
  /** Bind an aggregate call over input positions to the ids of the columns at those positions */
  def of(c: AggCall, input: IndexedSeq[Column]): AggregateCall = AggregateCall(
    c.name,
    c.args.map(BExpr.of(_, input)),
    c.distinct,
    c.filter.map(BExpr.of(_, input)),
    c.ptype
  )

/**
  * A condition of a [[RuntimePlan.InnerJoins]]. `literalCounts` maps the bound signatures of the
  * condition and its `and`/`or` sub-conditions to the number of literals each compares an
  * expression to, when it is `x = literal` (1) or `x in (n literals)` (n) in the query, for
  * selectivity: a bound constant may also be a folded expression or a scalar subquery result, which
  * are estimated with the default selectivity. Rewrites that keep sub-conditions (FactorOr) keep
  * their counts this way
  */
case class JoinCondition(cond: BExpr, literalCounts: Map[String, Int] = Map.empty):
  /** The number of literals the condition compares an expression to, if it is such a comparison */
  def literals: Option[Int] = literalCounts.get(cond.expr.sig)

  /** A condition over a sub-condition of this one, keeping the literal counts */
  def withCond(e: VExpr): JoinCondition = copy(cond = BExpr(e))

  override def toString: String = cond.toString

/** A sort key of a [[RuntimePlan.Sort]] */
case class OrderKey(expr: BExpr, ascending: Boolean, nullsFirst: Boolean):
  /** The executable key over an input whose column of id `i` is at `position(i)` */
  def lower(position: Array[Int]): SortKey = SortKey(expr.lower(position), ascending, nullsFirst)

object OrderKey:
  /** Bind a sort key over input positions to the ids of the columns at those positions */
  def of(k: SortKey, input: IndexedSeq[Column]): OrderKey = OrderKey(
    BExpr.of(k.expr, input),
    k.ascending,
    k.nullsFirst
  )

/** A window function call of a [[RuntimePlan.Window]], over the ids of its input */
case class WindowFunction(
    name: String,
    args: Seq[BExpr],
    partitionBy: Seq[BExpr],
    orderBy: Seq[OrderKey],
    frame: Option[FrameSpec],
    ignoreNulls: Boolean,
    ptype: PType
):
  /** The expressions the call reads */
  def exprs: Seq[BExpr] = args ++ partitionBy ++ orderBy.map(_.expr)

  /** The columns the call partitions by, as plain column references */
  def partitionColumns: Set[ColumnId] =
    partitionBy
      .collect { case BExpr(c: ColRef) =>
        ColumnId(c.index)
      }
      .toSet

  /** The executable call over an input whose column of id `i` is at `position(i)` */
  def lower(position: Array[Int]): WindowCall = WindowCall(
    name,
    args.map(_.lower(position)),
    partitionBy.map(_.lower(position)),
    orderBy.map(_.lower(position)),
    frame,
    ignoreNulls,
    ptype
  )

  override def toString: String =
    val over =
      Seq(
        Option.when(partitionBy.nonEmpty)(s"partition by ${partitionBy.mkString(", ")}"),
        Option.when(orderBy.nonEmpty)(
          s"order by ${orderBy
              .map { k =>
                if k.ascending then
                  k.expr.toString
                else
                  s"${k.expr} desc"
              }
              .mkString(", ")}"
        ),
        frame.map(_.toString)
      ).flatten
    s"${name}(${args.mkString(", ")}) over (${over.mkString(" ")})"

end WindowFunction

object WindowFunction:
  /** Bind a window call over input positions to the ids of the columns at those positions */
  def of(c: WindowCall, input: IndexedSeq[Column]): WindowFunction = WindowFunction(
    c.name,
    c.args.map(BExpr.of(_, input)),
    c.partitionBy.map(BExpr.of(_, input)),
    c.orderBy.map(OrderKey.of(_, input)),
    c.frame,
    c.ignoreNulls,
    c.ptype
  )

/**
  * The engine's runtime plan: relations over identified columns ([[ColumnId]]) and bound
  * expressions ([[BExpr]]), between the compiler's relations and the physical operators.
  * [[RuntimeBinder]] builds it and [[Lowering]] turns it into operators. Relations the binder
  * doesn't cover yet are [[RuntimePlan.Legacy]] leaves, already planned by the recursive planner.
  *
  * Passes over the plan match its cases without a wildcard default, so that a new case fails the
  * build (non-exhaustive matches are errors in wvlet-engine) until every pass handles it.
  */
enum RuntimePlan:
  /** Read a table source; `columns` are the source columns, possibly renamed or requalified */
  case Scan(source: TableSource, columns: IndexedSeq[Column])

  /** Rows evaluated at plan time */
  case Values(columns: IndexedSeq[Column], rows: Batch)

  /**
    * Rows of `child` for which `cond` is true. `literalCounts` keeps the literal counts of its
    * conjuncts by bound signature, as [[JoinCondition]] does, for when they join an inner join tree
    */
  case Filter(child: RuntimePlan, cond: BExpr, literalCounts: Map[String, Int] = Map.empty)

  /** One output column per expression */
  case Project(child: RuntimePlan, exprs: IndexedSeq[BExpr], columns: IndexedSeq[Column])

  /** Sort by `keys`; with a limit only the top rows are kept */
  case Sort(child: RuntimePlan, keys: IndexedSeq[OrderKey], limit: Option[Long])

  /** Skip `offset` rows, then return at most `limit` rows */
  case Limit(child: RuntimePlan, limit: Long, offset: Long)

  /**
    * Group by `keys` and compute `calls` per group (a global aggregation without keys): one output
    * column per key, then one per call
    */
  case Aggregate(
      child: RuntimePlan,
      keys: IndexedSeq[BExpr],
      calls: IndexedSeq[AggregateCall],
      columns: IndexedSeq[Column]
  )

  /**
    * A join whose output is the columns of `left`, then those of `right`. With keys it is a hash
    * join on `leftKeys` (over `left`) = `rightKeys` (over `right`), building on `right`, or on
    * `left` when `buildLeft` is set; without keys it is a nested loop join. The residual reads the
    * columns of both inputs
    */
  case Join(
      left: RuntimePlan,
      right: RuntimePlan,
      kind: JoinKind,
      leftKeys: IndexedSeq[BExpr],
      rightKeys: IndexedSeq[BExpr],
      residual: Option[BExpr],
      buildLeft: Boolean
  )

  /**
    * Inner joins of `inputs` in no particular order: the rows of their product for which every
    * condition is true, with the columns of the inputs in order. [[ReorderJoins]] replaces it with
    * a tree of [[Join]]s, which outputs the same columns in its join order
    */
  case InnerJoins(inputs: IndexedSeq[RuntimePlan], conds: IndexedSeq[JoinCondition])

  /**
    * The rows of every input, in order (UNION ALL): column i of each input is cast to the type of
    * output column i, the common type of the inputs' columns at that position
    */
  case Union(inputs: IndexedSeq[RuntimePlan], columns: IndexedSeq[Column])

  /**
    * INTERSECT (`intersect`) or EXCEPT of two inputs, comparing whole rows after casting them to
    * the output types; with `all`, bag semantics, otherwise distinct rows
    */
  case SetOp(
      left: RuntimePlan,
      right: RuntimePlan,
      intersect: Boolean,
      all: Boolean,
      columns: IndexedSeq[Column]
  )

  /** The distinct rows of `child` */
  case Distinct(child: RuntimePlan)

  /**
    * Window functions over `child`: its columns, then one column per call (`columns`), computed
    * over the call's partition of the input rows
    */
  case Window(child: RuntimePlan, calls: IndexedSeq[WindowFunction], columns: IndexedSeq[Column])

  /** A relation planned by the recursive planner, whose operator schema gives the columns */
  case Legacy(op: Operator, columns: IndexedSeq[Column])

  /**
    * A read of a CTE that several relations read, computed once: `columns` are the output of its
    * body, by position, under ids of this reader
    */
  case CTERef(cte: SharedCTE, columns: IndexedSeq[Column])

  /** The output columns, in order */
  def output: IndexedSeq[Column] =
    this match
      case Scan(_, columns) =>
        columns
      case Values(columns, _) =>
        columns
      case Filter(child, _, _) =>
        child.output
      case Project(_, _, columns) =>
        columns
      case Sort(child, _, _) =>
        child.output
      case Limit(child, _, _) =>
        child.output
      case Join(left, right, _, _, _, _, _) =>
        left.output ++ right.output
      case Aggregate(_, _, _, columns) =>
        columns
      case InnerJoins(inputs, _) =>
        inputs.flatMap(_.output)
      case Union(_, columns) =>
        columns
      case SetOp(_, _, _, _, columns) =>
        columns
      case Distinct(child) =>
        child.output
      case Window(child, _, columns) =>
        child.output ++ columns
      case Legacy(_, columns) =>
        columns
      case CTERef(_, columns) =>
        columns

  def children: Seq[RuntimePlan] =
    this match
      case Scan(_, _) | Values(_, _) | Legacy(_, _) | CTERef(_, _) =>
        Nil
      case Filter(child, _, _) =>
        Seq(child)
      case Project(child, _, _) =>
        Seq(child)
      case Sort(child, _, _) =>
        Seq(child)
      case Limit(child, _, _) =>
        Seq(child)
      case Join(left, right, _, _, _, _, _) =>
        Seq(left, right)
      case Aggregate(child, _, _, _) =>
        Seq(child)
      case InnerJoins(inputs, _) =>
        inputs
      case Union(inputs, _) =>
        inputs
      case SetOp(left, right, _, _, _) =>
        Seq(left, right)
      case Distinct(child) =>
        Seq(child)
      case Window(child, _, _) =>
        Seq(child)

  /**
    * The node over new children, in the order of [[children]]. Returns this node when every child
    * is the same instance
    */
  def withChildren(cs: Seq[RuntimePlan]): RuntimePlan =
    val old = children
    require(cs.size == old.size, s"${cs.size} children for ${old.size}")
    if cs.lazyZip(old).forall(_ eq _) then
      this
    else
      this match
        case Scan(_, _) | Values(_, _) | Legacy(_, _) | CTERef(_, _) =>
          this
        case f: Filter =>
          f.copy(child = cs(0))
        case p: Project =>
          p.copy(child = cs(0))
        case s: Sort =>
          s.copy(child = cs(0))
        case l: Limit =>
          l.copy(child = cs(0))
        case j: Join =>
          j.copy(left = cs(0), right = cs(1))
        case a: Aggregate =>
          a.copy(child = cs(0))
        case j: InnerJoins =>
          j.copy(inputs = cs.toIndexedSeq)
        case u: Union =>
          u.copy(inputs = cs.toIndexedSeq)
        case s: SetOp =>
          s.copy(left = cs(0), right = cs(1))
        case d: Distinct =>
          d.copy(child = cs(0))
        case w: Window =>
          w.copy(child = cs(0))

  end withChildren

  /** The output as an operator schema */
  def schema: Schema = Schema(output.map(_.toField))

  /**
    * The plan as text, source first, like `Operator.explain`: each line is a node with its output
    * columns (`name#id`). `↓` marks the node consuming the line above; inputs after the first are
    * indented, and `↙` marks the node consuming them
    */
  def explain: String =
    val lines                                  = Seq.newBuilder[String]
    def loop(p: RuntimePlan, depth: Int): Unit =
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
      case Filter(_, cond, _) =>
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
      case Aggregate(_, keys, calls, _) =>
        s"Aggregate(keys=[${keys.mkString(", ")}], ${calls.mkString(", ")})"
      case InnerJoins(_, conds) =>
        s"InnerJoins(${conds.mkString(", ")})"
      case Union(_, _) =>
        "Union"
      case SetOp(_, _, intersect, all, _) =>
        val op =
          if intersect then
            "Intersect"
          else
            "Except"
        val a =
          if all then
            "(all)"
          else
            ""
        s"${op}${a}"
      case Distinct(_) =>
        "Distinct"
      case Window(_, calls, _) =>
        s"Window(${calls.mkString(", ")})"
      case Legacy(op, _) =>
        s"Legacy[${op.name}]"
      case CTERef(cte, _) =>
        s"CTERef[${cte.name}]"

end RuntimePlan

/**
  * A CTE that several relations read, computed once for all of them. Its body is a bound runtime
  * plan; the planner lowers it once every reader is planned, keeping only the columns the readers
  * read
  */
trait SharedCTE:
  /** The CTE name */
  def name: String

  /** The bound and optimized body, whose output the readers read */
  def body: RuntimePlan

  /**
    * An operator reading the result, with the output columns of the body under `schema`. `filter`
    * is the condition the reader applies right above it, over the body's column ids: when every
    * reader has one, the planner pushes what they share into the body ([[PushDownFilters.shared]])
    */
  def reader(schema: Schema, filter: Option[JoinCondition] = None): Operator
