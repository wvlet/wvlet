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
package wvlet.lang.engine.plan

import wvlet.lang.engine.exec.*
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*

/**
  * Runs scan pipelines (a splittable scan with the filters, projections, and hash join probes over
  * it) as [[MorselOp]]s, so that parts of the scan are read and processed concurrently. Operators
  * above the pipelines still run on the calling thread. Below operators this pass doesn't know, the
  * plan is left as is.
  */
object Parallelizer:

  def apply(op: Operator, runner: TaskRunner): Operator =
    if runner.parallelism <= 1 then
      op
    else
      rewrite(op, runner)

  /**
    * A scan and the operators over it that can run on parts of the scan independently:
    * `rebuild(input)` makes a copy of those operators over another input. `prepare` runs on the
    * calling thread before parts start (hash join builds), and `close` releases what the copies
    * share
    */
  private case class Pipeline(
      scan: ScanOp,
      rebuild: Operator => Operator,
      prepare: List[() => Unit],
      close: List[() => Unit]
  )

  /** Joins whose probe side can be split: each probe row's output depends on that row only */
  private def probeSplittable(kind: JoinKind): Boolean =
    kind match
      case JoinKind.Inner | JoinKind.Left | JoinKind.Semi | JoinKind.Anti | JoinKind.Cross =>
        true
      case _ =>
        false

  private def pipeline(op: Operator, runner: TaskRunner): Option[Pipeline] =
    op match
      case s: ScanOp if s.source.canSplit =>
        Some(Pipeline(s, x => x, Nil, Nil))
      case f: FilterOp =>
        pipeline(f.child, runner).map(p =>
          p.copy(rebuild = x => FilterOp(p.rebuild(x), f.cond, f.keep))
        )
      case pr: ProjectOp =>
        pipeline(pr.child, runner).map(p =>
          p.copy(rebuild = x => ProjectOp(p.rebuild(x), pr.exprs, pr.schema))
        )
      case j: HashJoinOp if probeSplittable(j.kind) =>
        pipeline(j.left, runner).map { p =>
          // One join over the (itself parallelized) build input, whose build every copy shares
          val template = HashJoinOp(
            j.left,
            rewrite(j.right, runner),
            j.leftKeys,
            j.rightKeys,
            j.kind,
            j.residual,
            j.schema
          )
          Pipeline(
            p.scan,
            x => template.withLeft(p.rebuild(x)),
            p.prepare :+ (() => template.joinBuild.ensureBuilt()),
            p.close :+ (() => template.right.close())
          )
        }
      case _ =>
        None

  private def rewrite(op: Operator, runner: TaskRunner): Operator =
    def r(x: Operator): Operator = rewrite(x, runner)
    pipeline(op, runner) match
      case Some(p) =>
        MorselOp(p.scan, p.rebuild, op.schema, runner, p.prepare, p.close)
      case None =>
        op match
          case a: HashAggOp if TwoPhaseAgg.supports(a) =>
            pipeline(a.child, runner) match
              case Some(p) =>
                TwoPhaseAgg.plan(a, p.scan, p.rebuild, runner, p.prepare, p.close)
              case None =>
                HashAggOp(r(a.child), a.keys, a.aggs, a.schema)
          case f: FilterOp =>
            FilterOp(r(f.child), f.cond, f.keep)
          case p: ProjectOp =>
            ProjectOp(r(p.child), p.exprs, p.schema)
          case l: LimitOp =>
            LimitOp(r(l.child), l.limit, l.offset)
          case s: SortOp =>
            SortOp(r(s.child), s.keys, s.limit)
          case a: HashAggOp =>
            HashAggOp(r(a.child), a.keys, a.aggs, a.schema)
          case d: DistinctOp =>
            DistinctOp(r(d.child))
          case u: UnionAllOp =>
            UnionAllOp(u.inputs.map(r), u.schema)
          case j: HashJoinOp =>
            HashJoinOp(r(j.left), r(j.right), j.leftKeys, j.rightKeys, j.kind, j.residual, j.schema)
          case j: NestedLoopJoinOp =>
            NestedLoopJoinOp(r(j.left), r(j.right), j.cond, j.kind, j.schema)
          case s: SetOpOp =>
            SetOpOp(r(s.left), r(s.right), s.isIntersect, s.all, s.schema)
          case w: WindowOp =>
            WindowOp(r(w.child), w.calls, w.schema)
          case other =>
            other
    end match

  end rewrite

end Parallelizer

/**
  * Aggregation in two phases: each part of a scan pipeline is aggregated on its own (partial
  * aggregates), then the partial results are combined by the grouping keys. Supported for
  * aggregates whose partial results combine with another aggregate: count (summed), sum, min, max,
  * and avg (as a sum and a count)
  */
private[plan] object TwoPhaseAgg:
  private def lower(c: AggCall): String = c.name.toLowerCase

  private def isCount(c: AggCall): Boolean =
    lower(c) match
      case "count" | "count_star" | "count_if" | "countif" =>
        true
      case _ =>
        false

  def supports(a: HashAggOp): Boolean = a
    .aggs
    .forall { c =>
      !c.distinct && (isCount(c) || Set("sum", "min", "max", "avg", "mean").contains(lower(c)))
    }

  def plan(
      a: HashAggOp,
      scan: ScanOp,
      rebuild: Operator => Operator,
      runner: TaskRunner,
      prepare: List[() => Unit],
      release: List[() => Unit]
  ): Operator =
    val nk = a.keys.size
    // Partial aggregates, and for each original aggregate the partial columns it reads
    val partials             = IndexedSeq.newBuilder[AggCall]
    val sources              = IndexedSeq.newBuilder[IndexedSeq[Int]]
    var next                 = nk
    def add(c: AggCall): Int =
      partials += c
      next += 1
      next - 1
    a.aggs
      .foreach { c =>
        lower(c) match
          case "avg" | "mean" =>
            // Decimals sum exactly; other numbers sum as doubles, as Avg does (no overflow)
            val sumType =
              c.args.headOption.map(_.ptype) match
                case Some(d: PType.Decimal) =>
                  Aggregates.resultType("sum", Seq(d))
                case _ =>
                  PType.Float64
            val s = add(AggCall("sum", c.args, false, c.filter, sumType))
            val n = add(AggCall("count", c.args, false, c.filter, PType.Int64))
            sources += IndexedSeq(s, n)
          case _ =>
            sources += IndexedSeq(add(c))
      }
    val partialAggs   = partials.result()
    val srcs          = sources.result()
    val partialSchema = Schema(
      a.schema.fields.take(nk) ++
        partialAggs.zipWithIndex.map((c, i) => Field(s"__partial_${i}", c.ptype))
    )
    val partial = MorselOp(
      scan,
      x => HashAggOp(rebuild(x), a.keys, partialAggs, partialSchema),
      partialSchema,
      runner,
      prepare,
      release
    )
    def col(i: Int): VExpr = ColRef(i, partialSchema(i).ptype, partialSchema(i).name)
    // Combine partial results by key
    val finalAggs = partialAggs
      .zipWithIndex
      .map { (c, j) =>
        val in = col(nk + j)
        if isCount(c) then
          AggCall("sum", Seq(in), false, None, PType.Int64)
        else
          AggCall(lower(c), Seq(in), false, None, c.ptype)
      }
    val finalSchema = Schema(
      a.schema.fields.take(nk) ++
        finalAggs.zipWithIndex.map((c, i) => Field(s"__final_${i}", c.ptype))
    )
    val combined = HashAggOp(partial, (0 until nk).map(col), finalAggs, finalSchema)
    // Restore the original output: keys, then each aggregate in its original type
    def fcol(i: Int): VExpr           = ColRef(i, finalSchema(i).ptype, finalSchema(i).name)
    def as(e: VExpr, t: PType): VExpr =
      if e.ptype == t then
        e
      else
        CastExpr(e, t)
    val outputs = (0 until nk).map(fcol) ++
      a.aggs
        .zip(srcs)
        .map { (c, src) =>
          src match
            case IndexedSeq(s, n) =>
              Arith(
                ArithOp.Div,
                as(fcol(s), PType.Float64),
                as(fcol(n), PType.Float64),
                PType.Float64
              )
            case IndexedSeq(i) =>
              as(fcol(i), c.ptype)
        }
    ProjectOp(combined, outputs.toIndexedSeq, a.schema)

  end plan

end TwoPhaseAgg
