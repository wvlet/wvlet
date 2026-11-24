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

  /**
    * With `aboveBreakers`, the output of a grouped aggregation is a source whose parts are its
    * batches, so that the filters, projections, and join probes above it run as morsels too. With
    * `parallelPartitioning`, partitioned aggregations route their input rows to partitions as tasks
    */
  def apply(
      op: Operator,
      runner: TaskRunner,
      aboveBreakers: Boolean = true,
      parallelPartitioning: Boolean = true
  ): Operator =
    if runner.parallelism <= 1 then
      op
    else
      given AboveBreakers = AboveBreakers(aboveBreakers, parallelPartitioning)
      rewrite(op, runner)

  /**
    * Whether pipelines start at the outputs of grouped aggregations ([[BreakerSource]]), and
    * whether partitioned aggregations route their input in parallel
    */
  private case class AboveBreakers(enabled: Boolean, parallelPartitioning: Boolean)

  /**
    * A scan and the operators over it that can run on parts of the scan independently:
    * `rebuild(input)` makes a copy of those operators over another input. `tail`, when the pipeline
    * has right / full joins, makes the copy that runs once after every part over an empty input,
    * emitting the build rows no part matched. `prepare` runs on the calling thread before parts
    * start (hash join builds), and `close` releases what the copies share
    */
  private case class Pipeline(
      scan: ScanOp,
      rebuild: Operator => Operator,
      tail: Option[Operator => Operator],
      prepare: List[() => Unit],
      close: List[() => Unit],
      hasWork: Boolean = false
  ):
    /**
      * Add an operator over the pipeline, the same over the parts and the tail. `work`: whether it
      * computes something (not only selects or renames columns)
      */
    def map(f: Operator => Operator, work: Boolean = true): Pipeline = copy(
      rebuild = x => f(rebuild(x)),
      tail = tail.map(t => x => f(t(x))),
      hasWork = hasWork || work
    )

  private def isOuterBuild(kind: JoinKind): Boolean = kind.tracksBuildMatches

  private def pipeline(op: Operator, runner: TaskRunner)(using
      breakers: AboveBreakers
  ): Option[Pipeline] =
    op match
      case s: ScanOp if s.source.canSplit =>
        Some(Pipeline(s, x => x, None, Nil, Nil))
      case a: HashAggOp if breakers.enabled && a.keys.nonEmpty =>
        // The aggregation runs on the calling thread before any part starts (it parallelizes
        // its own work); its output batches are the parts
        val source = BreakerSource(aggregate(a, runner), "Aggregate")
        Some(Pipeline(ScanOp(source, a.schema, None, None), x => x, None, Nil, List(source.close)))
      case f: FilterOp =>
        pipeline(f.child, runner).map(
          _.map(x => FilterOp(x, f.cond, f.keep).withLateGather(f.lateGather))
        )
      case pr: ProjectOp =>
        pipeline(pr.child, runner).map(
          _.map(
            x => ProjectOp(x, pr.exprs, pr.schema),
            // Selecting, renaming, or adding constant columns (grouping sets) is not worth a task
            work =
              !pr
                .exprs
                .forall {
                  case _: ColRef | _: Const =>
                    true
                  case _ =>
                    false
                }
          )
        )
      case j: HashJoinOp =>
        pipeline(j.left, runner).map { p =>
          // One join over the (itself parallelized) build input, whose build every copy shares
          val template = HashJoinOp(
            j.left,
            rewrite(j.right, runner),
            j.leftKeys,
            j.rightKeys,
            j.kind,
            j.residual,
            j.schema,
            buildRunner = runner,
            runtimeFilters = j.runtimeFilters
          ).withLateGather(j.lateGather).withDirectIndex(j.directIndex)
          val shared = p.copy(
            prepare = p.prepare :+ (() => template.joinBuild.ensureBuilt()),
            close = p.close :+ (() => template.right.close())
          )
          if isOuterBuild(j.kind) then
            // Parts mark matched build rows; the tail emits the rest once every part is done
            shared.copy(
              rebuild = x => template.withLeft(p.rebuild(x), OuterMode.Part),
              tail = Some(x => template.withLeft(p.tail.getOrElse(p.rebuild)(x), OuterMode.Tail))
            )
          else
            shared.map(x => template.withLeft(x))
        }
      case j: NestedLoopJoinOp if j.kind == JoinKind.Cross && j.cond.isEmpty =>
        // A cross join (e.g. with a one-row aggregate): the right rows are computed once before
        // the parts start, and every part joins its rows with them
        pipeline(j.left, runner).map { p =>
          val right       = rewrite(j.right, runner)
          var rows: Batch = null
          // Not a child: parts must not close the shared right input, released once below
          def shared: Operator =
            new IteratorOp(right.schema, () => Iterator.single(rows), "CrossInput"):
              override protected def extraInputs(indent: Int): Seq[String] = Seq(
                right.explain(indent)
              )
              override def sharedInputs: Seq[Operator] = Seq(right)
          p.copy(
              prepare = p.prepare :+ (() => rows = Operator.materialize(right)),
              close = p.close :+ (() => right.close())
            )
            .map(x => NestedLoopJoinOp(x, shared, None, JoinKind.Cross, j.schema))
        }
      case _ =>
        None

  /** An aggregation grouping by every column of op, with no aggregates: its distinct rows */
  private def distinctAgg(op: Operator): Operator = HashAggOp(
    op,
    op.schema.fields.indices.map(i => ColRef(i, op.schema(i).ptype, op.schema(i).name)),
    IndexedSeq.empty,
    op.schema
  )

  /**
    * Whether DISTINCT over op can run as a two-phase aggregation: op is a scan pipeline, whose
    * parts are reduced to their distinct rows. Otherwise the partitioned aggregation would buffer
    * every input row, while DistinctOp keeps only the distinct ones
    */
  private def parallelDistinct(op: Operator, runner: TaskRunner)(using AboveBreakers): Boolean =
    op.schema.size > 0 && startsPipeline(op)

  /** Whether [[pipeline]] finds a pipeline in op, without building it */
  private def startsPipeline(op: Operator)(using breakers: AboveBreakers): Boolean =
    op match
      case s: ScanOp =>
        s.source.canSplit
      case a: HashAggOp =>
        breakers.enabled && a.keys.nonEmpty
      case f: FilterOp =>
        startsPipeline(f.child)
      case p: ProjectOp =>
        startsPipeline(p.child)
      case j: HashJoinOp =>
        startsPipeline(j.left)
      case j: NestedLoopJoinOp =>
        j.kind == JoinKind.Cross && j.cond.isEmpty && startsPipeline(j.left)
      case _ =>
        false

  private def alreadyDistinct(op: Operator): Boolean =
    op match
      case _: DistinctOp =>
        true
      case s: SetOpOp =>
        !s.all
      case a: HashAggOp =>
        a.keys.nonEmpty
      case _ =>
        false

  /** The distinct rows of a set operation input, unless they already are */
  private def distinctInput(op: Operator, runner: TaskRunner)(using AboveBreakers): Operator =
    op match
      case _: DistinctOp =>
        op
      case s: SetOpOp if !s.all =>
        op
      // Unique on its keys, so on its whole rows
      case a: HashAggOp if a.keys.nonEmpty =>
        op
      case _ if parallelDistinct(op, runner) =>
        distinctAgg(op)
      case _ =>
        op

  /** A hash aggregation run in parallel: in two phases over a pipeline, or partitioned by key */
  private def aggregate(a: HashAggOp, runner: TaskRunner)(using o: AboveBreakers): Operator =
    def r(x: Operator): Operator = rewrite(x, runner)
    if TwoPhaseAgg.supports(a) then
      pipeline(a.child, runner) match
        case Some(p) =>
          TwoPhaseAgg.plan(
            a,
            p.scan,
            p.rebuild,
            p.tail,
            runner,
            p.prepare,
            p.close,
            o.parallelPartitioning
          )
        case None if a.keys.nonEmpty =>
          PartitionedAggOp(
            r(a.child),
            a.keys,
            a.aggs,
            a.schema,
            runner,
            expectedGroups = a.expectedGroups,
            parallelPartitioning = o.parallelPartitioning
          )
        case None =>
          HashAggOp(r(a.child), a.keys, a.aggs, a.schema, a.expectedGroups)
    else if a.keys.nonEmpty then
      // Any aggregate (DISTINCT, quantiles, ...): every group falls in one key partition
      PartitionedAggOp(
        r(a.child),
        a.keys,
        a.aggs,
        a.schema,
        runner,
        expectedGroups = a.expectedGroups,
        parallelPartitioning = o.parallelPartitioning
      )
    else
      HashAggOp(r(a.child), a.keys, a.aggs, a.schema, a.expectedGroups)
    end if

  end aggregate

  /**
    * The column selections and renames between `op` and the grouped aggregation at the bottom of
    * its pipeline, over `agg` (that aggregation, rewritten) instead
    */
  private def rebuildOver(op: Operator, agg: Operator): Operator =
    op match
      case p: ProjectOp =>
        ProjectOp(rebuildOver(p.child, agg), p.exprs, p.schema)
      case _ =>
        agg

  private def rewrite(op: Operator, runner: TaskRunner)(using AboveBreakers): Operator =
    def r(x: Operator): Operator = rewrite(x, runner)
    pipeline(op, runner) match
      case Some(p) =>
        p.scan.source match
          case b: BreakerSource if !p.hasWork =>
            // Nothing above the aggregation worth running as morsels: keep the operators as they
            // are over it
            rebuildOver(op, b.op)
          case _ =>
            MorselOp(p.scan, p.rebuild, op.schema, runner, p.prepare, p.close, p.tail)
      case None =>
        op match
          case a: HashAggOp =>
            aggregate(a, runner)
          case f: FilterOp =>
            FilterOp(r(f.child), f.cond, f.keep).withLateGather(f.lateGather)
          case p: ProjectOp =>
            ProjectOp(r(p.child), p.exprs, p.schema)
          case l: LimitOp if l.child.isInstanceOf[DistinctOp] =>
            // DistinctOp streams new rows, so the limit can stop the input early
            LimitOp(DistinctOp(r(l.child.asInstanceOf[DistinctOp].child)), l.limit, l.offset)
          case l: LimitOp =>
            LimitOp(r(l.child), l.limit, l.offset)
          case s: SortOp =>
            SortOp(r(s.child), s.keys, s.limit, runner)
          case d: DistinctOp
              if d.schema.size > 0 && (
                d.child match
                  case u: UnionAllOp =>
                    u.inputs.exists(parallelDistinct(_, runner))
                  case _ =>
                    false
              ) =>
            // UNION (distinct): the distinct rows of each input (in parallel for scan pipelines,
            // streaming through DistinctOp otherwise), then merged by key. The final aggregation
            // holds only the inputs' distinct rows
            val u      = d.child.asInstanceOf[UnionAllOp]
            val inputs = u
              .inputs
              .map { i =>
                distinctInput(i, runner) match
                  case same if (same eq i) && !alreadyDistinct(i) && i.schema.size > 0 =>
                    DistinctOp(i)
                  case reduced =>
                    reduced
              }
            r(distinctAgg(UnionAllOp(inputs, u.schema)))
          case d: DistinctOp if parallelDistinct(d.child, runner) =>
            // DISTINCT groups by every column: as an aggregation, it runs in two phases (partial
            // per scan part, then partitioned by key), like DuckDB's distinct
            r(distinctAgg(d.child))
          case d: DistinctOp =>
            DistinctOp(r(d.child))
          case u: UnionAllOp =>
            UnionAllOp(u.inputs.map(r), u.schema)
          case j: HashJoinOp =>
            HashJoinOp(
              r(j.left),
              r(j.right),
              j.leftKeys,
              j.rightKeys,
              j.kind,
              j.residual,
              j.schema,
              buildRunner = runner,
              runtimeFilters = j.runtimeFilters
            ).withLateGather(j.lateGather).withDirectIndex(j.directIndex)
          case j: NestedLoopJoinOp =>
            NestedLoopJoinOp(r(j.left), r(j.right), j.cond, j.kind, j.schema)
          case s: SetOpOp if !s.all =>
            // INTERSECT / EXCEPT (without ALL) need only the distinct rows of each side: reduce
            // them in parallel first
            SetOpOp(
              r(distinctInput(s.left, runner)),
              r(distinctInput(s.right, runner)),
              s.isIntersect,
              false,
              s.schema
            )
          case s: SetOpOp =>
            SetOpOp(r(s.left), r(s.right), s.isIntersect, s.all, s.schema)
          case w: WindowOp =>
            WindowOp(r(w.child), w.calls, w.schema, runner)
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
      tail: Option[Operator => Operator],
      runner: TaskRunner,
      prepare: List[() => Unit],
      release: List[() => Unit],
      parallelPartitioning: Boolean
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
    // Sized per part: the query-wide estimate would pre-size one table per scan part
    val sizer   = PartialGroupSizer(a.expectedGroups)
    val partial = MorselOp(
      scan,
      x => HashAggOp(rebuild(x), a.keys, partialAggs, partialSchema, sizer.hint(x), sizer.report),
      partialSchema,
      runner,
      prepare,
      release,
      tail.map(t => x => HashAggOp(t(x), a.keys, partialAggs, partialSchema))
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
    // Many groups: combine them in parallel, partitioned by key
    val combined =
      if nk > 0 then
        PartitionedAggOp(
          partial,
          (0 until nk).map(col),
          finalAggs,
          finalSchema,
          runner,
          expectedGroups = a.expectedGroups,
          parallelPartitioning = parallelPartitioning
        )
      else
        HashAggOp(partial, (0 until nk).map(col), finalAggs, finalSchema)
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
