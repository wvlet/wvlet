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

import scala.collection.mutable

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
    * `parallelPartitioning`, partitioned aggregations route their input rows to partitions as
    * tasks. With `adaptivePartials`, the partial aggregates of two-phase aggregations stop grouping
    * when the groups don't reduce the rows ([[PartialAggSkip]])
    */
  def apply(
      op: Operator,
      runner: TaskRunner,
      aboveBreakers: Boolean = true,
      parallelPartitioning: Boolean = true,
      concurrentBuilds: Boolean = true,
      sharedPartials: Boolean = true,
      adaptivePartials: Boolean = true,
      setOpJoins: Boolean = true,
      packAggKeys: Boolean = true,
      unionPipelines: Boolean = true,
      streamPackAggKeys: Boolean = true,
      longDecimalKeys: Boolean = true,
      fusedAccumulators: Boolean = true,
      oneRowPartials: Boolean = true,
      selectedInputs: Boolean = true,
      longGroupTables: Boolean = true,
      sortedGroupRuns: Boolean = true,
      unionDistinctOnce: Boolean = true,
      anyValuePartials: Boolean = true,
      parallelOuterTail: Boolean = true
  ): Operator =
    if runner.parallelism <= 1 then
      op
    else
      given AboveBreakers = AboveBreakers(
        aboveBreakers,
        parallelPartitioning,
        concurrentBuilds,
        sharedPartials,
        adaptivePartials,
        setOpJoins,
        packAggKeys,
        unionPipelines,
        streamPackAggKeys,
        longDecimalKeys,
        fusedAccumulators,
        oneRowPartials,
        selectedInputs,
        longGroupTables,
        sortedGroupRuns,
        unionDistinctOnce,
        anyValuePartials,
        parallelOuterTail
      )
      rewrite(op, runner)

  /**
    * Whether pipelines start at the outputs of grouped aggregations ([[BreakerSource]]), whether
    * partitioned aggregations route their input in parallel, and whether the builds of a pipeline
    * run at the same time
    */
  private case class AboveBreakers(
      enabled: Boolean,
      parallelPartitioning: Boolean,
      concurrentBuilds: Boolean,
      sharedPartials: Boolean,
      adaptivePartials: Boolean,
      setOpJoins: Boolean,
      packAggKeys: Boolean,
      unionPipelines: Boolean,
      streamPackAggKeys: Boolean,
      longDecimalKeys: Boolean,
      fusedAccumulators: Boolean,
      oneRowPartials: Boolean,
      selectedInputs: Boolean,
      longGroupTables: Boolean,
      sortedGroupRuns: Boolean,
      unionDistinctOnce: Boolean,
      // any_value splits into two phases (`dependent_group_keys`, which emits it)
      anyValuePartials: Boolean,
      parallelOuterTail: Boolean
  )

  /**
    * The steps a pipeline runs before its parts start: with `concurrent_join_builds`, several
    * builds run as one step that runs them at the same time ([[TaskRunner.runTogether]])
    */
  private def prepareSteps(p: Pipeline, runner: TaskRunner)(using
      o: AboveBreakers
  ): List[() => Unit] =
    if o.concurrentBuilds && p.prepare.size > 1 then
      List(() => TaskRunner.runTogether(runner, p.prepare))
    else
      p.prepare

  /**
    * Scans and the operators over them that can run on parts of the scans independently: each
    * segment's `pipeline(input)` makes a copy of those operators over another input of its scan. A
    * pipeline has one segment, or one per scan of the inputs of a UNION ALL
    * (`union_all_pipelines`), whose operators above the union are the same. `tails`, when the
    * pipeline has right / full joins, make the copies that run after every part over an empty input
    * of a scan, emitting the build rows no part matched (one per union input with such joins).
    * `prepare` runs before parts start (hash join builds), on the calling thread or, with
    * `concurrent_join_builds`, together on coordinator threads, and `close` releases what the
    * copies share
    */
  private case class Pipeline(
      segments: Seq[MorselOp.Segment],
      tails: Seq[MorselOp.Tail],
      prepare: List[() => Unit],
      close: List[() => Unit],
      hasWork: Boolean = false
  ):
    /** The scan of the first segment */
    def scan: ScanOp = segments.head.scan

    /** The copy of the operators over an input of the first scan */
    def rebuild: Operator => Operator = segments.head.pipeline

    /**
      * Add an operator over the pipeline, the same over the parts and the tail. `work`: whether it
      * computes something (not only selects or renames columns)
      */
    def map(f: Operator => Operator, work: Boolean = true): Pipeline = copy(
      segments = segments.map(s => s.copy(pipeline = x => f(s.pipeline(x)))),
      tails = tails.map(_.map(f)),
      hasWork = hasWork || work
    )

    /** The parts of every segment in order, as one [[MorselOp]] */
    def morsels(schema: Schema, runner: TaskRunner, prepareSteps: List[() => Unit]): MorselOp =
      MorselOp(segments, schema, runner, prepareSteps, close, tails)

  private object Pipeline:
    def of(scan: ScanOp, close: List[() => Unit] = Nil): Pipeline = Pipeline(
      Seq(MorselOp.Segment(scan, x => x)),
      Nil,
      Nil,
      close
    )

  private def isOuterBuild(kind: JoinKind): Boolean = kind.tracksBuildMatches

  private def pipeline(op: Operator, runner: TaskRunner)(using
      breakers: AboveBreakers
  ): Option[Pipeline] =
    op match
      case s: ScanOp if s.source.canSplit =>
        Some(Pipeline.of(s))
      case s: SetOpOp if convertsToJoin(s) =>
        // As a semi / anti join, so that operators above it (count(*)) join its pipeline
        pipeline(setOpJoin(s, runner), runner)
      case a: HashAggOp if breakers.enabled && a.keys.nonEmpty =>
        // The aggregation runs on the calling thread before any part starts (it parallelizes
        // its own work); its output batches are the parts
        val source = BreakerSource(aggregate(a, runner), "Aggregate")
        Some(Pipeline.of(ScanOp(source, a.schema, None, None), List(source.close)))
      case u: UnionAllOp if breakers.unionPipelines =>
        unionPipeline(u, runner)
      case f: FilterOp =>
        pipeline(f.child, runner).map(_.map(x => FilterOp(x, f.cond, f.keep).withSettingsOf(f)))
      case pr: ProjectOp =>
        pipeline(pr.child, runner).map(
          _.map(
            x => ProjectOp(x, pr.exprs, pr.schema).withDenseSelected(pr.denseSelected),
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
            runtimeFilters = j.runtimeFilters,
            nullsEqual = j.nullsEqual,
            exclude = j.exclude
          ).withLateGather(j.lateGather)
            .withDirectIndex(j.directIndex)
            .withParallelBuild(j.parallelBuild)
            .withStringCodes(j.stringCodes)
            .withUnmatchedOnly(j.unmatchedOnly)
            .withMissFilter(j.joinBuild.missFilter)
            .withNarrowEntries(j.joinBuild.narrowEntries)
          val shared = p.copy(
            prepare = p.prepare :+ (() => template.joinBuild.ensureBuilt()),
            close = p.close :+ (() => template.right.close())
          )
          if isOuterBuild(j.kind) then
            // Parts mark matched build rows; the tail emits the rest once every part is done
            val tail =
              p.tails match
                case Seq(inner) =>
                  // The inner joins' unmatched rows probe this join, which emits its own after
                  // them: one run over every build row
                  MorselOp
                    .Tail
                    .whole(x => template.withLeft(inner.run(0, Int.MaxValue)(x), OuterMode.Tail))
                    .copy(scan = inner.scan)
                case inner if inner.nonEmpty =>
                  // The tails of several union inputs, as one union of their rows
                  MorselOp
                    .Tail
                    .whole(_ =>
                      template.withLeft(
                        UnionAllOp(
                          inner.map(t =>
                            t.run(0, Int.MaxValue)(emptyInput(t.scan.getOrElse(p.scan)))
                          ),
                          j.left.schema
                        ),
                        OuterMode.Tail
                      )
                    )
                case _ =>
                  MorselOp.Tail(
                    (from, until) =>
                      x =>
                        template.withLeft(p.rebuild(x), OuterMode.Tail).withTailRange(from, until),
                    () => template.joinBuild.build.size,
                    splittable = breakers.parallelOuterTail
                  )
            shared.copy(
              segments = p
                .segments
                .map(s => s.copy(pipeline = x => template.withLeft(s.pipeline(x), OuterMode.Part))),
              tails = Seq(tail),
              // A join is work: without it, `rewrite` would keep only the projections over an
              // aggregation below and drop the join
              hasWork = true
            )
          else
            shared.map(x => template.withLeft(x))
          end if
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

  /**
    * A UNION ALL of pipelines as one pipeline (`union_all_pipelines`): the segments of every input
    * in input order, each cast to the union's columns, so that the operators above the union (hash
    * join probes over one shared build, partial aggregates) run on the parts of every input. None
    * when an input is no pipeline, or has a tail (right / full joins) without
    * `parallel_outer_tail`; with it, the inputs' tails run after every part of the union. Checked
    * before building the inputs' pipelines, as [[startsPipeline]] does
    */
  private def unionPipeline(u: UnionAllOp, runner: TaskRunner)(using
      breakers: AboveBreakers
  ): Option[Pipeline] =
    if !startsPipeline(u) then
      None
    else
      val inputs = u.inputs.map(pipeline(_, runner))
      if inputs.exists(p => p.isEmpty || (p.get.tails.nonEmpty && !breakers.parallelOuterTail)) then
        None
      else
        val ps = inputs.flatten
        Some(
          Pipeline(
            ps.flatMap { p =>
              // The input's output columns, as UnionAllOp casts them
              p.segments.map(s => s.copy(pipeline = x => asUnionColumns(s.pipeline(x), u.schema)))
            },
            // With `parallel_outer_tail`, each input's tails, over an empty input of its own scan
            ps.flatMap(p =>
              p.tails
                .map(t =>
                  t.copy(scan = t.scan.orElse(Some(p.scan))).map(asUnionColumns(_, u.schema))
                )
            ),
            ps.flatMap(_.prepare).toList,
            ps.flatMap(_.close).toList,
            ps.exists(_.hasWork)
          )
        )

  /** An input of no rows with the columns of scan */
  private def emptyInput(scan: ScanOp): Operator = IteratorOp(
    scan.schema,
    () => Iterator.empty,
    scan.name
  )

  /** op with the columns of a UNION ALL's schema, cast as UnionAllOp casts them */
  private def asUnionColumns(op: Operator, schema: Schema): Operator =
    if op.schema.types == schema.types then
      op
    else
      ProjectOp(
        op,
        schema
          .fields
          .indices
          .map { i =>
            val c = ColRef(i, op.schema(i).ptype, op.schema(i).name)
            if c.ptype == schema(i).ptype then
              c
            else
              CastExpr(c, schema(i).ptype)
          },
        schema
      )

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
      case u: UnionAllOp =>
        breakers.unionPipelines && u.inputs.nonEmpty &&
        u.inputs.forall(i => startsPipeline(i) && (breakers.parallelOuterTail || !hasTail(i)))
      case _ =>
        false

  /**
    * Whether the pipeline that [[startsPipeline]] finds in op has a tail: a right / full join whose
    * unmatched build rows are emitted after every part
    */
  private def hasTail(op: Operator): Boolean =
    op match
      case f: FilterOp =>
        hasTail(f.child)
      case p: ProjectOp =>
        hasTail(p.child)
      case j: HashJoinOp =>
        isOuterBuild(j.kind) || hasTail(j.left)
      case j: NestedLoopJoinOp =>
        hasTail(j.left)
      case u: UnionAllOp =>
        u.inputs.exists(hasTail)
      case _ =>
        false

  private def alreadyDistinct(op: Operator): Boolean =
    op match
      case _: DistinctOp =>
        true
      case s: SetOpOp =>
        !s.all
      // Unique on its keys, so on its whole rows
      case a: HashAggOp =>
        a.keys.nonEmpty
      // A semi / anti join emits some of its probe rows, each at most once
      case j: HashJoinOp =>
        j.kind.outputsProbeOnly && alreadyDistinct(j.left)
      case _ =>
        false

  /**
    * The inputs of a UNION ALL under a distinct, with each input that is itself the distinct rows
    * of a UNION ALL of the same column types replaced by that union's inputs: the outer distinct
    * removes the same rows, and casting an input to the outer types directly gives the values the
    * inner union's casts would
    */
  private def distinctUnionInputs(u: UnionAllOp): Seq[Operator] = u
    .inputs
    .flatMap {
      case d: DistinctOp =>
        d.child match
          case inner: UnionAllOp if inner.schema.types == u.schema.types =>
            distinctUnionInputs(inner)
          case _ =>
            Seq(d)
      case other =>
        Seq(other)
    }

  /**
    * With `union_distinct_once`, the union (nested UNION (distinct) inputs flattened) that a
    * distinct over a UNION ALL reads, when it is one pipeline to aggregate in two phases
    */
  private def onceDistinctUnion(d: DistinctOp, runner: TaskRunner)(using
      o: AboveBreakers
  ): Option[UnionAllOp] =
    d.child match
      case u: UnionAllOp if o.unionDistinctOnce =>
        Some(UnionAllOp(distinctUnionInputs(u), u.schema)).filter(parallelDistinct(_, runner))
      case _ =>
        None

  /** The distinct rows of a set operation input, unless they already are */
  private def distinctInput(op: Operator, runner: TaskRunner)(using AboveBreakers): Operator =
    op match
      case _ if alreadyDistinct(op) =>
        op
      case _ if parallelDistinct(op, runner) =>
        distinctAgg(op)
      case _ =>
        op

  /** A hash aggregation run in parallel: in two phases over a pipeline, or partitioned by key */
  private def aggregate(a: HashAggOp, runner: TaskRunner)(using o: AboveBreakers): Operator =
    def r(x: Operator): Operator = rewrite(x, runner)
    if TwoPhaseAgg.supports(a, o.anyValuePartials) then
      pipeline(a.child, runner) match
        case Some(p) =>
          TwoPhaseAgg.plan(
            a,
            p.segments,
            p.tails,
            runner,
            prepareSteps(p, runner),
            p.close,
            o.parallelPartitioning,
            o.sharedPartials,
            o.adaptivePartials,
            o.packAggKeys,
            o.streamPackAggKeys,
            o.longDecimalKeys,
            o.fusedAccumulators,
            o.oneRowPartials,
            o.selectedInputs,
            o.longGroupTables,
            o.sortedGroupRuns
          )
        case None if a.keys.nonEmpty =>
          PartitionedAggOp(
            r(a.child),
            a.keys,
            a.aggs,
            a.schema,
            runner,
            expectedGroups = a.expectedGroups,
            parallelPartitioning = o.parallelPartitioning,
            packKeys = o.packAggKeys
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
        parallelPartitioning = o.parallelPartitioning,
        packKeys = o.packAggKeys
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
        ProjectOp(rebuildOver(p.child, agg), p.exprs, p.schema).withDenseSelected(p.denseSelected)
      case _ =>
        agg

  /** Whether [[setOpJoin]] turns s into a semi / anti join (`set_op_joins`) */
  private def convertsToJoin(s: SetOpOp)(using o: AboveBreakers): Boolean =
    o.setOpJoins && !s.all && s.schema.size > 0

  /**
    * INTERSECT / EXCEPT (without ALL) as a semi / anti hash join of the left input's distinct rows
    * with the right input, on every column with NULLs equal (`set_op_joins`), as DuckDB plans them.
    * Nested set operations convert first, so a chain of them becomes one probe pipeline over the
    * distinct rows of its first input. Other operators are returned as they are
    */
  private def setOpJoin(op: Operator, runner: TaskRunner)(using o: AboveBreakers): Operator =
    op match
      case s: SetOpOp if convertsToJoin(s) =>
        val left  = setOpJoin(s.left, runner)
        val right = setOpJoin(s.right, runner)
        val probe =
          left match
            // As an aggregation, so that the probe pipeline starts at its output
            case d: DistinctOp if parallelDistinct(d.child, runner) =>
              distinctAgg(d.child)
            case _ =>
              distinctInput(left, runner) match
                case same if (same eq left) && !alreadyDistinct(left) =>
                  DistinctOp(left)
                case reduced =>
                  reduced
        // The right input needs no distinct rows, but reducing them in parallel shrinks the build
        val build = distinctInput(right, runner)
        // Keys in the set operation's column types, as SetOpOp compares them
        def keys(in: Operator): IndexedSeq[VExpr] = s
          .schema
          .fields
          .indices
          .map { i =>
            val c = ColRef(i, in.schema(i).ptype, in.schema(i).name)
            if c.ptype == s.schema(i).ptype then
              c
            else
              CastExpr(c, s.schema(i).ptype)
          }
        val probeKeys = keys(probe)
        val join      = HashJoinOp(
          probe,
          build,
          probeKeys,
          keys(build),
          if s.isIntersect then
            JoinKind.Semi
          else
            JoinKind.Anti
          ,
          None,
          probe.schema,
          nullsEqual = true
        )
        if probe.schema.types == s.schema.types then
          join
        else
          ProjectOp(join, probeKeys, s.schema)
      case _ =>
        op

  private def rewrite(op: Operator, runner: TaskRunner)(using AboveBreakers): Operator =
    op match
      case s: SetOpOp if convertsToJoin(s) =>
        rewrite(setOpJoin(s, runner), runner)
      case _ =>
        rewriteOp(op, runner)

  private def rewriteOp(op: Operator, runner: TaskRunner)(using AboveBreakers): Operator =
    def r(x: Operator): Operator = rewrite(x, runner)
    // A union with nothing above it worth running as one pipeline keeps its inputs' own ones
    pipeline(op, runner).filter(p => p.segments.size == 1 || p.hasWork) match
      case Some(p) =>
        p.scan.source match
          case b: BreakerSource if !p.hasWork =>
            // Nothing above the aggregation worth running as morsels: keep the operators as they
            // are over it
            rebuildOver(op, b.op)
          case _ =>
            p.morsels(op.schema, runner, prepareSteps(p, runner))
      case None =>
        op match
          case a: HashAggOp =>
            aggregate(a, runner)
          case f: FilterOp =>
            FilterOp(r(f.child), f.cond, f.keep).withSettingsOf(f)
          case p: ProjectOp =>
            ProjectOp(r(p.child), p.exprs, p.schema).withDenseSelected(p.denseSelected)
          case l: LimitOp if l.child.isInstanceOf[DistinctOp] =>
            // DistinctOp streams new rows, so the limit can stop the input early
            LimitOp(DistinctOp(r(l.child.asInstanceOf[DistinctOp].child)), l.limit, l.offset)
          case l: LimitOp =>
            LimitOp(r(l.child), l.limit, l.offset)
          case s: SortOp =>
            SortOp(r(s.child), s.keys, s.limit, runner)
          case d: DistinctOp if onceDistinctUnion(d, runner).isDefined =>
            // UNION (distinct) over pipelines: one two-phase distinct over the union of the
            // inputs' pipelines (nested UNION (distinct) inputs included), whose partials reduce
            // each scan part
            r(distinctAgg(onceDistinctUnion(d, runner).get))
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
              runtimeFilters = j.runtimeFilters,
              nullsEqual = j.nullsEqual,
              exclude = j.exclude
            ).withLateGather(j.lateGather)
              .withDirectIndex(j.directIndex)
              .withParallelBuild(j.parallelBuild)
              .withStringCodes(j.stringCodes)
              .withUnmatchedOnly(j.unmatchedOnly)
              .withMissFilter(j.joinBuild.missFilter)
              .withNarrowEntries(j.joinBuild.narrowEntries)
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
            WindowOp(
              r(w.child),
              w.calls,
              w.schema,
              runner,
              topN = w.topN,
              runningKernels = w.runningKernels,
              sortKernels = w.sortKernels,
              sharePartitions = w.sharePartitions,
              streamTopN = w.streamTopN
            )
          case other =>
            other
    end match

  end rewriteOp

end Parallelizer

/**
  * Aggregation in two phases: each part of a scan pipeline is aggregated on its own (partial
  * aggregates), then the partial results are combined by the grouping keys. Supported for
  * aggregates whose partial results combine with another aggregate: count (summed), sum, min, max,
  * and avg (as a sum and a count)
  */
private[plan] object TwoPhaseAgg:
  private def lower(c: AggCall): String = c.name.toLowerCase

  /** Whether `c` counts rows, so that it is 0 rather than NULL over no rows */
  private[plan] def isCount(c: AggCall): Boolean =
    lower(c) match
      case "count" | "count_star" | "count_if" | "countif" =>
        true
      case _ =>
        false

  def supports(a: HashAggOp, anyValue: Boolean): Boolean = supportsCalls(a.aggs, anyValue)

  /**
    * Whether every aggregate's partial results combine with another aggregate; with `anyValue`,
    * any_value and arbitrary too (any_value of their partials)
    */
  def supportsCalls(calls: Seq[AggCall], anyValue: Boolean): Boolean = calls.forall { c =>
    !c.distinct && c.order.isEmpty && (
      isCount(c) || Set("sum", "min", "max", "avg", "mean").contains(lower(c)) ||
        anyValue &&
        (lower(c) == "any_value" || lower(c) == "arbitrary")
    )
  }

  /**
    * The partial aggregates of `calls`, and for each call the indexes of the partial aggregates it
    * reads (a sum and a count for avg, the call itself otherwise). With `shared`, identical partial
    * aggregates are computed once (`shared_agg_partials`): `avg(x)` reads the partials of `sum(x)`
    * and `count(x)` of the same aggregation
    */
  def decompose(
      calls: Seq[AggCall],
      shared: Boolean = false
  ): (IndexedSeq[AggCall], IndexedSeq[IndexedSeq[Int]]) =
    val partials             = mutable.ArrayBuffer.empty[AggCall]
    val sources              = IndexedSeq.newBuilder[IndexedSeq[Int]]
    def add(c: AggCall): Int =
      // Matched with ==, not by sig, which leaves out RowFn closures
      val i =
        if shared then
          partials.indexOf(c)
        else
          -1
      if i >= 0 then
        i
      else
        partials += c
        partials.size - 1
    calls.foreach { c =>
      lower(c) match
        case "avg" | "mean" =>
          // Decimals sum exactly; other numbers sum as doubles, as Avg does (no overflow)
          val sumType =
            c.args.headOption.map(_.ptype) match
              case Some(d: PType.Decimal) =>
                Aggregates.resultType("sum", Seq(d))
              case _ =>
                PType.Float64
          val s = add(c.copy(name = "sum", distinct = false, ptype = sumType, order = Nil))
          val n = add(c.copy(name = "count", distinct = false, ptype = PType.Int64, order = Nil))
          sources += IndexedSeq(s, n)
        case _ =>
          sources += IndexedSeq(add(c))
    }
    (partials.toIndexedSeq, sources.result())

  end decompose

  /** The aggregate combining the partial results `in` of partial aggregate `c` */
  def merge(c: AggCall, in: VExpr): AggCall =
    if isCount(c) then
      AggCall("sum", Seq(in), false, None, PType.Int64)
    else
      AggCall(lower(c), Seq(in), false, None, c.ptype)

  /**
    * The value of `c` from its combined partial results `src` (as `decompose` gave them), in the
    * type of `c`
    */
  def finish(c: AggCall, src: IndexedSeq[VExpr]): VExpr =
    def as(e: VExpr, t: PType): VExpr =
      if e.ptype == t then
        e
      else
        CastExpr(e, t)
    src match
      case IndexedSeq(s, n) =>
        as(Arith(ArithOp.Div, as(s, PType.Float64), as(n, PType.Float64), PType.Float64), c.ptype)
      case IndexedSeq(i) =>
        as(i, c.ptype)

  def plan(
      a: HashAggOp,
      segments: Seq[MorselOp.Segment],
      tails: Seq[MorselOp.Tail],
      runner: TaskRunner,
      prepare: List[() => Unit],
      release: List[() => Unit],
      parallelPartitioning: Boolean,
      sharedPartials: Boolean,
      adaptivePartials: Boolean = false,
      packAggKeys: Boolean = false,
      streamPackAggKeys: Boolean = false,
      longDecimalKeys: Boolean = false,
      fusedAccumulators: Boolean = true,
      oneRowPartials: Boolean = true,
      selectedInputs: Boolean = true,
      longGroupTables: Boolean = true,
      sortedGroupRuns: Boolean = true
  ): Operator =
    val nk = a.keys.size
    // Partial aggregates, and for each original aggregate the partial columns it reads
    val (partialAggs, rel) = decompose(a.aggs, sharedPartials)
    val srcs               = rel.map(_.map(_ + nk))
    val partialSchema      = Schema(
      a.schema.fields.take(nk) ++
        partialAggs.zipWithIndex.map((c, i) => Field(s"__partial_${i}", c.ptype))
    )
    // Sized per part: the query-wide estimate would pre-size one table per scan part
    val sizer = PartialGroupSizer(a.expectedGroups)
    val skip  =
      if adaptivePartials then
        PartialAggSkip()
      else
        null
    val partial = MorselOp(
      segments.map(s =>
        s.copy(pipeline =
          x =>
            HashAggOp(
              s.pipeline(x),
              a.keys,
              partialAggs,
              partialSchema,
              sizer.hint(x),
              sizer.report,
              skip
            ).withKeyOptions(streamPackAggKeys, longDecimalKeys)
              .withFusedAccumulators(fusedAccumulators)
              .withColumnMajorSums(a.columnMajorSums)
              .withOneRowPartials(oneRowPartials)
              .withSelectedInputs(selectedInputs)
              .withDenseSelected(a.denseSelected)
              .withLongGroupTables(longGroupTables)
              .withSortedGroupRuns(sortedGroupRuns)
        )
      ),
      partialSchema,
      runner,
      prepare,
      release,
      tails.map(t =>
        t.map((x: Operator) =>
          HashAggOp(x, a.keys, partialAggs, partialSchema)
            .withKeyOptions(streamPackAggKeys, longDecimalKeys)
            .withFusedAccumulators(fusedAccumulators)
            .withColumnMajorSums(a.columnMajorSums)
            .withSelectedInputs(selectedInputs)
            .withDenseSelected(a.denseSelected)
            .withLongGroupTables(longGroupTables)
            .withSortedGroupRuns(sortedGroupRuns)
        )
      )
    )
    def col(i: Int): VExpr = ColRef(i, partialSchema(i).ptype, partialSchema(i).name)
    // Combine partial results by key
    val finalAggs   = partialAggs.zipWithIndex.map((c, j) => merge(c, col(nk + j)))
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
          parallelPartitioning = parallelPartitioning,
          packKeys = packAggKeys
        )
      else
        HashAggOp(partial, (0 until nk).map(col), finalAggs, finalSchema)
    // Restore the original output: keys, then each aggregate in its original type
    def fcol(i: Int): VExpr = ColRef(i, finalSchema(i).ptype, finalSchema(i).name)
    val outputs             = (0 until nk).map(fcol) ++
      a.aggs.zip(srcs).map((c, src) => finish(c, src.map(fcol)))
    ProjectOp(combined, outputs.toIndexedSeq, a.schema)

  end plan

end TwoPhaseAgg
