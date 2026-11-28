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

import wvlet.lang.api.StatusCode
import wvlet.lang.engine.catalog.*
import wvlet.lang.engine.exec.*
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.runtime.CTEJoinKeys
import wvlet.lang.engine.runtime.Stats
import wvlet.lang.engine.runtime.BExpr
import wvlet.lang.engine.runtime.JoinCondition
import wvlet.lang.engine.runtime.Optimizer
import wvlet.lang.engine.runtime.ParallelResultStats
import wvlet.lang.engine.runtime.PlanValidator
import wvlet.lang.engine.runtime.PushDownFilters
import wvlet.lang.engine.runtime.RuntimePlan
import wvlet.lang.engine.runtime.SharedCTE
import wvlet.lang.engine.vector.*

import scala.collection.mutable

/**
  * The shared results of [[Planner]]: operator results computed once and read by several readers
  * (CTEs read more than once, grouping-set inputs), the shared CTEs whose bodies are lowered once
  * every reader is bound ([[DeferredCTE]]), and their finalization and concurrent computation
  */
trait SharedResults:
  this: Planner =>

  /**
    * The rows of an operator, computed once on first read and shared by several readers (e.g., a
    * CTE referenced more than once, or the input of grouping sets)
    */
  private[plan] class SharedResult(val op: Operator) extends SharedBatches:
    // The result's own reservation, returned once its last reader finishes ([[armReads]])
    private val holder = memory.child()
    private val result = ComputeOnce { () =>
      debug(s"Materializing a shared result:\n${op.explain()}")
      holder.drain(op)
    }

    // Reads by the planned query's readers still to finish; -1 until armed (`release_cte_results`)
    private val pendingReads = java.util.concurrent.atomic.AtomicInteger(-1)
    @volatile
    private var released = false

    /** Whether the batches were dropped after the last reader finished */
    def isReleased: Boolean = released

    /**
      * Drop the batches once `reads` more reads finish ([[readDone]]): the reads of the planned
      * query's readers, counted from its operator tree
      */
    def armReads(reads: Int): Unit = pendingReads.set(reads)

    /** One read of the planned query finished: the last one drops the batches */
    def readDone(): Unit =
      if pendingReads.get() > 0 && pendingReads.decrementAndGet() == 0 then
        val e = StatusCode
          .UNEXPECTED_STATE
          .newException("A shared result is read after its last reader finished")
        if result.discard(e) then
          released = true
          holder.release()

    /** The batches, computed on this thread unless another thread has started them */
    def batches: Seq[Batch] = result.get()

    /** Whether the batches are computed (or being computed) */
    def isComputed: Boolean = result.isStarted

    def compute(stop: () => Boolean): Unit = result.runUnless(stop)

    def awaitIfStarted(): Unit = result.awaitIfStarted()

    private var explained = false

    // The first reader shows the shared plan once (it is not a child: readers must not close it)
    /** The plan, shown under the first reader only */
    def sharedPlan(indent: Int): Option[String] =
      if explained then
        None
      else
        explained = true
        Some(s"${"  " * indent}(shared)\n${op.explain(indent + 1)}")

    def newReader(label: String): Operator =
      if op.schema.names.distinct.size == op.schema.size then
        // A scan of the result as a table whose parts are its batches, so that the Parallelizer
        // runs the filters, join probes, and aggregations over a reader in parallel, as over a
        // Parquet scan (scans select columns by name, so names must be unique)
        ScanOp(SharedTable(label, op.schema, this), op.schema, None, None)
      else
        SharedReader(this, op.schema, label)

  end SharedResult

  /** A reader of a shared result over duplicate column names, which a scan can't select by name */
  private[plan] class SharedReader(val shared: SharedResult, schema: Schema, label: String)
      extends IteratorOp(schema, () => shared.batches.iterator, label):
    override protected def extraInputs(indent: Int): Seq[String] = shared.sharedPlan(indent).toSeq
    override def sharedInputs: Seq[Operator]                     = Seq(shared.op)

  /** Batches computed on first read, scanned as a table whose parts are the batches */
  private[plan] class SharedTable(val name: String, val schema: Schema, val shared: SharedResult)
      extends TableSource
      with CountedReads:
    protected def sharedResult: Option[SharedResult] = Some(shared)

    def scan(spec: ScanSpec): Iterator[Batch] =
      val sel = selector(spec)
      counted(shared.batches.iterator.map(sel))

    /** Selects the requested columns of a batch; the positions are looked up once per scan */
    private def selector(spec: ScanSpec): Batch => Batch =
      spec.columns match
        case None =>
          b => b
        case Some(cols) =>
          val idx = cols.map(c => schema.names.indexOf(c)).toArray
          b => b.select(idx)

    override def canSplit: Boolean = true

    override def scanSplits(spec: ScanSpec): Option[ScanSplits] =
      val bs = shared.batches.filter(_.size > 0)
      if bs.size <= 1 then
        None
      else
        val sel = selector(spec)
        Some(
          counted(
            ScanSplits(
              bs.toIndexedSeq.map(b => () => Iterator.single(sel(b))),
              () => (),
              bs.toIndexedSeq.map(_.size.toLong)
            )
          )
        )

    override def sliceSplits(spec: ScanSpec, parts: Int): Option[ScanSplits] =
      shared.batches.filter(_.size > 0) match
        case Seq(b) =>
          ScanSplits.slices(selector(spec)(b), parts).map(counted)
        case _ =>
          None

    override def explainDetail(indent: Int): Option[String] = shared.sharedPlan(indent)

  end SharedTable

  /**
    * A result read by several runtime-plan readers (`CTERef`s), such as the input of grouping sets:
    * `op`, the operator of `body`, is physically optimized and computed on first read
    */
  private[plan] class SharedInput(val name: String, val body: RuntimePlan, op: Operator)
      extends SharedCTE:
    private val shared = SharedResult(optimizePhysical(op))

    override def reader(
        schema: Schema,
        filter: Option[JoinCondition],
        setContext: Boolean
    ): Operator = shared.newReader(name)

  /** A shared result of `op` (lowered from `body`) for runtime-plan readers */
  private[engine] def sharedInput(name: String, body: RuntimePlan, op: Operator): SharedCTE =
    // An aggregation over joins in the body (the finest set of chained grouping sets) computed
    // below them, which the runtime passes over the plan reading this input don't reach
    val eager =
      if enabled(Optimization.EagerGroupingSets) then
        runtimeOptimizer.eagerAggregation(body)
      else
        body
    if eager eq body then
      SharedInput(name, body, op)
    else
      SharedInput(name, eager, lower(runtimeOptimizer.pushScanFilters(eager)))

  /**
    * A CTE read more than once. Its body is bound when it is first read, and lowered once every
    * reader is planned ([[finalizeCTEs]]), keeping only the columns the readers read, and only the
    * rows they keep when every reader reports its filter. A read before then (a subquery evaluated
    * at plan time) computes every column and row
    */
  private[plan] class DeferredCTE(val name: String) extends SharedCTE:
    // Set right after binding: the body can't read its own CTE. `body` is `bound` optimized
    var bound: RuntimePlan = null
    var body: RuntimePlan  = null
    // The CTEs that the body reads, which are finalized after this one
    val reads = mutable.LinkedHashSet.empty[DeferredCTE]
    // The body columns that the query reads by name; the ones after them are long keys added for
    // readers' joins ([[extendKey]])
    var visibleColumns = 0
    // The body position of the long key added for the string key at a body position
    private val extendedKeys = mutable.Map.empty[Int, Option[Int]]

    override def readerSchema: Schema = Schema(body.schema.fields.take(visibleColumns))

    /**
      * The body position of a long key that determines the string key at body `position`, added to
      * the body for joins of its readers ([[CTEJoinKeys]]), if the body can carry one. Only before
      * the body is lowered or computed
      */
    private def columns(p: RuntimePlan): Seq[(String, PType)] = p.output.map(c => (c.name, c.ptype))

    def extendKey(position: Int): Option[Int] = extendedKeys.getOrElseUpdate(
      position,
      // Positions are the optimized body's: the bound body must output the same columns
      if finalized.nonEmpty || rowCount.nonEmpty ||
        body.schema.names.distinct.size != body.schema.size || columns(bound) != columns(body)
      then
        None
      else
        CTEJoinKeys
          .extendBody(bound, position, () => runtimeBinder.newColumnId())
          .flatMap { extended =>
            if options.validatePlans then
              PlanValidator.validate(extended)
            // A separate run, so that firedRules counts the body's rules once
            val optimized = Optimizer
              .default(options, newColumnId = Some(() => runtimeBinder.newColumnId()))
              .run(extended)
            Option.when(columns(optimized) == columns(extended)) {
              bound = extended
              body = optimized
              body.output.size - 1
            }
          }
    )

    // Positions of the body output that readers read
    private val needed    = mutable.BitSet.empty
    private var needsAll  = false
    private var finalized = Option.empty[(IndexedSeq[Int], SharedResult)]
    // The filter each reader applies right above it, in creation order (None: none or unknown)
    private val readerFilters = mutable.ArrayBuffer.empty[Option[JoinCondition]]
    // Whether the result keeps only the rows the readers' filters agree on
    private var filtered = false
    // The reader filters pushed into the body
    private var pushed = Seq.empty[JoinCondition]
    // How many readers of the planned query read the result in a duplicate-insensitive context,
    // and how many don't ([[finalizeCTEs]])
    private var setReads   = 0
    private var otherReads = 0
    // Whether the body's joins were reduced for readers that read only its distinct rows
    private var reduced = false
    // The exact rows of the result, once materialized for planning
    private var rowCount          = Option.empty[Long]
    private lazy val label        = s"CTE[${name}]"
    private lazy val uniqueNames  = body.schema.names.distinct.size == body.schema.size
    private lazy val bodyPosition = body.schema.names.zipWithIndex.toMap

    def isFinalized: Boolean = finalized.nonEmpty

    /** Record that a reader reads these body output positions */
    def need(positions: Iterable[Int]): Unit = needed ++= positions

    /** Record that a reader of the planned query reads in a duplicate-insensitive context or not */
    def readIn(setContext: Boolean): Unit =
      if setContext then
        setReads += 1
      else
        otherReads += 1

    override def rows: Option[Long] = rowCount

    // Statistics read from the computed result: distinct counts by body position, and the rows
    // passing a condition (over the body's ids, so shared by readers). Keyed by the expression, as
    // signatures don't quote literals (`in ('a,b')` and `in ('a', 'b')` share one)
    private val distinctCounts = mutable.Map.empty[Int, Option[Double]]
    private val passingRows    = mutable.Map.empty[VExpr, Option[Long]]

    /**
      * Whether statistics can be read from the result: computed for planning, enabled, and small
      * enough that a pass over it costs little next to the query
      */
    private def hasResultStats: Boolean =
      rowCount.exists(_ <= Planner.MaxResultStatsRows) && enabled(Optimization.CTEReaderStats) &&
        !sharedResult.exists(_.isReleased)

    override def distinct(position: Int): Option[Double] =
      if !hasResultStats then
        None
      else
        distinctCounts.getOrElseUpdate(
          position, {
            val k = finalized.get._1.indexOf(position)
            Option
              .when(k >= 0)(k)
              .map { k =>
                (
                  if enabled(Optimization.ParallelResultStats) then
                    ParallelResultStats.distinct(batches, k, TaskRunner.default)
                  else
                    None
                ).getOrElse(
                    Stats.resultDistinct(
                      batches,
                      k,
                      body.schema(position).ptype,
                      primitive = enabled(Optimization.PrimitiveResultDistinct)
                    )
                  )
                  .toDouble
              }
          }
        )

    override def filteredRows(cond: BExpr): Option[Long] =
      if !hasResultStats then
        None
      else
        passingRows.getOrElseUpdate(
          cond.expr, {
            // Body column id -> position in the result batches (-1 for a dropped column)
            val keep = finalized.get._1
            val out  = body.output
            val pos  = Array.fill(out.map(_.id.value).max + 1)(-1)
            out.indices.foreach(i => pos(out(i).id.value) = keep.indexOf(i))
            if cond.refs.exists(r => r.value >= pos.length || pos(r.value) < 0) then
              None
            else if enabled(Optimization.ParallelResultStats) then
              ParallelResultStats.rows(batches, cond.lower(pos), TaskRunner.default)
            else
              Stats.resultRows(batches, cond.lower(pos))
          }
        )

    /** Compute the result now, and record its exact row count for planning */
    def materialize(): Unit =
      if rowCount.isEmpty then
        rowCount = Some(batches.map(_.size.toLong).sum)

    override def reader(
        schema: Schema,
        filter: Option[JoinCondition],
        setContext: Boolean
    ): Operator =
      // A reader with the filter of an earlier reader (a plan lowered again) keeps the rows that
      // the result keeps for that reader
      if filtered && !filter.exists(f => readerFilters.contains(Some(f))) then
        throw StatusCode
          .UNEXPECTED_STATE
          .newException(s"${label} is read after its result was filtered for its other readers")
      // A reduced result has the distinct rows of the body, not its duplicates
      if reduced && !setContext then
        throw StatusCode
          .UNEXPECTED_STATE
          .newException(s"${label} is read for its duplicates after its joins were reduced")
      if uniqueNames then
        readerFilters += filter
        // A reader bound before a long key was added for joins reads the columns before it
        ScanOp(
          CTETable(label, body.schema, this, setContext),
          schema,
          Option.when(schema.size < body.schema.size)(schema.fields.indices),
          None
        )
      else
        // An iterator reads every column and row
        readerFilters += None
        needsAll = true
        finalized.foreach { (keep, _) =>
          if keep.size != body.output.size then
            throw StatusCode
              .UNEXPECTED_STATE
              .newException(s"${label} is read by an iterator after it dropped columns")
        }
        CTEIteratorReader(this, schema, label)

    end reader

    /** The body positions of the result columns, and the result */
    private def result: (IndexedSeq[Int], SharedResult) =
      if finalized.isEmpty then
        needsAll = true
        finalizeBody(pushFilters = false)
      finalized.get

    /**
      * The plan of the body for the columns read so far, if not done yet. With `pushFilters`, once
      * every reader is planned, the body keeps only the rows the readers' filters agree on
      */
    def finalizeBody(pushFilters: Boolean): Unit =
      if finalized.isEmpty then
        val agreed =
          if pushFilters && !needsAll && readerFilters.nonEmpty &&
            readerFilters.forall(_.nonEmpty) && enabled(Optimization.PushDownFilters) &&
            enabled(Optimization.PushDownCTEFilters)
          then
            PushDownFilters.shared(readerFilters.flatten.toSeq)
          else
            Nil
        val body =
          if agreed.isEmpty then
            this.body
          else
            runtimeOptimizer.pushFilters(this.body, agreed)
        filtered = agreed.nonEmpty
        pushed = agreed
        reduced =
          !needsAll && setReads > 0 && otherReads == 0 && enabled(Optimization.ReduceSemiJoins)
        val keep =
          if needsAll || needed.isEmpty || !enabled(Optimization.PruneColumns) then
            body.output.indices
          else
            needed.toIndexedSeq.sorted
        finalized = Some((keep, SharedResult(lower(body, keep))))

    /** The result operator of a body (filters pushed) for the body output positions `keep` */
    private def lower(body: RuntimePlan, keep: IndexedSeq[Int]): Operator =
      val out    = body.output
      val pruned =
        val p =
          if keep.size == out.size then
            body
          else
            runtimeOptimizer.pruneColumns(body, keep.map(out(_).id).toSet)
        if reduced then
          runtimeOptimizer.reduceSemiJoins(p, keep.map(out(_).id).toSet)
        else
          p
      // The readers' filters pushed into the body change the estimates that outer build sides
      // and eager aggregation read (TPC-DS q04, q11: `d_year in (2001, 2002)`), so they run
      // again. A reduced body's readers read only its distinct rows, as do the CTE readers in it
      val op = SharedResults.this.lower(
        runtimeOptimizer.pushScanFilters(
          if pushed.isEmpty then
            pruned
          else
            runtimeOptimizer.afterReaderFilters(pruned)
        ),
        reduced
      )
      val pos      = pruned.output.map(_.id).zipWithIndex.toMap
      val selected =
        if pruned.output.size == keep.size then
          op
        else
          ProjectOp(
            op,
            keep.map(i => ColRef(pos(out(i).id), out(i).ptype, out(i).name)),
            Schema(keep.map(i => body.schema(i)))
          )
      optimizePhysical(selected)

    end lower

    /** Whether the body can be planned again: finalized, and not computed yet */
    def canReplan: Boolean = bound != null && finalized.exists(!_._2.isComputed)

    /**
      * Replace the result with the plan of `replanned` (the body optimized again, with the same
      * output columns), keeping the columns and rows the result keeps for the readers
      */
    def replan(replanned: RuntimePlan): Unit = finalized.foreach { (keep, _) =>
      // Joins output columns in join order: restore the body's order, which keep refers to
      val ordered =
        if replanned.output == body.output then
          replanned
        else if replanned.output.map(_.id).toSet != body.output.map(_.id).toSet then
          throw StatusCode
            .UNEXPECTED_STATE
            .newException(s"${label} re-planned outputs other columns")
        else
          RuntimePlan.Project(replanned, body.output.map(BExpr.ref), body.output)
      val filteredBody =
        if pushed.isEmpty then
          ordered
        else
          runtimeOptimizer.pushFilters(ordered, pushed)
      val op = lower(filteredBody, keep)
      if !readsKeptColumns(op) then
        throw StatusCode
          .UNEXPECTED_STATE
          .newException(s"${label} re-planned reads a column a CTE result dropped")
      finalized = Some((keep, SharedResult(op)))
    }

    /** The result operator, once finalized */
    def resultOp: Option[Operator] = finalized.map(_._2.op)

    /** The result, once finalized */
    def sharedResult: Option[SharedResult] = finalized.map(_._2)

    /** Whether the result keeps the columns of a scan (body output positions; None: all) */
    def keeps(columns: Option[IndexedSeq[Int]]): Boolean = finalized.forall { (keep, _) =>
      columns.getOrElse(body.output.indices).forall(keep.contains)
    }

    /** Selects the columns of a scan (body column names) from the result batches */
    def selector(columns: Option[Seq[String]]): Batch => Batch =
      val (keep, _) = result
      val idx = columns.getOrElse(body.schema.names).map(bodyPosition).map(keep.indexOf).toArray
      if idx.contains(-1) then
        throw StatusCode
          .UNEXPECTED_STATE
          .newException(
            s"${label} reads ${columns
                .getOrElse(Nil)
                .mkString(", ")}, which its result doesn't keep"
          )
      if idx.sameElements(keep.indices) then
        b => b
      else
        b => b.select(idx)

    def batches: Seq[Batch]                     = result._2.batches
    def sharedPlan(indent: Int): Option[String] = result._2.sharedPlan(indent)

  end DeferredCTE

  /** A reader of every column and row of a deferred CTE, for bodies with duplicate column names */
  private[plan] class CTEIteratorReader(val cte: DeferredCTE, schema: Schema, label: String)
      extends IteratorOp(schema, () => cte.batches.iterator, label):
    override protected def extraInputs(indent: Int): Seq[String] = cte.sharedPlan(indent).toSeq
    override def sharedInputs: Seq[Operator]                     = cte.resultOp.toSeq

  /**
    * `results` and the shared results that `roots` and `results` read, directly or through other
    * shared results. A result comes after the results it reads
    */
  private[plan] def sharedResultsOf(
      roots: Seq[Operator],
      results: Seq[SharedResult] = Nil
  ): Seq[SharedResult] =
    val visited                    = java.util.IdentityHashMap[AnyRef, Unit]()
    val out                        = mutable.ArrayBuffer.empty[SharedResult]
    def add(r: SharedResult): Unit =
      if !visited.containsKey(r) then
        visited.put(r, ())
        walk(r.op)
        out += r
    def walk(op: Operator): Unit =
      if !visited.containsKey(op) then
        visited.put(op, ())
        op match
          case s: ScanOp =>
            s.source match
              case t: CTETable =>
                t.cte.sharedResult.foreach(add)
              case t: SharedTable =>
                add(t.shared)
              case _ =>
          case r: SharedReader =>
            add(r.shared)
          case r: CTEIteratorReader =>
            r.cte.sharedResult.foreach(add)
          case _ =>
        (op.children ++ op.sharedInputs).foreach(walk)
    roots.foreach(walk)
    results.foreach(add)
    out.toSeq

  end sharedResultsOf

  /** Whether shared results are computed concurrently, on coordinator threads */
  private[plan] def concurrentSharedResults: Boolean =
    enabled(Optimization.ConcurrentSharedCTEs) && enabled(Optimization.Parallelize) &&
      TaskRunner.default.parallelism > 1

  /**
    * Compute `results` and the shared results they read, those not computed yet at the same time on
    * coordinator threads, and wait until all are done (their failures are raised when read). With
    * `fair_coordinators`, this thread computes, in order, those no coordinator has started, so
    * coordinators busy with other queries' work can't stall it
    */
  private[plan] def computeSharedResults(results: Seq[SharedResult]): Unit =
    if concurrentSharedResults then
      val pending = sharedResultsOf(Nil, results).filterNot(_.isComputed)
      if pending.size > 1 then
        val handles = pending.map(r => TaskRunner.default.coordinate(() => r.compute(() => false)))
        if enabled(Optimization.FairCoordinators) then
          pending.foreach(_.compute(() => false))
          pending.foreach(_.awaitIfStarted())
          // Queued coordinator entries of results computed here would only find them started
          handles.foreach(_.cancel())
        else
          handles.foreach(_.get())

  /**
    * The plan `root`, which starts computing the shared results it reads on coordinator threads
    * when it starts running ([[StartSharedOp]]), when there are several
    */
  private[plan] def startingSharedResults(root: Operator): Operator =
    if !concurrentSharedResults then
      root
    else
      val pending = sharedResultsOf(Seq(root)).filterNot(_.isComputed)
      if pending.size > 1 then
        StartSharedOp(root, pending, TaskRunner.default)
      else
        root

  /**
    * The result of a deferred CTE, scanned as a table whose parts are its batches. Its schema is
    * the body's; readers rename the columns in their own schema
    */
  private[plan] class CTETable(
      val name: String,
      val schema: Schema,
      val cte: DeferredCTE,
      val setContext: Boolean = false
  ) extends TableSource
      with CountedReads:
    protected def sharedResult: Option[SharedResult] = cte.sharedResult

    def scan(spec: ScanSpec): Iterator[Batch] =
      val sel = cte.selector(spec.columns)
      counted(cte.batches.iterator.map(sel))

    override def estimatedRows: Option[Long] = cte.rows

    override def estimatedDistinct(column: String): Option[Double] =
      val i = schema.names.indexOf(column)
      if i < 0 then
        None
      else
        cte.distinct(i)

    override def canSplit: Boolean = true

    override def scanSplits(spec: ScanSpec): Option[ScanSplits] =
      val bs = cte.batches.filter(_.size > 0)
      if bs.size <= 1 then
        None
      else
        val sel = cte.selector(spec.columns)
        Some(
          counted(
            ScanSplits(
              bs.toIndexedSeq.map(b => () => Iterator.single(sel(b))),
              () => (),
              bs.toIndexedSeq.map(_.size.toLong)
            )
          )
        )

    override def sliceSplits(spec: ScanSpec, parts: Int): Option[ScanSplits] =
      cte.batches.filter(_.size > 0) match
        case Seq(b) =>
          ScanSplits.slices(cte.selector(spec.columns)(b), parts).map(counted)
        case _ =>
          None

    override def explainDetail(indent: Int): Option[String] = cte.sharedPlan(indent)

  end CTETable

  /**
    * Lower the bodies of the shared CTEs once every reader is planned: each keeps the columns its
    * readers' scans read after physical pruning, in `root` and in the bodies of the CTEs that read
    * it, which are finalized first
    */
  private[plan] def finalizeCTEs(root: Operator): Unit =
    val visited                     = java.util.IdentityHashMap[AnyRef, Unit]()
    def collect(op: Operator): Unit =
      if !visited.containsKey(op) then
        visited.put(op, ())
        op match
          case s: ScanOp =>
            s.source match
              case t: CTETable =>
                // The table schema is the body's
                t.cte.need(s.columns.getOrElse(t.schema.fields.indices))
                t.cte.readIn(t.setContext)
              case t: SharedTable =>
                collect(t.shared.op)
              case _ =>
          case _ =>
        (op.children ++ op.sharedInputs).foreach(collect)

    collect(root)
    var pending = deferredCTEs.toList
    while pending.nonEmpty do
      // Not read by another pending CTE's body; CTEs aren't recursive, so one always is
      val ready = pending.filterNot(c => pending.exists(_.reads.contains(c)))
      val next  =
        if ready.isEmpty then
          pending
        else
          ready
      next.foreach { c =>
        c.finalizeBody(pushFilters = true)
        finalizeOrder += c
        c.resultOp.foreach(collect)
      }
      pending = pending.filterNot(next.contains)

  end finalizeCTEs

  /**
    * Let each shared result that `root` reads drop its batches once the scans that read it in
    * `root` (and in the results it reads) have finished (`release_cte_results`), instead of when
    * the query ends. Counted from the final operator tree, as readers made while binding may be
    * discarded. A result with a reader of another kind (over duplicate column names) is kept
    */
  private[plan] def armSharedReads(root: Operator): Unit =
    if enabled(Optimization.ReleaseCTEResults) && memory.isTracking then
      val scans = mutable.LinkedHashMap.empty[SharedResult, List[CountedReads]]
      val kept  = mutable.Set.empty[SharedResult]
      def scanOf(r: SharedResult, t: CountedReads): Unit = scans(r) = t :: scans.getOrElse(r, Nil)
      val visited                                        = java.util.IdentityHashMap[AnyRef, Unit]()
      // The bodies of results computed while planning (to re-plan with their row counts) have
      // read their inputs already: their scans would never report
      sharedResultsOf(Seq(root)).filter(_.isComputed).foreach(r => visited.put(r.op, ()))
      def walk(op: Operator): Unit =
        if !visited.containsKey(op) then
          visited.put(op, ())
          op match
            case s: ScanOp =>
              s.source match
                case t: CTETable =>
                  t.cte.sharedResult.foreach(r => scanOf(r, t))
                  t.cte.resultOp.foreach(walk)
                case t: SharedTable =>
                  scanOf(t.shared, t)
                  walk(t.shared.op)
                case _ =>
            case r: SharedReader =>
              kept += r.shared
            case r: CTEIteratorReader =>
              r.cte.sharedResult.foreach(kept += _)
            case _ =>
          (op.children ++ op.sharedInputs).foreach(walk)
      walk(root)
      scans.foreach { (r, ts) =>
        if !kept.contains(r) then
          ts.foreach(_.armRead())
          r.armReads(ts.size)
      }

  /**
    * A reader table of a shared result that reports to it when a read of the planned query has
    * finished: a scan's iterator ran out, or the splits a [[MorselOp]] read were closed
    */
  private[plan] trait CountedReads:
    protected def sharedResult: Option[SharedResult]
    // Reads of the planned query through this table not yet finished
    private val armed = java.util.concurrent.atomic.AtomicInteger(0)

    def armRead(): Unit = armed.incrementAndGet()

    private def finished(): Unit =
      if armed.getAndUpdate(n => math.max(0, n - 1)) > 0 then
        sharedResult.foreach(_.readDone())

    protected def counted(it: Iterator[Batch]): Iterator[Batch] =
      new Iterator[Batch]:
        private var done     = false
        def hasNext: Boolean =
          val h = it.hasNext
          if !h && !done then
            done = true
            finished()
          h
        def next(): Batch = it.next()

    protected def counted(s: ScanSplits): ScanSplits =
      val once = java.util.concurrent.atomic.AtomicBoolean(false)
      s.copy(close =
        () =>
          s.close()
          if once.compareAndSet(false, true) then
            finished()
      )

  end CountedReads

end SharedResults
