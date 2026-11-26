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

import wvlet.lang.api.Span.NoSpan
import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.codegen.CodeFormatterConfig
import wvlet.lang.compiler.codegen.SqlGenerator
import wvlet.lang.compiler.DBType
import wvlet.lang.engine.catalog.*
import wvlet.lang.engine.exec.*
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.runtime.RuntimeBinder
import wvlet.lang.engine.runtime.Stats
import wvlet.lang.engine.runtime.BExpr
import wvlet.lang.engine.runtime.JoinCondition
import wvlet.lang.engine.runtime.Lowering
import wvlet.lang.engine.runtime.Optimizer
import wvlet.lang.engine.runtime.PlanAccelerator
import wvlet.lang.engine.runtime.PlanValidator
import wvlet.lang.engine.runtime.PushDownFilters
import wvlet.lang.engine.runtime.RuntimePlan
import wvlet.lang.engine.runtime.ShareSubplans
import wvlet.lang.engine.runtime.SharedCTE
import wvlet.lang.engine.vector.*
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.uni.log.LogSupport

import scala.collection.mutable
import scala.util.control.NonFatal

/**
  * Translates a typed, model-expanded wvlet [[Relation]] into a tree of physical [[Operator]]s.
  * Column names are resolved against the runtime schemas of the operators (not the compiler's
  * types), so queries over files with unknown schemas plan the same way as catalog tables.
  */
class Planner(
    catalog: EngineCatalog,
    sources: SourceResolver,
    private[engine] val options: OptimizerOptions = OptimizerOptions(),
    accelerator: Option[PlanAccelerator] = None
)(using ctx: Context)
    extends LogSupport:

  private[engine] def enabled(o: Optimization): Boolean = options.isEnabled(o)

  /** Lower a runtime plan into operators, with runtime join filters when enabled */
  private[engine] def lower(p: RuntimePlan, inSet: Boolean = false): Operator = Lowering(
    p,
    joinFilters = enabled(Optimization.RuntimeJoinFilters),
    rowJoinFilters = enabled(Optimization.RowJoinFilters),
    buildSideJoinFilters = enabled(Optimization.BuildSideJoinFilters),
    presizeAggs = enabled(Optimization.PresizeAggregates),
    inSet = inSet,
    accelerator = accelerator.filter(_ => enabled(Optimization.GpuOffload)),
    footerCache = enabled(Optimization.ParquetFooterCache),
    windowTopN = enabled(Optimization.WindowTopN),
    windowKernels = enabled(Optimization.RunningWindowKernels),
    commonSubexpressions = enabled(Optimization.CommonSubexpressions),
    foldConstants = enabled(Optimization.FoldConstants),
    windowSortKernels = enabled(Optimization.WindowSortKernels),
    scanRowFilters = enabled(Optimization.ScanRowFilters),
    dictionaryCodes = enabled(Optimization.DictionaryGroupKeys),
    dictionaryEntries = enabled(Optimization.DictionaryEntryScans),
    lazyStrings = enabled(Optimization.LazyDictionaryStrings),
    selectiveRowFilters = enabled(Optimization.SelectiveScanRowFilters),
    maskedCaseWhen = enabled(Optimization.MaskedCaseWhen),
    windowSharePartitions = enabled(Optimization.WindowSharePartitions),
    windowTopNStream = enabled(Optimization.WindowTopNStream),
    constCaseSelect = enabled(Optimization.ConstCaseSelect),
    caseAggFilters = enabled(Optimization.CaseAggFilters),
    substringInLists = enabled(Optimization.SubstringInLists),
    valueSelectivity = enabled(Optimization.StringValueSelectivity),
    columnImpliedRowFilters = enabled(Optimization.ColumnImpliedRowFilters),
    deferredNullChecks = enabled(Optimization.DeferredNullChecks),
    fusedDecimalArith = enabled(Optimization.FusedDecimalArith)
  )

  /** Whether a join builds on the other input, as this one has much more rows than that */
  private[engine] def swapBuild(rows: Double, otherRows: Double): Boolean = JoinOrderOptimizer
    .swapBuild(options, rows, otherRows)

  /**
    * Whether a semi / anti join builds on its outer input, as the subquery input (`rows`) is larger
    * ([[JoinOrderOptimizer.swapSemiBuild]])
    */
  private[engine] def swapSemiBuild(rows: Double, otherRows: Double, aggregates: Boolean): Boolean =
    JoinOrderOptimizer.swapSemiBuild(options, rows, otherRows, aggregates)

  /**
    * The physical passes over a lowered subquery computed at plan time, as [[plan]] runs them for a
    * nested relation, so that it runs in parallel
    */
  private[engine] def physical(op: Operator): Operator = optimizePhysical(op)

  /** The physical passes over a lowered plan: column pruning (with scan filters), parallelism */
  private def optimizePhysical(op: Operator): Operator =
    val pruned = ColumnPruner(
      op,
      pruneColumns = enabled(Optimization.PruneColumns),
      scanFilters = enabled(Optimization.PushScanFilters),
      numericScanFilters = enabled(Optimization.DecimalFloatScanFilters)
    )
    if !enabled(Optimization.LateGather) then
      Planner.disableLateGather(pruned)
    if !enabled(Optimization.DenseSelectedRows) then
      Planner.disableDenseSelected(pruned)
    if !enabled(Optimization.ScanJoinRowFilters) then
      Planner.disableSourceJoinFilters(pruned)
    if !enabled(Optimization.JoinRowFiltersFirst) then
      Planner.disableJoinFiltersFirst(pruned)
    if !enabled(Optimization.SparseKeyPassRates) then
      Planner.disableSparseKeyPassRates(pruned)
    if !enabled(Optimization.ShortCircuitFilters) || !enabled(Optimization.ReorderConjuncts) ||
      !enabled(Optimization.DictionaryFilters) || !enabled(Optimization.DictionarySelection)
    then
      Planner.configureFilters(
        pruned,
        shortCircuit = enabled(Optimization.ShortCircuitFilters),
        reorder = enabled(Optimization.ReorderConjuncts),
        dictionary = enabled(Optimization.DictionaryFilters),
        dictionarySelection = enabled(Optimization.DictionarySelection)
      )
    if !enabled(Optimization.JoinDirectIndex) then
      Planner.disableJoinDirectIndex(pruned)
    if !enabled(Optimization.ParallelJoinBuild) then
      Planner.disableParallelJoinBuild(pruned)
    if !enabled(Optimization.JoinBuildStringCodes) then
      Planner.disableJoinBuildStringCodes(pruned)
    // Before parallelizing: the per-part aggregations take the option from these operators
    if !enabled(Optimization.ColumnMajorFusedSums) then
      Planner.disableColumnMajorSums(pruned)
    val physical =
      if enabled(Optimization.Parallelize) then
        Parallelizer(
          pruned,
          TaskRunner.default,
          aboveBreakers = enabled(Optimization.ParallelAboveBreakers),
          parallelPartitioning = enabled(Optimization.ParallelAggPartitioning),
          concurrentBuilds = enabled(Optimization.ConcurrentJoinBuilds),
          sharedPartials = enabled(Optimization.SharedAggPartials),
          adaptivePartials = enabled(Optimization.AdaptivePartialAggs),
          setOpJoins = enabled(Optimization.SetOpJoins),
          packAggKeys = enabled(Optimization.PackAggKeys),
          streamPackAggKeys = enabled(Optimization.StreamPackAggKeys),
          longDecimalKeys = enabled(Optimization.LongDecimalKeys),
          unionPipelines = enabled(Optimization.UnionAllPipelines),
          fusedAccumulators = enabled(Optimization.FusedAccumulators),
          oneRowPartials = enabled(Optimization.OneRowPartials),
          selectedInputs = enabled(Optimization.AggregateSelectedRows),
          longGroupTables = enabled(Optimization.LongGroupTables),
          sortedGroupRuns = enabled(Optimization.SortedGroupRuns),
          unionDistinctOnce = enabled(Optimization.UnionDistinctOnce),
          anyValuePartials = enabled(Optimization.DependentGroupKeys)
        )
      else
        pruned
    val streamPack   = enabled(Optimization.StreamPackAggKeys)
    val longDecimals = enabled(Optimization.LongDecimalKeys)
    if !streamPack || !longDecimals then
      Planner.setAggKeyOptions(physical, streamPack, longDecimals)
    if !enabled(Optimization.FusedAccumulators) then
      Planner.disableFusedAccumulators(physical)
    if !enabled(Optimization.AggregateSelectedRows) then
      Planner.disableSelectedInputs(physical)
    if !enabled(Optimization.ColumnMajorFusedSums) then
      Planner.disableColumnMajorSums(physical)
    if !enabled(Optimization.DenseSelectedRows) then
      Planner.disableDenseSelected(physical)
    if !enabled(Optimization.LongGroupTables) then
      Planner.disableLongGroupTables(physical)
    if !enabled(Optimization.SortedGroupRuns) then
      Planner.disableSortedGroupRuns(physical)
    if !enabled(Optimization.SortedAggSplits) then
      Planner.disableSortedAggSplits(physical)
    if !enabled(Optimization.PackedAggRouting) then
      Planner.disablePackedAggRouting(physical)
    physical

  end optimizePhysical

  private var hiddenSeq                                  = 0
  private[engine] def hiddenName(prefix: String): String =
    hiddenSeq += 1
    s"__wv_${prefix}${hiddenSeq}"

  private[engine] def isHidden(f: Field): Boolean = f.name.startsWith("__wv_")

  // Named relations of WITH clauses
  private val cteDefs = mutable.HashMap.empty[String, AliasedRelation]
  // How many times each CTE is referenced: one read more than once is shared
  private val cteRefCounts = mutable.HashMap.empty[String, Int]
  // Shared CTEs by name, and every one created, in order
  private val cteRefs      = mutable.HashMap.empty[String, (AliasedRelation, DeferredCTE)]
  private val deferredCTEs = mutable.ArrayBuffer.empty[DeferredCTE]
  // The shared CTEs in the order their bodies were finalized (readers' bodies first)
  private val finalizeOrder = mutable.ArrayBuffer.empty[DeferredCTE]
  // The shared CTEs whose bodies are being bound, innermost first
  private var bindingCTEs = List.empty[DeferredCTE]
  // Nesting of plan(): subqueries are planned by this planner too
  private var planDepth = 0
  // Uncorrelated scalar subqueries are evaluated once per query
  private val scalarResults = java.util.IdentityHashMap[Relation, VExpr]()
  // Scalar subqueries prefetchScalarSubqueries has considered, so nested plans don't retry them
  private val prefetchTried = java.util.IdentityHashMap[Relation, Unit]()
  // Groups computed at plan time that aggregations of an IN subquery over the same input merge
  // (`share_in_subquery_groups`)
  private val sharedGroups = mutable.ArrayBuffer.empty[SharedGroups]
  // Aggregation inputs whose IN filter couldn't share groups, planned with the filter instead
  private val inShareDeclined = java.util.IdentityHashMap[Relation, Unit]()
  // Groups computed for an IN filter and the subquery's values, by the input's loose key, keys,
  // calls, and IN target: an aggregation planned again (e.g. for a hidden sort key) reuses them
  private val inGroupsComputed = mutable.HashMap.empty[Any, (Batch, Batch, PType)]

  /** Window function calls collected while binding a projection */
  private var windowCollector: Option[WindowCollector] = None

  /**
    * The runtime plans of the outermost relation of the last [[plan]] call, before and after the
    * logical passes, for tools that show them (`WvletEngine.planStages`)
    */
  private[engine] var rootPlans: Option[RootPlans] = None

  /** Plan a relation, then drop columns no operator needs so that scans read fewer columns */
  def plan(rel: Relation): Operator =
    planDepth += 1
    try
      val r =
        if planDepth == 1 && enabled(Optimization.ShareSubqueryScans) &&
          enabled(Optimization.ShareCTEs)
        then
          SharedScans.rewrite(this, rel)
        else
          rel
      if enabled(Optimization.PrefetchScalarSubqueries) then
        prefetchScalarSubqueries(r)
      if planDepth == 1 then
        planRoot(r)
      else
        optimizePhysical(planRelation(r))
    finally
      planDepth -= 1

  /**
    * Plan the outermost relation: lower it, then the shared CTE bodies for the columns and rows its
    * readers read. When joins read a shared CTE, compute it and order the joins again with its
    * exact row count ([[replanJoinedCTEs]])
    */
  private def planRoot(r: Relation): Operator =
    try
      val bound     = runtimeBinder.bind(r)
      val optimized = optimizeRuntime(bound)
      rootPlans = Some(RootPlans(bound, optimized))
      val op = optimizePhysical(lower(optimized))
      finalizeCTEs(op)
      val root =
        if enabled(Optimization.ReplanMaterializedCTEs) then
          if enabled(Optimization.ReplanCTEBodies) then
            replanCTEBodies()
          replanJoinedCTEs(bound, optimized).getOrElse(op)
        else
          op
      startingSharedResults(root)
    finally
      runtimeBinder.clearBoundInputs()

  /**
    * Compute the shared CTEs that joins of `bound` read (inner join graphs, and outer, semi, and
    * anti hash joins, whose build side depends on the rows), and run the logical passes over
    * `bound` again, which now read their exact row counts. Semi and anti joins, which the binder
    * builds itself, choose their build side again when they read such a CTE
    * (`replan_semi_join_builds`). The re-optimized plan, lowered, if it differs from `optimized`.
    * Inputs the binder optimized while binding (aggregation, pivot, and subquery inputs, whose
    * joins are ordered in `bound`) are optimized again from their bound plans when they read such a
    * CTE (`replan_bound_cte_joins`). The shared input of grouping sets or unpivot is followed into
    * its body, which is refreshed the same way and lowered again as a new shared input when it
    * changes (`replan_shared_inputs`).
    *
    * A CTE's readers read the same columns and filters in any join order, so the second plan reads
    * the result the first plan's readers finalized ([[DeferredCTE.reader]]); if it can't, the first
    * plan is kept (with `validatePlans`, the error is raised). The CTE bodies that join other
    * shared CTEs are re-planned before this ([[replanCTEBodies]])
    */
  private def replanJoinedCTEs(bound: RuntimePlan, optimized: RuntimePlan): Option[Operator] =
    replanRuntime(bound, optimized).flatMap { replanned =>
      try
        val op = optimizePhysical(lower(replanned))
        if !readsKeptColumns(op) then
          throw StatusCode
            .UNEXPECTED_STATE
            .newException("A re-planned shared CTE reader reads a column its result dropped")
        Some(op)
      catch
        case e: WvletLangException
            if e.statusCode == StatusCode.UNEXPECTED_STATE && !options.validatePlans =>
          warn(s"Keeping the first plan: ${e.getMessage}")
          None
    }

  /**
    * The logical plan of `bound` optimized again once the shared CTEs that its joins read are
    * computed ([[replanJoinedCTEs]]), if it differs from `optimized`
    */
  private def replanRuntime(bound: RuntimePlan, optimized: RuntimePlan): Option[RuntimePlan] =
    import RuntimePlan.*
    val boundInputs  = enabled(Optimization.ReplanBoundCTEJoins)
    val semiJoins    = enabled(Optimization.ReplanSemiJoinBuilds)
    val sharedInputs = enabled(Optimization.ReplanSharedInputs)
    // An input the binder optimized while binding is read in its bound form, with unordered joins.
    // An optimized input may be a node of its own bound plan (e.g. `Filter(p, true)` optimizes to
    // `p`), so an input being expanded reads as itself
    val expanding = java.util.IdentityHashMap[RuntimePlan, Unit]()
    def boundInput(p: RuntimePlan): Option[RuntimePlan] =
      if boundInputs && !expanding.containsKey(p) then
        runtimeBinder.boundInput(p)
      else
        None
    def expand[A](p: RuntimePlan)(f: => A): A =
      expanding.put(p, ())
      try f
      finally expanding.remove(p)
    val joined = mutable.LinkedHashSet.empty[DeferredCTE]
    // The shared inputs of grouping sets or unpivot (read by several readers) whose bodies were
    // visited
    val visited = java.util.IdentityHashMap[SharedInput, Unit]()
    def collect(p: RuntimePlan, underJoin: Boolean): Unit =
      p match
        case CTERef(cte: DeferredCTE, _) if underJoin =>
          joined += cte
        case CTERef(s: SharedInput, _) if sharedInputs =>
          // Its readers are not joins: only the joins in its body decide what it collects
          if !visited.containsKey(s) then
            visited.put(s, ())
            collect(s.body, underJoin = false)
        case _ =>
          boundInput(p) match
            case Some(input) =>
              expand(p)(collect(input, underJoin))
            case None =>
              collectChildren(p, underJoin)
    def collectChildren(p: RuntimePlan, underJoin: Boolean): Unit =
      val below =
        underJoin || (
          p match
            case _: InnerJoins =>
              true
            case j: Join =>
              j.leftKeys.nonEmpty && (
                j.kind == JoinKind.Left || j.kind == JoinKind.Right || j.kind == JoinKind.Full ||
                  semiJoins &&
                  (j.kind == JoinKind.Semi || j.kind == JoinKind.Anti)
              )
            case _ =>
              false
        )
      p.children.foreach(collect(_, below))
    collect(bound, underJoin = false)
    if joined.isEmpty then
      None
    else
      // The joined results (and the shared results they read) are computed together
      computeSharedResults(joined.toSeq.flatMap(_.sharedResult))
      joined.foreach(_.materialize())
      // A separate run, so that firedRules counts the rules of the plan once
      val optimizer = Optimizer.default(options, semiJoins, Some(() => runtimeBinder.newColumnId()))
      val sharedReadsJoined                    = java.util.IdentityHashMap[SharedInput, Boolean]()
      def readsJoined(p: RuntimePlan): Boolean =
        p match
          case CTERef(cte: DeferredCTE, _) =>
            joined.contains(cte)
          case CTERef(s: SharedInput, _) if sharedInputs =>
            // Each reader of a shared input reads the same body: walk it once
            if !sharedReadsJoined.containsKey(s) then
              sharedReadsJoined.put(s, readsJoined(s.body))
            sharedReadsJoined.get(s)
          case _ =>
            p.children.exists(readsJoined)
      // Shared inputs refreshed so far: every reader of one reads the same new input
      val refreshed = java.util.IdentityHashMap[SharedInput, SharedInput]()
      def refreshShared(s: SharedInput): SharedInput =
        if refreshed.containsKey(s) then
          refreshed.get(s)
        else
          val r =
            if !readsJoined(s.body) then
              s
            else
              val body = keepOutput(s.body, refresh(s.body))
              if body == s.body then
                s
              else
                try
                  SharedInput(s.name, body, lower(body))
                catch
                  case e: WvletLangException
                      if e.statusCode == StatusCode.UNEXPECTED_STATE && !options.validatePlans =>
                    warn(s"Keeping the first plan of ${s.name}: ${e.getMessage}")
                    s
          refreshed.put(s, r)
          r
      // `o`, optimized from `p`, outputting p's columns in p's order; p if o outputs other columns
      def keepOutput(p: RuntimePlan, o: RuntimePlan): RuntimePlan =
        if o.output == p.output then
          o
        else if o.output.map(_.id).toSet == p.output.map(_.id).toSet then
          // Joins output columns in join order: restore the first order, which a parent may
          // read by position (e.g. a union)
          Project(o, p.output.map(BExpr.ref), p.output)
        else
          p
      // Optimize again, from the bottom up, the bound inputs that read a computed CTE
      def refresh(p: RuntimePlan): RuntimePlan =
        boundInput(p) match
          case Some(input) if readsJoined(input) =>
            keepOutput(p, optimizer.run(expand(p)(refresh(input))))
          case _ =>
            p match
              case CTERef(s: SharedInput, columns) if sharedInputs =>
                val r = refreshShared(s)
                if r eq s then
                  p
                else
                  CTERef(r, columns)
              case _ =>
                p.withChildren(p.children.map(refresh))
      val rebound = refresh(bound)
      if options.validatePlans then
        PlanValidator.validate(rebound)
      val replanned = optimizer.run(rebound)
      if replanned == optimized then
        None
      else
        Some(replanned)
    end if

  end replanRuntime

  /**
    * Order the joins of the shared CTE bodies that read other shared CTEs again, once those are
    * computed: bodies are finalized before the CTEs they read (whose columns and rows their readers
    * decide), so the inner CTEs are computed and each body re-planned afterwards, innermost first,
    * keeping the columns and rows its result keeps for its own readers
    */
  private def replanCTEBodies(): Unit = finalizeOrder
    .reverse
    .foreach { c =>
      if c.reads.nonEmpty && c.canReplan then
        replanRuntime(c.bound, c.body).foreach { replanned =>
          try
            c.replan(replanned)
          catch
            case e: WvletLangException
                if e.statusCode == StatusCode.UNEXPECTED_STATE && !options.validatePlans =>
              warn(s"Keeping the first plan of CTE[${c.name}]: ${e.getMessage}")
        }
    }

  /**
    * Whether every scan of a finalized shared CTE in `root` reads only columns its result keeps; a
    * plan lowered again after finalization must, or it would fail when it runs
    */
  private def readsKeptColumns(root: Operator): Boolean =
    val visited                      = java.util.IdentityHashMap[AnyRef, Unit]()
    def check(op: Operator): Boolean =
      visited.containsKey(op) || {
        visited.put(op, ())
        val ok =
          op match
            case s: ScanOp =>
              s.source match
                case t: CTETable =>
                  t.cte.keeps(s.columns)
                case t: SharedTable =>
                  check(t.shared.op)
                case _ =>
                  true
            case _ =>
              true
        ok && (op.children ++ op.sharedInputs).forall(check)
      }
    check(root)

  /**
    * The names of the CTEs in scope and of those `r` defines. CTE names are resolved by scope while
    * planning, so a relation reading one of these names may read a CTE
    */
  private[engine] def cteNamesIn(r: Relation): Set[String] =
    val names = mutable.Set.from(cteDefs.keys)
    r.traverse { case w: WithQuery =>
      w.queryDefs.foreach(d => names += d.alias.leafName.toLowerCase)
    }
    names.toSet

  /**
    * Whether `rel` reads a table named as one of `cteNames`, or a shared scan's CTE through a
    * filter the tree doesn't show it in ([[sharedScanOf]])
    */
  private def readsCTE(rel: Relation, cteNames: Set[String]): Boolean =
    var found = false
    rel.traverse {
      case f: Filter if sharedScans.containsKey(f) =>
        found = true
      case t: TableRef if cteNames.contains(t.name.leafName.toLowerCase) =>
        found = true
      case t: TableScan if cteNames.contains(t.name.name.toLowerCase) =>
        found = true
    }
    found

  /**
    * A key of the bound runtime plan of `rel` ([[ShareSubplans.key]]), equal for inputs that read
    * the same rows ([[boundInput]])
    */
  private def inputKey(rel: Relation): Option[Any] = boundInput(rel).flatMap(ShareSubplans.key)

  /**
    * The loose key of the bound runtime plan of `rel` ([[ShareSubplans.looseKey]]), with the
    * canonical id of each column of `schema`, the schema `rel` plans to: equal for inputs that read
    * the same rows, whatever the order of their inner-join inputs or their table aliases. None when
    * a column doesn't name exactly one column of the bound plan
    */
  private def looseInputKey(rel: Relation, schema: Schema): Option[(Any, IndexedSeq[Int])] =
    boundInput(rel).flatMap { p =>
      val out = p.output
      ShareSubplans
        .looseKey(p)
        .flatMap { (key, outIds) =>
          // The planner may order the columns otherwise: each column is the one runtime column of
          // its name, qualifiers, and type
          val ids = schema
            .fields
            .map { f =>
              val js = out
                .indices
                .filter(j =>
                  out(j).name == f.name && out(j).ptype == f.ptype &&
                    out(j).qualifiers == f.qualifiers
                )
              Option.when(js.size == 1)(outIds(js.head))
            }
          Option.when(out.size == schema.size && ids.forall(_.isDefined))((key, ids.map(_.get)))
        }
    }

  /**
    * The bound runtime plan of `rel`, for its keys. Binding evaluates subqueries, which the
    * discarded binding would run once more: an input with subqueries has none, nor does a
    * correlated one, which fails on its outer names. The hidden-name counter is restored, so that
    * names don't shift
    */
  private def boundInput(rel: Relation): Option[RuntimePlan] =
    val seq = hiddenSeq
    try
      var subqueries = false
      rel.traverseExpressions {
        case _: SubQueryExpression | _: Exists | _: InRelation | _: NotInRelation =>
          subqueries = true
      }
      Option.unless(subqueries)(runtimeBinder.bind(rel))
    catch
      case _: Exception =>
        None
    finally
      hiddenSeq = seq

  /**
    * Evaluate uncorrelated scalar subqueries that aggregate the same input (e.g. the count, avg,
    * and avg of each TPC-DS q09 bucket) as one query, so that the input is read once for all of
    * them. Inputs are the same when their bound runtime plans are equal up to column ids
    * ([[ShareSubplans.key]]). The results fill [[scalarResults]]
    */
  private def prefetchScalarSubqueries(r: Relation): Unit =
    val cteNames = cteNamesIn(r)
    val subs     = mutable.ArrayBuffer.empty[(Any, Project)]
    r.traverseExpressions { case s: SubQueryExpression =>
      s.query match
        case p: Project
            if p.selectItems.size == 1 && Binder.containsAggregate(p.selectItems.head) &&
              !scalarResults.containsKey(p) && !prefetchTried.containsKey(p) &&
              !callsVolatile(p.selectItems.head) =>
          prefetchTried.put(p, ())
          if !readsCTE(p.child, cteNames) then
            inputKey(p.child).foreach(k => subs += k -> p)
        case _ =>
    }
    subs
      .groupBy(_._1)
      .values
      .map(_.map(_._2))
      .filter(_.size > 1)
      .foreach { ps =>
        val merged = Project(ps.head.child, ps.map(_.selectItems.head).toList, NoSpan)
        val op     =
          try
            Option.when(!isCorrelated(merged, Schema.empty))(Planner.nested(this).plan(merged))
          catch
            case e: Exception =>
              // Each subquery is planned and evaluated on its own instead
              debug(s"Could not merge scalar subqueries: ${e.getMessage}")
              None
        op.foreach { op =>
          val b = Operator.materialize(op)
          if b.size <= 1 then
            ps.indices
              .foreach { i =>
                val v =
                  if b.size == 0 then
                    null
                  else
                    b.columns(i).get(0)
                scalarResults.put(ps(i), Const(v, op.schema(i).ptype))
              }
        }
      }

  end prefetchScalarSubqueries

  // --- relations ------------------------------------------------------------------------

  /** Binds relations to the engine's runtime plan */
  private lazy val runtimeBinder = RuntimeBinder(this)

  /** The logical passes over runtime plans */
  private lazy val runtimeOptimizer = Optimizer.default(
    options,
    newColumnId = Some(() => runtimeBinder.newColumnId())
  )

  /** Plan a relation into operators: bind it to the runtime plan, optimize, and lower it */
  private[engine] def planRelation(r: Relation): Operator = lower(runtimePlan(r))

  /** The optimized runtime plan of a relation */
  def runtimePlan(r: Relation): RuntimePlan = optimizeRuntime(runtimeBinder.bind(r))

  /** Run the logical passes over a bound runtime plan (and validate it, with `validatePlans`) */
  private[engine] def optimizeRuntime(p: RuntimePlan): RuntimePlan =
    if options.validatePlans then
      PlanValidator.validate(p)
    runtimeOptimizer.run(p)

  /** How many times each logical rule fired in the runtime plans of this planner, by rule name */
  def firedRules: Map[String, Int] = runtimeOptimizer.firedRules

  /** Register the CTEs of a WITH clause, which table references resolve by name */
  private[engine] def registerCTEs(w: WithQuery): Unit =
    w.queryDefs.foreach(d => cteDefs(d.alias.leafName.toLowerCase) = d)
    countCTERefs(w)

  /**
    * The shared CTE a table name refers to: a CTE read more than once, whose body is bound on the
    * first read
    */
  private[engine] def sharedCTE(name: String): Option[SharedCTE] =
    val key = name.toLowerCase
    cteDefs.get(key) match
      case Some(d) if enabled(Optimization.ShareCTEs) && cteRefCounts.getOrElse(key, 0) > 1 =>
        val cte =
          cteRefs.get(key) match
            case Some((defn, c)) if defn eq d =>
              c
            case _ =>
              val c = DeferredCTE(name)
              cteRefs(key) = (d, c)
              deferredCTEs += c
              bindingCTEs = c :: bindingCTEs
              try
                c.bound = runtimeBinder.bind(d)
                c.body = optimizeRuntime(c.bound)
              finally
                bindingCTEs = bindingCTEs.tail
              c
        bindingCTEs.headOption.foreach(_.reads += cte)
        Some(cte)
      case _ =>
        None

  /** The definition of a CTE in scope, by name */
  private[engine] def cteDef(name: String): Option[AliasedRelation] = cteDefs.get(name.toLowerCase)

  private[engine] def parseSql(r: RawSQL): Relation = sources.parseSql(r)

  private def batchOf(schema: Schema, rows: Seq[Seq[Any]]): Batch =
    val cols = schema
      .fields
      .indices
      .map { c =>
        Vec.fromValues(schema(c).ptype, rows.map(_(c)))
      }
    Batch(cols.toArray, rows.size)

  /**
    * The output schema of a set operation: the names of the first input, and at each position the
    * common type of the inputs
    */
  private[engine] def mergeSchemas(schemas: Seq[Schema]): Schema =
    val first = schemas.head
    Schema(
      first
        .fields
        .indices
        .map { i =>
          val t = schemas
            .map(s =>
              if i < s.size then
                s(i).ptype
              else
                PType.Null
            )
            .reduce(PType.common)
          Field(first(i).name, t)
        }
    )

  // Filters over a table whose scan is shared (`share_subquery_scans`), with the relation each
  // reads instead: its other conjuncts over the shared CTE
  private val sharedScans = java.util.IdentityHashMap[Filter, Relation]()
  // Reads of the shared scans' CTEs, which their tree doesn't show
  private val sharedScanReads = mutable.HashMap.empty[String, Int]

  /** Read the filter `f` as `r`, a filter over a shared scan ([[SharedScans]]) */
  private[engine] def readSharedScan(f: Filter, r: Relation): Unit = sharedScans.put(f, r)

  /** Count `n` reads of the CTE `name` of a shared scan */
  private[engine] def addCTEReads(name: String, n: Int): Unit =
    sharedScanReads(name.toLowerCase) = n

  /** The relation the filter `f` reads instead, when it reads a shared scan */
  private[engine] def sharedScanOf(f: Filter): Option[Relation] = Option(sharedScans.get(f))

  /** Count the references to each CTE of `w`, including those in subqueries */
  private def countCTERefs(w: WithQuery): Unit =
    val names = w.queryDefs.map(_.alias.leafName.toLowerCase).toSet
    // A nested WITH reusing a name adds to the outer counts: at worst, an extra materialization
    names.foreach(n => cteRefCounts.getOrElseUpdate(n, 0))
    def count(n: String): Unit =
      if names.contains(n) then
        cteRefCounts(n) = cteRefCounts(n) + 1
    w.traverse {
      case t: TableRef =>
        count(t.name.leafName.toLowerCase)
      case t: TableScan =>
        count(t.name.name.toLowerCase)
    }
    names.foreach(n => sharedScanReads.get(n).foreach(k => cteRefCounts(n) = cteRefCounts(n) + k))

  /** The source of a catalog table (not a CTE) */
  private[engine] def tableSource(name: String, fullName: String): TableSource = catalog
    .lookup(name, fullName)
    .orElse(sources.tableSource(name, fullName))
    .getOrElse {
      throw StatusCode.TABLE_NOT_FOUND.newException(s"Table not found: ${fullName}")
    }

  /** The source of a file path */
  private[engine] def fileSource(path: String): TableSource = sources
    .fileSource(path)
    .getOrElse {
      throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported file format: ${path}")
    }

  /** The rows of a VALUES list, evaluated, and their schema */
  private[engine] def valuesBatch(
      v: Values,
      columnNames: Option[List[wvlet.lang.model.DataType.NamedType]]
  ): (Schema, Batch) =
    val binder                  = Binder(this, BindScope(Schema.empty))
    val rows: List[List[VExpr]] = v
      .rows
      .map {
        case a: ArrayConstructor =>
          a.values.map(binder.bind)
        case r: RowConstructor =>
          r.values.map(binder.bind)
        case other =>
          List(binder.bind(other))
      }
    val width                     = rows.headOption.map(_.size).getOrElse(0)
    val names: IndexedSeq[String] = (0 until width).map { i =>
      columnNames
        .flatMap(_.lift(i))
        .map(_.name.name)
        .orElse {
          v.schema match
            case s: wvlet.lang.model.DataType.SchemaType =>
              s.columnTypes.lift(i).map(_.name.name).filter(n => n.nonEmpty && !n.startsWith("<"))
            case _ =>
              None
        }
        .getOrElse(s"col${i}")
    }
    val types = (0 until width).map { i =>
      val t = rows.map(_(i).ptype).reduceOption(PType.common).getOrElse(PType.Null)
      if t == PType.Null then
        PType.Int32
      else
        t
    }
    val schema = Schema(names.indices.map(i => Field(names(i), types(i))))
    val one    = Batch.ofRows(1)
    val values = rows.map(r =>
      r.zipWithIndex.map((e, i) => ValueOps.cast(e.eval(one).get(0), e.ptype, types(i)))
    )
    (schema, batchOf(schema, values))

  end valuesBatch

  // --- pivot / unpivot -----------------------------------------------------------------

  /** Column indexes referenced by an expression */
  private def columnRefs(e: VExpr): Set[Int] =
    e match
      case c: ColRef =>
        Set(c.index)
      case other =>
        other.children.flatMap(columnRefs).toSet

  /**
    * Plan a PIVOT whose input is planned with `plan`: an aggregation with one filtered call per
    * (pivot values, aggregate). The input is computed at plan time only when a pivot key lists no
    * values, which then come from its rows, and the aggregation reads those rows
    */
  private[engine] def pivot[P](
      p: Pivot,
      aggItems: List[Attribute],
      plan: Relation => P,
      builder: AggPlanBuilder[P]
  ): P =
    val child  = plan(p.child)
    val schema = builder.op(child).schema
    // The input rows, when some pivot values come from them
    val batch =
      if p.pivotKeys.exists(_.values.isEmpty) then
        Some(Operator.materialize(builder.op(child)))
      else
        None
    val input  = batch.map(builder.values(schema, _)).getOrElse(child)
    val scope  = BindScope(schema)
    val binder = Binder(this, scope)
    // Pivot columns and their values
    val pivots: List[(VExpr, Seq[Any])] = p
      .pivotKeys
      .map { k =>
        val col    = binder.bind(k.name)
        val values =
          if k.values.nonEmpty then
            k.values
              .map { v =>
                val c = binder.bind(v)
                ValueOps.cast(c.eval(Batch.ofRows(1)).get(0), c.ptype, col.ptype)
              }
          else
            val b   = batch.get
            val vec = col.eval(b)
            (0 until b.size)
              .map(vec.get)
              .filter(_ != null)
              .distinct
              .sortWith((x, y) => ValueOps.compare(x, y) < 0)
        (col, values)
      }
    val combos: Seq[List[Any]] =
      pivots.foldRight(Seq(List.empty[Any])) { case ((_, vs), acc) =>
        for
          v    <- vs
          rest <- acc
        yield v :: rest
      }
    // USING aggregates
    val aggCtx0    = AggContext(scope, IndexedSeq.empty, IndexedSeq.empty)
    val aggBinder0 = Binder(this, BindScope(aggCtx0.outputSchema), Some(aggCtx0))
    def aggFunction(e: Expression): FunctionApply =
      Binder.expandTemplates(e) match
        case f: FunctionApply =>
          f
        case a: SingleColumn =>
          aggFunction(a.expr)
        case a: Alias =>
          aggFunction(a.expr)
        case other =>
          throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported pivot aggregate: ${other}")
    val countStar = AggCall(
      "count_star",
      Nil,
      distinct = false,
      filter = None,
      Aggregates.resultType("count_star", Nil)
    )
    val aggs: List[(String, AggCall)] =
      if aggItems.isEmpty then
        List(("count", countStar))
      else
        aggItems.map(a => (nameOf(a), aggBinder0.aggCall(aggFunction(a))))
    // Whether the single aggregate has an explicit name (`agg _.sum(x) as total`)
    val aliased = aggItems.exists(a => a.nameExpr.nonEmpty && !a.nameExpr.leafName.startsWith("<"))
    // Grouping keys
    val pivotCols                         = pivots.flatMap(x => columnRefs(x._1)).toSet
    val usedCols                          = aggs.flatMap(_._2.args.flatMap(columnRefs)).toSet
    val keys: IndexedSeq[(String, VExpr)] =
      if p.groupingKeys.nonEmpty then
        p.groupingKeys
          .map { k =>
            val n    = k.name.leafName
            val name =
              if n.nonEmpty && !n.startsWith("<") then
                n
              else
                exprName(k.child)
            (name, binder.bind(k.child))
          }
          .toIndexedSeq
      else
        schema
          .fields
          .indices
          .filterNot(i => pivotCols.contains(i) || usedCols.contains(i) || isHidden(schema(i)))
          .map(i => (schema(i).name, ColRef(i, schema(i).ptype, schema(i).name): VExpr))
    // One filtered aggregate per (pivot values, aggregate)
    val calls  = IndexedSeq.newBuilder[AggCall]
    val fields = IndexedSeq.newBuilder[Field]
    combos.foreach { combo =>
      val cond = pivots
        .zip(combo)
        .map { case ((col, _), v) =>
          Compare(CmpOp.Eq, col, Const(v, col.ptype)): VExpr
        }
        .reduce((a, b) => AndExpr(a, b))
      val prefix = pivots
        .zip(combo)
        .map { case ((col, _), v) =>
          ValueOps.cast(v, col.ptype, PType.Str)
        }
        .mkString("_")
      aggs.foreach { (aggName, call) =>
        val filter = call.filter.map(f => AndExpr(f, cond)).getOrElse(cond)
        calls += call.copy(filter = Some(filter))
        fields +=
          Field(
            // DuckDB: <value> for a single unnamed aggregate, otherwise <value>_<name>
            if aggs.size == 1 && !aliased then
              prefix
            else
              s"${prefix}_${aggName}"
            ,
            call.ptype
          )
      }
    }
    val aggCalls  = calls.result()
    val outSchema = Schema(keys.map((n, e) => Field(n, e.ptype)) ++ fields.result())
    builder.aggregate(input, keys.map(_._2), aggCalls, outSchema)

  end pivot

  /**
    * Plan an UNPIVOT whose input is planned with `plan`: one row per (input row, target column)
    * with a non-NULL value, as a union of one filtered projection per target column. With several
    * target columns, the branches read one evaluation of the input, computed on first read
    */
  private[engine] def unpivot[P](u: Unpivot, plan: Relation => P, builder: AggPlanBuilder[P]): P =
    val child   = plan(u.child)
    val schema  = builder.op(child).schema
    val key     = u.unpivotKey
    val targets = key
      .targetColumns
      .map { c =>
        val i = schema.names.indexWhere(_.equalsIgnoreCase(c.leafName))
        if i < 0 then
          throw ColumnNotFound(s"Column not found: ${c.leafName}")
        i
      }
    val others    = schema.fields.indices.filterNot(targets.contains)
    val valueType = targets.map(schema(_).ptype).reduce(PType.common)
    val outSchema = Schema(
      others.map(schema(_)).map(_.noQualifier) ++
        IndexedSeq(
          Field(key.unpivotColumnName.leafName, PType.Str),
          Field(key.getValueColumnName.leafName, valueType)
        )
    )
    val reader: () => P =
      if targets.size > 1 then
        builder.sharedInput(child, schema.fields.indices, "UnpivotInput")
      else
        () => child
    def col(i: Int): VExpr = ColRef(i, schema(i).ptype, schema(i).name)
    val parts              = targets.map { t =>
      builder.project(
        builder.filter(reader(), IsNullExpr(col(t), negate = true)),
        others.map(col) ++
          IndexedSeq(Const(schema(t).name, PType.Str), castExpr(col(t), valueType)),
        outSchema
      )
    }
    builder.union(parts.toIndexedSeq, outSchema)

  end unpivot

  // --- SQL SELECT blocks ----------------------------------------------------------------

  /** A SQL SELECT block as wvlet operators: filters, grouping, HAVING, projection, order, limit */
  private[engine] def desugarSQLSelect(s: SQLSelect): Relation =
    var r: Relation = s.child
    s.filters.foreach(f => r = Filter(r, f.filterExpr, f.span))
    if s.groupingKeys.nonEmpty then
      r = GroupBy(r, s.groupingKeys, s.span)
      s.having.foreach(h => r = Filter(r, h.filterExpr, h.span))
    r = Project(r, s.selectItems, s.span)
    if s.orderBy.nonEmpty then
      r = Sort(r, s.orderBy.toList, s.span)
    s.limit
      .foreach {
        case l: LongLiteral =>
          r = Limit(r, l, s.span)
        case _ =>
      }
    r

  // --- projection -----------------------------------------------------------------------

  /** Output name of a select item */
  def nameOf(a: Attribute): String =
    if a.nameExpr.nonEmpty && !a.nameExpr.leafName.startsWith("<") then
      a.nameExpr.leafName
    else
      a match
        case s: SingleColumn =>
          exprName(s.expr)
        case al: Alias =>
          exprName(al.expr)
        case other =>
          exprName(other)

  def exprName(e: Expression): String =
    e match
      case i: Identifier =>
        i.leafName
      case d: DotRef =>
        d.name.leafName
      case t: TypedExpression =>
        exprName(t.child)
      case p: ParenthesizedExpression =>
        exprName(p.child)
      case s: SingleColumn if s.nameExpr.nonEmpty =>
        s.nameExpr.leafName
      case s: SingleColumn =>
        exprName(s.expr)
      case other =>
        printSql(other).replace("count(*)", "count_star()")

  /** Single-line SQL text of an expression */
  private def printSql(e: Expression): String = sqlText(e).replaceAll("\\s+", " ")

  /** SQL text of an expression, with the whitespace inside its string literals kept */
  private def sqlText(e: Expression): String =
    try
      SqlGenerator(
        CodeFormatterConfig(sqlDBType = DBType.DuckDB, fitToLine = true, maxLineWidth = 10000)
      ).print(e)
    catch
      case _: Exception =>
        e.toString

  private def isStar(e: Expression): Boolean =
    e match
      case _: Wildcard =>
        true
      case i: Identifier =>
        i.leafName == "*"
      case _ =>
        false

  /** Bind select items over an operator, expanding `*` */
  private[engine] def bindItems(
      binder: Binder,
      items: List[Attribute]
  ): (IndexedSeq[VExpr], IndexedSeq[Field]) =
    val exprs  = IndexedSeq.newBuilder[VExpr]
    val fields = IndexedSeq.newBuilder[Field]
    // Every visible column, optionally only those of a qualifier (t.*)
    def expandStar(qualifier: Option[String]): Unit =
      val visibleFields = binder.scope.schema.fields
      visibleFields
        .indices
        .foreach { i =>
          val f = visibleFields(i)
          if !isHidden(f) && qualifier.forall(q => f.qualifiers.exists(_.equalsIgnoreCase(q))) then
            exprs += ColRef(binder.scope.offset + i, f.ptype, f.name)
            fields += f
        }
    items.foreach {
      // `select *` of a parsed SQL subquery
      case s: SingleColumn if isStar(s.expr) =>
        expandStar(None)
      case a: AllColumns =>
        val qualifier =
          a.nameExpr match
            case d: DotRef =>
              Some(d.qualifier).collect { case i: Identifier =>
                i.leafName
              }
            case _ =>
              None
        expandStar(qualifier)
      case attr =>
        val e = binder.bind(attr)
        exprs += e
        val qualifiers =
          e match
            case c: ColRef
                if c.index - binder.scope.offset >= 0 &&
                  c.index - binder.scope.offset < binder.scope.width =>
              binder.scope.schema(c.index - binder.scope.offset).qualifiers
            case _ =>
              Nil
        fields += Field(nameOf(attr), e.ptype, qualifiers)
    }
    (exprs.result(), fields.result())

  end bindItems

  /** Whether a projection aggregates (its input is a grouping, or an item is an aggregate) */
  private[engine] def isAggregation(child: Relation, items: List[Attribute]): Boolean =
    isAggregationInput(child) || items.exists(i => Binder.containsAggregate(i))

  // --- unnest ---------------------------------------------------------------------------

  private def isUnnest(name: NameExpr): Boolean = name.leafName.equalsIgnoreCase("unnest")

  /** unnest(...) as a relation, with its alias */
  private[engine] def unnestCall(
      r: Relation
  ): Option[(TableFunctionCall, Option[AliasedRelation])] =
    r match
      case t: TableFunctionCall if isUnnest(t.name) =>
        Some((t, None))
      case a @ AliasedRelation(t: TableFunctionCall, _, _, _) if isUnnest(t.name) =>
        Some((t, Some(a)))
      case _ =>
        None

  /** The list argument of a select item `unnest(list)` */
  private[engine] def unnestArg(e: Expression): Option[Expression] =
    e match
      case s: SingleColumn =>
        unnestArg(s.expr)
      case a: Alias =>
        unnestArg(a.expr)
      case t: TypedExpression =>
        unnestArg(t.child)
      case f: FunctionApply if f.window.isEmpty && Binder.functionName(f) == "unnest" =>
        f.args match
          case List(a) =>
            Some(a.value)
          case _ =>
            throw StatusCode.NOT_IMPLEMENTED.newException("unnest expects a single list argument")
      case _ =>
        None

  /**
    * The lists of unnest(list, ...) bound over `schema`, and their element fields, named by the
    * alias's column names and qualified by its name
    */
  private[engine] def unnestLists(
      schema: Schema,
      call: TableFunctionCall,
      alias: Option[AliasedRelation]
  ): (IndexedSeq[VExpr], IndexedSeq[Field]) =
    val binder = Binder(this, BindScope(schema))
    val lists  = call.args.map(a => binder.bind(a.value)).toIndexedSeq
    val names  = alias.flatMap(_.columnNames).map(_.map(_.name.name)).getOrElse(Nil)
    val q      = alias.map(_.alias.leafName).toList
    val fields = lists
      .indices
      .map { i =>
        Field(names.lift(i).getOrElse("unnest"), Binder.listElementType(lists(i)), q)
      }
    (lists, fields)

  /**
    * Select items with `unnest(list)` items bound over `schema`, each as its list: the expressions,
    * their fields, and the positions of the list items. Items bind one by one, since `*` expands to
    * several fields
    */
  private[engine] def bindUnnestItems(
      schema: Schema,
      items: List[Attribute]
  ): (IndexedSeq[VExpr], IndexedSeq[Field], IndexedSeq[Int]) =
    val replaced = items.map { i =>
      unnestArg(i) match
        case Some(arg) =>
          SingleColumn(NameExpr.fromString(nameOf(i)), arg, NoSpan)
        case None =>
          i
    }
    val binder = Binder(this, BindScope(schema))
    val bound  = replaced.map(r => bindItems(binder, List(r)))
    val exprs  = bound.flatMap(_._1).toIndexedSeq
    val fields = bound.flatMap(_._2).toIndexedSeq
    val isList =
      items
        .zip(bound)
        .flatMap((item, b) => Seq.fill(b._2.size)(unnestArg(item).isDefined))
        .toIndexedSeq
    (exprs, fields, fields.indices.filter(isList))

  /** The element field of the list item at position i of [[bindUnnestItems]] */
  private[engine] def unnestElementField(
      exprs: IndexedSeq[VExpr],
      fields: IndexedSeq[Field],
      i: Int
  ): Field = fields(i).copy(ptype = Binder.listElementType(exprs(i)))

  /**
    * Bind expressions over an input of schema `schema`. When they contain window functions, the
    * first pass registers the calls, and binding runs again over the window output (the input
    * columns, then one column per call)
    *
    * @return
    *   the collected window calls, if any, and the result of `bindAll`
    */
  private[engine] def bindWithWindows[A](schema: Schema, items: List[Expression])(
      bindAll: Binder => A
  ): (Option[WindowCollector], A) =
    if items.exists(i => Binder.containsWindow(i)) then
      val collector = WindowCollector(schema)
      val saved     = windowCollector
      windowCollector = Some(collector)
      try
        bindAll(Binder(this, BindScope(schema)))
        val frozen = collector.frozen
        windowCollector = Some(frozen)
        (Some(frozen), bindAll(Binder(this, BindScope(frozen.schema))))
      finally
        windowCollector = saved
    else
      (None, bindAll(Binder(this, BindScope(schema))))

  /**
    * Expand ROLLUP / CUBE / GROUPING SETS into the distinct grouping keys and the key indexes of
    * each grouping set (None for a plain GROUP BY). Several elements combine as a cross product
    */
  private def expandGroupingSets(
      keys: List[GroupingKey]
  ): (List[GroupingKey], Option[List[Set[Int]]]) =
    val base                         = mutable.ArrayBuffer.empty[GroupingKey]
    def indexOf(k: GroupingKey): Int =
      val text = exprText(k.child)
      base.indexWhere(b => exprText(b.child) == text) match
        case -1 =>
          base += k
          base.size - 1
        case i =>
          i
    def keyOf(e: Expression): GroupingKey =
      e match
        case k: GroupingKey =>
          k
        case a: FunctionArg =>
          keyOf(a.value)
        case other =>
          UnresolvedGroupingKey(NameExpr.EmptyName, other, NoSpan)
    def prefixes(ks: List[GroupingKey]): List[Set[Int]] =
      val idx = ks.map(indexOf)
      (idx.size to 0 by -1).map(n => idx.take(n).toSet).toList
    def subsets(ks: List[GroupingKey]): List[Set[Int]] =
      val idx = ks.map(indexOf)
      idx.foldRight(List(Set.empty[Int]))((i, acc) => acc.map(_ + i) ++ acc)
    def funcName(k: GroupingKey): Option[(String, List[GroupingKey])] =
      k.child match
        case f: FunctionApply if f.window.isEmpty =>
          Binder.functionName(f).toLowerCase match
            case n @ ("rollup" | "cube") =>
              Some(n -> f.args.map(keyOf))
            case _ =>
              None
        case _ =>
          None
    var hasSets                        = false
    val elements: List[List[Set[Int]]] = keys.map {
      case r: Rollup =>
        hasSets = true
        prefixes(r.groupingKeys)
      case c: Cube =>
        hasSets = true
        subsets(c.groupingKeys)
      case g: GroupingSets =>
        hasSets = true
        g.groupingSets.map(_.map(indexOf).toSet)
      case k =>
        funcName(k) match
          case Some(("rollup", args)) =>
            hasSets = true
            prefixes(args)
          case Some((_, args)) =>
            hasSets = true
            subsets(args)
          case None =>
            List(Set(indexOf(k)))
    }
    val sets =
      elements.foldLeft(List(Set.empty[Int]))((acc, e) =>
        for
          a <- acc;
          b <- e
        yield a ++ b
      )
    (base.toList, Option.when(hasSets)(sets))

  end expandGroupingSets

  /**
    * The rows of an operator, computed once on first read and shared by several readers (e.g., a
    * CTE referenced more than once, or the input of grouping sets)
    */
  private class SharedResult(val op: Operator) extends SharedBatches:
    private val result = ComputeOnce { () =>
      debug(s"Materializing a shared result:\n${op.explain()}")
      op.drain()
    }

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
  private class SharedReader(val shared: SharedResult, schema: Schema, label: String)
      extends IteratorOp(schema, () => shared.batches.iterator, label):
    override protected def extraInputs(indent: Int): Seq[String] = shared.sharedPlan(indent).toSeq
    override def sharedInputs: Seq[Operator]                     = Seq(shared.op)

  /** Batches computed on first read, scanned as a table whose parts are the batches */
  private class SharedTable(val name: String, val schema: Schema, val shared: SharedResult)
      extends TableSource:
    def scan(spec: ScanSpec): Iterator[Batch] =
      val sel = selector(spec)
      shared.batches.iterator.map(sel)

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
          ScanSplits(
            bs.toIndexedSeq.map(b => () => Iterator.single(sel(b))),
            () => (),
            bs.toIndexedSeq.map(_.size.toLong)
          )
        )

    override def explainDetail(indent: Int): Option[String] = shared.sharedPlan(indent)

  end SharedTable

  /**
    * A result read by several runtime-plan readers (`CTERef`s), such as the input of grouping sets:
    * `op`, the operator of `body`, is physically optimized and computed on first read
    */
  private class SharedInput(val name: String, val body: RuntimePlan, op: Operator)
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
  private class DeferredCTE(val name: String) extends SharedCTE:
    // Set right after binding: the body can't read its own CTE. `body` is `bound` optimized
    var bound: RuntimePlan = null
    var body: RuntimePlan  = null
    // The CTEs that the body reads, which are finalized after this one
    val reads = mutable.LinkedHashSet.empty[DeferredCTE]

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
      rowCount.exists(_ <= Planner.MaxResultStatsRows) && enabled(Optimization.CTEReaderStats)

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
                Stats
                  .resultDistinct(
                    batches,
                    k,
                    body.schema(position).ptype,
                    primitive = enabled(Optimization.PrimitiveResultDistinct)
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
        ScanOp(CTETable(label, body.schema, this, setContext), schema, None, None)
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
      // The readers' filters pushed into the body can make an aggregation of it worth computing
      // below its joins (TPC-DS q04, q11: `d_year in (2001, 2002)`); optimizeRuntime already ran
      // the pass over the body without them. A reduced body's readers read only its
      // distinct rows, as do the CTE readers in it
      val op = Planner.this.lower(
        runtimeOptimizer.pushScanFilters(
          if pushed.isEmpty then
            pruned
          else
            runtimeOptimizer.eagerAggregation(pruned)
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
  private class CTEIteratorReader(val cte: DeferredCTE, schema: Schema, label: String)
      extends IteratorOp(schema, () => cte.batches.iterator, label):
    override protected def extraInputs(indent: Int): Seq[String] = cte.sharedPlan(indent).toSeq
    override def sharedInputs: Seq[Operator]                     = cte.resultOp.toSeq

  /**
    * `results` and the shared results that `roots` and `results` read, directly or through other
    * shared results. A result comes after the results it reads
    */
  private def sharedResultsOf(
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
  private def concurrentSharedResults: Boolean =
    enabled(Optimization.ConcurrentSharedCTEs) && enabled(Optimization.Parallelize) &&
      TaskRunner.default.parallelism > 1

  /**
    * Compute `results` and the shared results they read, those not computed yet at the same time on
    * coordinator threads, and wait until all are done (their failures are raised when read)
    */
  private def computeSharedResults(results: Seq[SharedResult]): Unit =
    if concurrentSharedResults then
      val pending = sharedResultsOf(Nil, results).filterNot(_.isComputed)
      if pending.size > 1 then
        val handles = pending.map(r => TaskRunner.default.coordinate(() => r.compute(() => false)))
        handles.foreach(_.get())

  /**
    * The plan `root`, which starts computing the shared results it reads on coordinator threads
    * when it starts running ([[StartSharedOp]]), when there are several
    */
  private def startingSharedResults(root: Operator): Operator =
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
  private class CTETable(
      val name: String,
      val schema: Schema,
      val cte: DeferredCTE,
      val setContext: Boolean = false
  ) extends TableSource:
    def scan(spec: ScanSpec): Iterator[Batch] =
      val sel = cte.selector(spec.columns)
      cte.batches.iterator.map(sel)

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
          ScanSplits(
            bs.toIndexedSeq.map(b => () => Iterator.single(sel(b))),
            () => (),
            bs.toIndexedSeq.map(_.size.toLong)
          )
        )

    override def explainDetail(indent: Int): Option[String] = cte.sharedPlan(indent)

  end CTETable

  /**
    * Lower the bodies of the shared CTEs once every reader is planned: each keeps the columns its
    * readers' scans read after physical pruning, in `root` and in the bodies of the CTEs that read
    * it, which are finalized first
    */
  private def finalizeCTEs(root: Operator): Unit =
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

  private[engine] def isAggregationInput(r: Relation): Boolean =
    r match
      case _: GroupBy =>
        true
      case Filter(_: GroupBy, _, _) =>
        true
      case _ =>
        false

  /**
    * The output of adding (or, with `prepend`, prepending) the bound new columns `exprs` / `fields`
    * to an input of schema `input`: a new column with an existing name replaces it in place, and
    * input columns are read at their positions (which a window evaluation keeps)
    */
  private[engine] def columnEditItems(
      input: Schema,
      prepend: Boolean,
      exprs: IndexedSeq[VExpr],
      fields: IndexedSeq[Field]
  ): IndexedSeq[(VExpr, Field)] =
    val existing = input
      .fields
      .indices
      .map { i =>
        val f = input(i)
        val k = fields.indexWhere(_.name.equalsIgnoreCase(f.name))
        if k >= 0 then
          (exprs(k), fields(k))
        else
          (ColRef(i, f.ptype, f.name), f)
      }
    val added = fields
      .indices
      .filterNot(k => input.fields.exists(_.name.equalsIgnoreCase(fields(k).name)))
      .map(k => (exprs(k), fields(k)))
    if prepend then
      added ++ existing
    else
      existing ++ added

  end columnEditItems

  /** The positions of the input columns that `e` keeps */
  private[engine] def excludeIndexes(
      input: Schema,
      e: ExcludeColumnsFromRelation
  ): IndexedSeq[Int] =
    val drops = e.columnNames.map(_.leafName.toLowerCase).toSet
    input.fields.indices.filter(i => !drops.contains(input(i).name.toLowerCase))

  /** The input columns of `r` under their new names */
  private[engine] def renameSchema(input: Schema, r: RenameColumnsFromRelation): Schema =
    val renames =
      r.columnAliases
        .map { a =>
          val from =
            a.expr match
              case i: Identifier =>
                i.leafName
              case d: DotRef =>
                d.name.leafName
              case other =>
                other.toString
          from.toLowerCase -> a.nameExpr.leafName
        }
        .toMap
    input.mapFields(f => renames.get(f.name.toLowerCase).map(f.withName).getOrElse(f))

  /** The positions of the input columns in the order `s` shifts them to */
  private[engine] def shiftIndexes(input: Schema, s: ShiftColumns): IndexedSeq[Int] =
    val names         = s.shiftItems.map(_.leafName.toLowerCase)
    val idx           = input.fields.indices
    val (moved, rest) = idx.partition(i => names.contains(input(i).name.toLowerCase))
    val ordered       = names.flatMap(n => moved.find(i => input(i).name.toLowerCase == n))
    if s.isLeftShift then
      ordered.toIndexedSeq ++ rest
    else
      rest ++ ordered

  /** The rows of DESCRIBE over a relation of schema `input`: the name and type of each column */
  private[engine] def describeRows(input: Schema): (Schema, Batch) =
    val rows = input
      .fields
      .filterNot(isHidden)
      .map(f => Seq[Any](f.name, f.ptype.toDataType.typeDescription))
    val schema = Schema(
      IndexedSeq(Field("column_name", PType.Str), Field("column_type", PType.Str))
    )
    (schema, batchOf(schema, rows))

  // --- aggregation ----------------------------------------------------------------------

  /**
    * Plan an aggregation of `input` (a GROUP BY, possibly with HAVING, or a global aggregation)
    * selecting `items`, whose input is planned with `plan`. With grouping sets, one aggregation per
    * set reads a shared evaluation of the input, and a union combines them. A correlated scalar
    * subquery (or EXISTS) in a select item is a left join over the aggregation's output
    * (`rewriteSubqueries`), so that its outer references read the group's keys, NULL in a grouping
    * set without them; `filter` plans the subquery's conditions
    */
  private[engine] def aggregation[P](
      input: Relation,
      items: Option[List[Attribute]],
      defaultItems: Boolean,
      plan: Relation => P,
      filter: Filter => P,
      builder: JoinPlanBuilder[P] & AggPlanBuilder[P]
  ): P =
    val (groupBy, having) =
      input match
        case g: GroupBy =>
          (Some(g), None)
        case Filter(g: GroupBy, cond, _) =>
          (Some(g), Some(cond))
        case _ =>
          (None, None)
    val source           = groupBy.map(_.child).getOrElse(input)
    val hiddenSeqAtStart = hiddenSeq
    // A filter `key in (subquery)` whose subquery aggregates the same input: the input is planned
    // without it, and the groups are filtered instead (`share_in_subquery_groups`)
    val inShare =
      if groupBy.isDefined && enabled(Optimization.ShareInSubqueryGroups) &&
        !inShareDeclined.containsKey(source)
      then
        inSubqueryShare(source)
      else
        None
    // An IN subquery grouping one of the input's tables by the join key: the table is replaced by
    // the subquery's groups, whose totals the aggregation merges (`reuse_in_subquery_aggregates`)
    val reused =
      if inShare.isEmpty && enabled(Optimization.ReuseInSubqueryAggregates) then
        for
          g   <- groupBy
          its <- items
          if g.groupingKeys.forall(_.isInstanceOf[UnresolvedGroupingKey])
          rewritten <- InSubqueryAggregates.rewrite(this, g, having, its)
        yield
          val grouped = g.copy(child = rewritten)
          input match
            case f: Filter =>
              f.copy(child = grouped)
            case _ =>
              grouped
      else
        None
    val reusedPlan = reused.flatMap { r =>
      try
        Some(aggregation(r, items, defaultItems, plan, filter, builder))
      catch
        case NonFatal(e) =>
          // Plan the original input when the rewritten one fails to plan
          debug(s"Not reusing IN subquery aggregates: ${e.getMessage}")
          hiddenSeq = hiddenSeqAtStart
          None
    }
    if reusedPlan.isDefined then
      return reusedPlan.get
    val in                      = plan(inShare.map(_.rest).getOrElse(source))
    val inOp                    = builder.op(in)
    val scope                   = BindScope(inOp.schema)
    val binder                  = Binder(this, scope)
    val (keyDefs, groupingSets) = expandGroupingSets(groupBy.map(_.groupingKeys).getOrElse(Nil))
    // Grouping keys; `group by 1` and `group by <select alias>` refer to select items
    val baseKeyExprs =
      keyDefs
        .map { k =>
          def selectItem(i: Int): Option[Attribute] = items.flatMap(_.lift(i))
          val e: Expression                         =
            k.child match
              case l: LongLiteral if items.isDefined =>
                selectItem(l.value.toInt - 1).getOrElse(k.child)
              case other =>
                other
          val bound =
            try
              binder.bind(e)
            catch
              case ex: ColumnNotFound =>
                e match
                  case i: Identifier =>
                    items
                      .flatMap(_.find(a => nameOf(a).equalsIgnoreCase(i.leafName)))
                      .map(a => binder.bind(a))
                      .getOrElse(throw ex)
                  case _ =>
                    throw ex
          val name =
            if k.name.nonEmpty && !k.name.leafName.startsWith("<") then
              k.name.leafName
            else
              e match
                case a: Attribute =>
                  nameOf(a)
                case other =>
                  exprName(other)
          (name, bound)
        }
        .toIndexedSeq
    // With grouping sets, a grouping id key tells which keys each row aggregates over
    val keyExprs =
      if groupingSets.isDefined then
        baseKeyExprs :+ (hiddenName("gid") -> Const(0L, PType.Int64))
      else
        baseKeyExprs
    val aggCtx = AggContext(
      scope,
      keyExprs.map(_._1),
      keyExprs.map(_._2),
      groupingId = Option.when(groupingSets.isDefined)(baseKeyExprs.size)
    )
    val aggBinder                    = Binder(this, BindScope(aggCtx.outputSchema), Some(aggCtx))
    val selectItems: List[Attribute] = items.getOrElse {
      // GroupBy without a selection: keys, then an arbitrary value of every input column
      val keyItems =
        baseKeyExprs
          .indices
          .map(i =>
            SingleColumn(
              NameExpr.fromString(baseKeyExprs(i)._1),
              UnquotedIdentifier(baseKeyExprs(i)._1, NoSpan),
              NoSpan
            )
          )
          .toList
      keyItems
    }
    // Bind select items in aggregation mode
    def bindSelectItems(
        b: Binder = aggBinder,
        its: List[Attribute] = selectItems
    ): IndexedSeq[(VExpr, Field)] =
      val boundItems = mutable.ArrayBuffer.empty[(VExpr, Field)]
      its.foreach {
        case a: AllColumns =>
          // `select *` over a grouping: every key
          baseKeyExprs
            .indices
            .foreach(i =>
              boundItems +=
                ((aggCtx.keyRef(i), Field(baseKeyExprs(i)._1, baseKeyExprs(i)._2.ptype)))
            )
        case attr =>
          val e = b.bind(attr)
          boundItems += ((e, Field(nameOf(attr), e.ptype)))
      }
      boundItems.toIndexedSeq
    val hasWindow = selectItems.exists(a => Binder.containsWindow(a))
    val saved     = windowCollector
    // Window functions over aggregated rows: the first pass registers window calls (and the
    // aggregate calls inside them); the window operator then runs over the aggregation output
    val collector = Option.when(hasWindow)(WindowCollector(inOp.schema))
    if hasWindow then
      windowCollector = collector
    // Items with correlated subqueries, which the binder plans alone, fail to bind: each subquery
    // becomes a hidden name, read from a join over the aggregation's output below
    var correlated = Option.empty[CorrelatedItems]
    val firstPass  =
      try
        bindSelectItems()
      catch
        case e: ColumnNotFound if !hasWindow && selectItems.exists(hasCorrelatedSubquery) =>
          val extracted = selectItems.map {
            case a: AllColumns =>
              (a, Nil)
            case item =>
              val (value, subs) = extractCorrelatedSubqueries(item)
              (SingleColumn(NameExpr.fromString(nameOf(item)), value, item.span), subs)
          }
          val c = CorrelatedItems(e, extracted.map(_._1), extracted.flatMap(_._2))
          correlated = Some(c)
          // Register the items' aggregate calls, with NULL in place of each subquery
          val nulls = c.subqueries.map((name, _) => name -> NullLiteral(NoSpan)).toMap
          // Items that fail here (e.g. a subquery inside an aggregate) keep the original error
          try
            bindSelectItems(Binder(this, aggBinder.scope, Some(aggCtx), nulls), c.items)
          catch
            case NonFatal(_) =>
              throw e
      finally
        windowCollector = saved
    val extraItems = mutable.ArrayBuffer.empty[(VExpr, Field)]
    if defaultItems && items.isEmpty then
      // arbitrary(c) for every input column
      inOp
        .schema
        .fields
        .indices
        .foreach { i =>
          val f = inOp.schema(i)
          if !isHidden(f) then
            val c = ColRef(i, f.ptype, f.name)
            val e = aggCtx.addCall(
              AggCall("arbitrary", Seq(c), distinct = false, filter = None, f.ptype)
            )
            extraItems += ((e, Field(s"arbitrary(${f.name})", f.ptype)))
        }
    // The groups, computed at plan time when scalar subqueries of the condition total them
    // (`having_totals_from_groups`), with the number of calls they compute
    val computedGroups =
      having match
        case Some(h) if groupingSets.isEmpty && enabled(Optimization.HavingTotalsFromGroups) =>
          computeGroupTotals(h, source, in, aggCtx, aggBinder, builder)
        case _ =>
          None
    val havingExpr = having.map(h => aggBinder.bind(h))
    val aggOp: P   =
      // The input, keys, and calls of the aggregation: over groups computed at plan time when an
      // IN subquery shares them, or another aggregation computed them
      // (`share_in_subquery_groups`)
      val (aggIn, keys, calls) =
        val keys       = keyExprs.map(_._2)
        val calls      = aggCtx.calls.toIndexedSeq
        val fromGroups =
          inShare match
            case Some(s) if correlated.isEmpty && computedGroups.isEmpty =>
              groupsFilteredByIn(
                s,
                in,
                inOp.schema,
                baseKeyExprs.map(_._2),
                keys,
                calls,
                aggCtx,
                builder
              )
            case None
                if sharedGroups.nonEmpty && groupingSets.isEmpty && correlated.isEmpty &&
                  computedGroups.isEmpty =>
              groupsFromShared(source, inOp.schema, keys, calls, builder)
            case _ =>
              None
        if fromGroups.isEmpty && inShare.isDefined then
          // The groups can't be shared: plan the aggregation again over the input with its IN
          // filter, from the same hidden names
          inShareDeclined.put(source, ())
          hiddenSeq = hiddenSeqAtStart
          return aggregation(input, items, defaultItems, plan, filter, builder)
        fromGroups.getOrElse((in, keys, calls))
      end val
      val inSchema = builder.op(aggIn).schema
      groupingSets match
        case None =>
          computedGroups
            .collect {
              // Binding the condition again registered no new call
              case (groups, n) if n == calls.size =>
                groups
            }
            .getOrElse(builder.aggregate(aggIn, keys, calls, aggCtx.outputSchema))
        case Some(sets)
            if enabled(Optimization.ChainGroupingSets) && sets.distinct.size > 1 &&
              sets.contains(sets.reduce(_ ++ _)) &&
              TwoPhaseAgg.supportsCalls(calls, enabled(Optimization.DependentGroupKeys)) =>
          chainGroupingSets(
            aggIn,
            keys,
            calls,
            sets,
            baseKeyExprs.size,
            aggCtx.outputSchema,
            builder
          )
        case Some(sets) =>
          // One aggregation per grouping set; keys outside the set are NULL, and the grouping id
          // has a bit set for each of them. The sets share one evaluation of the input, reduced
          // to the columns the keys and aggregates read and computed on first use
          val n    = baseKeyExprs.size
          val refs = mutable.BitSet.empty
          keys.foreach(VExprs.collectRefs(_, refs))
          calls.foreach { c =>
            c.args.foreach(VExprs.collectRefs(_, refs))
            c.filter.foreach(VExprs.collectRefs(_, refs))
          }
          val cols = refs.toIndexedSeq
          val m    = Array.fill(inSchema.size)(-1)
          cols.indices.foreach(i => m(cols(i)) = i)
          val reader      = builder.sharedInput(aggIn, cols, "GroupingSetInput")
          val setKeyExprs = keys.map(VExprs.remap(_, m))
          val setCalls    = calls.map(c =>
            c.copy(args = c.args.map(VExprs.remap(_, m)), filter = c.filter.map(VExprs.remap(_, m)))
          )
          builder.union(
            sets
              .map { set =>
                val gid     = (0 until n).filterNot(set.contains).map(k => 1L << k).sum
                val setKeys = (0 until n).map(k =>
                  if set.contains(k) then
                    setKeyExprs(k)
                  else
                    Const(null, keys(k).ptype)
                ) :+ Const(gid, PType.Int64)
                // The grand total (no keys in the set) is a global aggregation, so empty input
                // still yields one row
                groupByVaryingKeys(
                  reader(),
                  setKeys.toIndexedSeq,
                  setCalls,
                  aggCtx.outputSchema,
                  builder,
                  allowGlobal = set.isEmpty
                )
              }
              .toIndexedSeq,
            aggCtx.outputSchema
          )
      end match
    end aggOp
    val filtered = havingExpr.map(h => builder.filter(aggOp, h)).getOrElse(aggOp)
    collector match
      case Some(c) =>
        val rebased = c.rebase(builder.op(filtered).schema).frozen
        val win     = builder.window(filtered, rebased.windowCalls, rebased.schema)
        windowCollector = Some(rebased)
        val items2 =
          try bindSelectItems() ++ extraItems
          finally windowCollector = saved
        builder.project(win, items2.map(_._1), Schema(items2.map(_._2)))
      case None =>
        correlated match
          case None =>
            val items2 = firstPass ++ extraItems
            builder.project(filtered, items2.map(_._1), Schema(items2.map(_._2)))
          case Some(c) =>
            // The keys under their input columns' names and qualifiers, so that outer references
            // resolve as over the input (a select alias naming a key is no input column). Items
            // still find keys by `aggCtx.keyNames`
            val out       = builder.op(filtered).schema
            val qualified = Schema(
              out
                .fields
                .indices
                .map { i =>
                  keyExprs.lift(i).map(_._2) match
                    case Some(ColRef(k, _, _)) =>
                      out(i).copy(
                        name = inOp.schema(k).name,
                        qualifiers = inOp.schema(k).qualifiers
                      )
                    case _ =>
                      out(i)
                }
            )
            var p = builder.project(
              filtered,
              qualified.fields.indices.map(i => ColRef(i, qualified(i).ptype, qualified(i).name)),
              qualified
            )
            // A subquery correlated to an enclosing query further out stays unbound, as before:
            // the caller sees the original error
            val items2 =
              try
                val columns =
                  c.subqueries
                    .map { (name, sub) =>
                      val (joined, ref) = rewriteSubqueries(builder, p, sub, plan, filter)
                      p = joined
                      name -> ref
                    }
                    .toMap
                // The joins keep the aggregation's columns first, so keys and calls keep their refs
                val b = Binder(this, BindScope(builder.op(p).schema), Some(aggCtx), columns)
                bindSelectItems(b, c.items) ++ extraItems
              catch
                case NonFatal(_) =>
                  throw c.error
            builder.project(p, items2.map(_._1), Schema(items2.map(_._2)))
    end match

  end aggregation

  /**
    * The parts of an aggregation's input `source` that `share_in_subquery_groups` applies to: a
    * filter with a conjunct `target in (sub)`, where `sub` is uncorrelated and aggregates the same
    * rows as the filter without that conjunct (`rest`), by their loose keys
    * ([[ShareSubplans.looseKey]]). The other conjuncts have no subqueries, so that `rest` has a key
    */
  private def inSubqueryShare(source: Relation): Option[Planner.InShare] =
    source match
      case Filter(child, cond, span) =>
        val conds                              = splitConjuncts(cond)
        lazy val cteNames                      = cteNamesIn(source)
        def looseKey(r: Relation): Option[Any] = boundInput(r)
          .flatMap(ShareSubplans.looseKey)
          .map(_._1)
        val found = conds
          .indices
          .collectFirst {
            case i if conds(i).isInstanceOf[InRelation] =>
              (conds(i).asInstanceOf[InRelation], conds.patch(i, Nil, 1))
          }
        found.flatMap { (in, others) =>
          val rest =
            if others.isEmpty then
              child
            else
              Filter(child, others.reduce((a, b) => And(a, b, NoSpan)), span)
          lazy val restKey = looseKey(rest)
          // The inputs of the subquery's aggregations
          val aggInputs = List.newBuilder[Relation]
          in.in
            .traverse { case g: GroupBy =>
              aggInputs += g.child
            }
          Option.when(
            !hasSubquery(in.a) && !others.exists(hasSubquery) && !readsCTE(source, cteNames) &&
              restKey.isDefined && aggInputs.result().exists(r => looseKey(r) == restKey) &&
              isUncorrelated(in.in)
          )(Planner.InShare(rest, in.a, in.in))
        }
      case _ =>
        None

  /**
    * The groups of an aggregation whose input has a filter `target in (sub)` ([[inSubqueryShare]]),
    * where `target` is a grouping key: the groups of the input without the filter (`in`), computed
    * at plan time, which `sub`'s aggregations of the same input merge ([[groupsFromShared]]), then
    * filtered by the subquery's values. Returns them with the aggregation's keys (`keys`, whose
    * first are `baseKeys`, then a grouping id) and calls over them, or None when a call doesn't
    * merge over groups ([[SharedGroups.mergeable]]) or `target` is no key
    */
  private def groupsFilteredByIn[P](
      s: Planner.InShare,
      in: P,
      schema: Schema,
      baseKeys: IndexedSeq[VExpr],
      keys: IndexedSeq[VExpr],
      calls: IndexedSeq[AggCall],
      aggCtx: AggContext,
      builder: AggPlanBuilder[P]
  ): Option[(P, IndexedSeq[VExpr], IndexedSeq[AggCall])] =
    val n = baseKeys.size
    val t =
      try
        baseKeys.indexOf(Binder(this, BindScope(schema)).bind(s.target))
      catch
        case NonFatal(_) =>
          -1
    lazy val tType = baseKeys(t).ptype
    if t < 0 || !calls.forall(SharedGroups.mergeable) || tType == PType.Float64 then
      None
    else
      looseInputKey(s.rest, schema).flatMap { (key, ids) =>
        val out       = aggCtx.outputSchema
        val gSchema   = Schema(out.fields.take(n) ++ out.fields.drop(keys.size))
        val canonKeys = baseKeys.map(SharedGroups.canon(_, ids))
        val canonCall = calls.map(SharedGroups.canon(_, ids))
        val memoKey   = (key, canonKeys, canonCall, t, s.sub)
        val computed  = inGroupsComputed
          .get(memoKey)
          .orElse {
            val rows = builder.compute(builder.aggregate(in, baseKeys, calls, gSchema))
            val g    = SharedGroups(key, canonKeys, canonCall, gSchema, rows)
            sharedGroups += g
            val values =
              try
                val op = Planner.nested(this).plan(s.sub)
                Option.when(op.schema.size == 1)((Operator.materialize(op), op.schema(0).ptype))
              finally
                sharedGroups -= g
            values.map { (b, bt) =>
              inGroupsComputed(memoKey) = (rows, b, bt)
              (rows, b, bt)
            }
          }
        computed.flatMap { (rows, b, bt) =>
          val g = SharedGroups(key, canonKeys, canonCall, gSchema, rows)
          // The subquery's values in a type both sides compare in, as `bindInSubquery` does
          val vt     = PType.common(tType, bt)
          val col    = Kernels.castTo(b.columns(0), vt)
          val set    = (0 until b.size).map(col.get).filter(_ != null).toSet
          val target = castExpr(ColRef(t, tType, gSchema(t).name), vt)
          SharedGroups
            .merge(g, baseKeys, calls, ids)
            .filter(_ => vt != PType.Float64)
            .map { (ks, cs) =>
              val kept = builder.filter(
                builder.values(gSchema, rows),
                InList(target, set, hasNullValue = false, negate = false)
              )
              (kept, ks ++ keys.drop(n), cs)
            }
        }
      }
    end if

  end groupsFilteredByIn

  /**
    * An aggregation of `source` (planned to `schema`) by `keys` of `calls` as an aggregation of
    * groups of the same input computed at plan time ([[groupsFilteredByIn]]), when they have its
    * keys and calls
    */
  private def groupsFromShared[P](
      source: Relation,
      schema: Schema,
      keys: IndexedSeq[VExpr],
      calls: IndexedSeq[AggCall],
      builder: AggPlanBuilder[P]
  ): Option[(P, IndexedSeq[VExpr], IndexedSeq[AggCall])] = looseInputKey(source, schema).flatMap {
    (key, ids) =>
      sharedGroups
        .reverseIterator
        .filter(_.key == key)
        .flatMap(g =>
          SharedGroups
            .merge(g, keys, calls, ids)
            .map { (ks, cs) =>
              debug(s"Merging ${g.rows.size} shared groups by ${ks.size} of their keys")
              (builder.values(g.schema, g.rows), ks, cs)
            }
        )
        .nextOption()
  }

  /**
    * Compute the groups of an aggregation at plan time when uncorrelated scalar subqueries of its
    * condition (`having`) aggregate the same input as the aggregation: each subquery's value then
    * comes from the groups (e.g. TPC-H q11's total value is the sum of the per-part sums), so the
    * input is read once instead of once more for the subquery. A subquery qualifies when its bound
    * input equals the aggregation's ([[ShareSubplans.key]]) and its value is an expression of `sum`
    * over integers or decimals, `min`, and `max`, whose totals over the groups equal their values
    * over the input. The values fill [[scalarResults]]; the result is the computed groups, with the
    * number of calls they compute, or None when no subquery qualifies
    */
  private def computeGroupTotals[P](
      having: Expression,
      source: Relation,
      in: P,
      aggCtx: AggContext,
      aggBinder: Binder,
      builder: AggPlanBuilder[P]
  ): Option[(P, Int)] =
    val subs = List.newBuilder[Project]
    // Whether the condition has other subqueries, or subqueries inside aggregate calls: binding
    // it before the groups are computed would evaluate them, or register calls that differ once
    // the subqueries' values are known
    var others = false
    // Subqueries of the condition itself, not those nested in other subqueries
    def visit(x: Expression): Expression =
      x match
        case s: SubQueryExpression =>
          s.query match
            case p: Project
                if p.selectItems.size == 1 && !scalarResults.containsKey(p) &&
                  Binder.containsAggregate(p.selectItems.head) &&
                  !hasSubquery(p.selectItems.head) =>
              subs += p
            case _ =>
              others = true
          x
        case _: Exists | _: InRelation | _: NotInRelation =>
          others = true
          x
        case f: FunctionApply
            if f.window.isEmpty && Aggregates.isAggregate(Binder.functionName(f)) =>
          others ||= hasSubquery(f)
          x
        case other =>
          other.mapChildExpressions(visit)
    visit(having)
    val candidates     = subs.result().distinct
    lazy val cteNames  = cteNamesIn(source)
    lazy val sourceKey = Option.unless(readsCTE(source, cteNames))(inputKey(source)).flatten
    val numKeys        = aggCtx.keys.size
    // Whether the value of a subquery over the aggregation's input totals over its groups
    def totalsOverGroups(item: Attribute): Boolean =
      val ctx = AggContext(aggCtx.input, aggCtx.keyNames, aggCtx.keys, aggCtx.groupingId)
      try
        val value = Binder(this, BindScope(ctx.outputSchema), Some(ctx)).bind(item)
        val refs  = mutable.BitSet.empty
        VExprs.collectRefs(value, refs)
        refs.forall(_ >= numKeys) && ctx.calls.forall(SharedGroups.mergeable)
      catch
        case NonFatal(_) =>
          false
    val shared = candidates.filter(p =>
      numKeys > 0 && sourceKey.isDefined && totalsOverGroups(p.selectItems.head) &&
        !isCorrelated(p, Schema.empty) && !readsCTE(p.child, cteNames) &&
        inputKey(p.child) == sourceKey
    )
    if others || shared.isEmpty || shared.size != candidates.size then
      None
    else
      val values = shared.map(p => p -> aggBinder.bind(p.selectItems.head))
      // Register the condition's calls before computing the groups, with a NULL of each
      // subquery's type in place of its value
      values.foreach((p, value) => scalarResults.put(p, Const(null, value.ptype)))
      val bound =
        try
          aggBinder.bind(having)
          true
        catch
          case NonFatal(e) =>
            debug(s"Not totaling over the groups: ${e.getMessage}")
            values.foreach((p, _) => scalarResults.remove(p))
            false
      Option.when(bound) {
        val schema = aggCtx.outputSchema
        val calls  = aggCtx.calls.toIndexedSeq
        val rows   = builder.compute(builder.aggregate(in, aggCtx.keys, calls, schema))
        values.foreach { (p, value) =>
          scalarResults.put(p, groupTotal(rows, schema, calls, numKeys, value))
        }
        (builder.values(schema, rows), calls.size)
      }

  end computeGroupTotals

  /**
    * The value of `value`, an expression over the calls of an aggregation, over all its groups
    * (`rows`): each call it reads totals over the groups (a sum of sums, the min of mins, the max
    * of maxes)
    */
  private def groupTotal(
      rows: Batch,
      schema: Schema,
      calls: IndexedSeq[AggCall],
      numKeys: Int,
      value: VExpr
  ): VExpr =
    val refs = mutable.BitSet.empty
    VExprs.collectRefs(value, refs)
    val cols = refs.toIndexedSeq
    val m    = Array.fill(schema.size)(-1)
    cols.indices.foreach(i => m(cols(i)) = i)
    val totals = cols.map { j =>
      val f = schema(j)
      AggCall(calls(j - numKeys).name, Seq(ColRef(j, f.ptype, f.name)), false, None, f.ptype)
    }
    val totalSchema = Schema(cols.map(schema(_)))
    val total       = HashAggOp(BatchesOp(schema, Seq(rows)), IndexedSeq.empty, totals, totalSchema)
    val result      = Operator.materialize(
      ProjectOp(
        total,
        IndexedSeq(VExprs.remap(value, m)),
        Schema(IndexedSeq(Field("v", value.ptype)))
      )
    )
    val v =
      if result.size == 0 then
        null
      else
        result.columns(0).get(0)
    Const(v, value.ptype)

  end groupTotal

  /**
    * Select items of an aggregation with each correlated subquery in place of a hidden name, the
    * subqueries by name, and the binding error they gave
    */
  private case class CorrelatedItems(
      error: ColumnNotFound,
      items: List[Attribute],
      subqueries: List[(String, Expression)]
  )

  /**
    * `e` with each correlated scalar subquery (or EXISTS) in place of a hidden name, and the
    * subqueries by name: the same top-down walk as `rewriteSubqueries`
    */
  private def extractCorrelatedSubqueries(e: Expression): (Expression, List[(String, Expression)]) =
    val found                            = List.newBuilder[(String, Expression)]
    def named(x: Expression): Expression =
      val name = hiddenName("sqi")
      found += name -> x
      UnquotedIdentifier(name, NoSpan)
    def visit(x: Expression): Expression =
      x match
        case s: SubQueryExpression if isCorrelated(s.query, Schema.empty) =>
          named(s)
        case ex: Exists if isCorrelated(subqueryRelation(ex.child), Schema.empty) =>
          named(ex)
        case _: SubQueryExpression | _: Exists | _: InRelation | _: NotInRelation =>
          x
        case other =>
          other.mapChildExpressions(visit)
    val rewritten =
      e match
        case s: SingleColumn =>
          visit(s.expr)
        case a: Alias =>
          visit(a.expr)
        case other =>
          visit(other)
    (rewritten, found.result())

  /**
    * Grouping sets whose key union is one of the sets (ROLLUP, CUBE), computed from finer sets: the
    * finest set aggregates `in` into partial results ([[TwoPhaseAgg.decompose]]), and each coarser
    * set merges the partial results of its smallest finer set. Each set's rows are its finished
    * partial results, with the keys outside the set NULL and the grouping id set, as one
    * aggregation per set over `in` gives them
    *
    * @param keys
    *   the `n` grouping keys, then the grouping id
    * @param out
    *   the keys, the grouping id, then the calls
    */
  private def chainGroupingSets[P](
      in: P,
      keys: IndexedSeq[VExpr],
      calls: IndexedSeq[AggCall],
      sets: List[Set[Int]],
      n: Int,
      out: Schema,
      builder: AggPlanBuilder[P]
  ): P =
    val (partials, sources) = TwoPhaseAgg.decompose(calls)
    // Finest first, so that every set's finer sets come before it
    val order = sets.distinct.sortBy(-_.size).toIndexedSeq
    // A set's partial results: its keys in key order, then the partial aggregates
    def keysOf(set: Set[Int]): IndexedSeq[Int] = (0 until n).filter(set.contains)
    def schemaOf(set: Set[Int]): Schema        = Schema(
      keysOf(set).map(out(_)) ++
        partials.zipWithIndex.map((c, j) => Field(s"__partial_${j}", c.ptype))
    )
    // The smallest finer set (the first, among equal sizes) each coarser set merges
    val sourceOf: Map[Set[Int], Set[Int]] =
      order
        .indices
        .drop(1)
        .map { i =>
          val set = order(i)
          set -> order.take(i).filter(f => set.subsetOf(f)).minBy(_.size)
        }
        .toMap
    // Readers of each set's partial results: the coarser sets merging them and its own rows
    def reads(set: Set[Int]): Int = sourceOf.values.count(_ == set) + sets.count(_ == set)
    val readers                   = mutable.Map.empty[Set[Int], () => P]
    order.foreach { set =>
      val schema = schemaOf(set)
      val agg    =
        sourceOf.get(set) match
          case None =>
            groupByVaryingKeys(
              in,
              keysOf(set).map(keys),
              partials,
              schema,
              builder,
              allowGlobal = set.isEmpty
            )
          case Some(finer) =>
            val fk                 = keysOf(finer)
            val fs                 = schemaOf(finer)
            def col(i: Int): VExpr = ColRef(i, fs(i).ptype, fs(i).name)
            val merged = partials.indices.map(j => TwoPhaseAgg.merge(partials(j), col(fk.size + j)))
            groupByVaryingKeys(
              readers(finer)(),
              keysOf(set).map(k => col(fk.indexOf(k))),
              merged,
              schema,
              builder,
              allowGlobal = set.isEmpty
            )
      readers(set) =
        if reads(set) > 1 then
          builder.sharedInput(agg, schema.fields.indices, "GroupingSetPartials")
        else
          () => agg
    }
    builder.union(
      sets
        .map { set =>
          val ks                 = keysOf(set)
          val schema             = schemaOf(set)
          def col(i: Int): VExpr = ColRef(i, schema(i).ptype, schema(i).name)
          val gid                = (0 until n).filterNot(set.contains).map(k => 1L << k).sum
          val keyOut             = (0 until n).map { k =>
            ks.indexOf(k) match
              case -1 =>
                Const(null, keys(k).ptype)
              case i =>
                col(i)
          } :+ Const(gid, PType.Int64)
          val callOut = calls
            .zip(sources)
            .map { (c, src) =>
              val v = TwoPhaseAgg.finish(c, src.map(j => col(ks.size + j)))
              // A count over no rows (the grand total of an empty input) is 0
              if TwoPhaseAgg.isCount(c) then
                Coalesce(Seq(v, Const(0L, PType.Int64)), PType.Int64)
              else
                v
            }
          builder.project(readers(set)(), keyOut ++ callOut, out)
        }
        .toIndexedSeq,
      out
    )

  end chainGroupingSets

  /**
    * A hash aggregation that groups only by the keys that are not constants (a grouping set's NULL
    * keys and grouping id): constants are the same in every row, so they don't change the groups,
    * and a projection puts them back. Output: the keys, then the aggregates, as in `out`. With
    * `allowGlobal`, all-constant keys make a global aggregation (a grouping set's grand total)
    */
  private def groupByVaryingKeys[P](
      input: P,
      keys: IndexedSeq[VExpr],
      calls: IndexedSeq[AggCall],
      out: Schema,
      builder: AggPlanBuilder[P],
      allowGlobal: Boolean
  ): P =
    val varying = keys.indices.filterNot(k => keys(k).isInstanceOf[Const])
    // Without varying keys, the aggregation becomes global (one row even for empty input), which
    // only the caller can allow
    if varying.size == keys.size || (varying.isEmpty && !allowGlobal) then
      builder.aggregate(input, keys, calls, out)
    else
      val nk  = keys.size
      val agg = builder.aggregate(
        input,
        varying.map(keys),
        calls,
        Schema(varying.map(out(_)) ++ out.fields.drop(nk))
      )
      val keyExprs = (0 until nk).map { k =>
        val pos = varying.indexOf(k)
        if pos >= 0 then
          ColRef(pos, out(k).ptype, out(k).name)
        else
          keys(k)
      }
      val callExprs = (nk until out.size).map(c =>
        ColRef(varying.size + c - nk, out(c).ptype, out(c).name)
      )
      builder.project(agg, keyExprs ++ callExprs, out)

  end groupByVaryingKeys

  // --- sort -----------------------------------------------------------------------------

  private[engine] def sortKeys(
      binder: Binder,
      items: List[SortItem],
      schema: Schema
  ): Seq[SortKey] = items.map { s =>
    val e =
      s.sortKey match
        case l: LongLiteral =>
          val i = l.value.toInt - 1
          ColRef(binder.scope.offset + i, schema(i).ptype, schema(i).name)
        case other =>
          binder.bind(other)
    sortKey(s, e)
  }

  private[engine] def sortKey(s: SortItem, e: VExpr): SortKey =
    // DuckDB's default: NULLS LAST for both directions
    val nullsFirst = s.nullOrdering.contains(NullOrdering.NullIsFirst)
    SortKey(e, !s.ordering.contains(SortOrdering.Descending), nullsFirst)

  /** SQL text of an expression, used to match ORDER BY keys with select items */
  private def exprText(e: Expression): String =
    e match
      case p: ParenthesizedExpression =>
        exprText(p.child)
      case other =>
        printSql(other)

  private def itemExpr(a: Attribute): Option[Expression] =
    a match
      case s: SingleColumn =>
        Some(s.expr)
      case al: Alias =>
        Some(al.expr)
      case _ =>
        None

  /**
    * Rewrite ORDER BY keys that repeat a select item expression (e.g., `dt.d_year`,
    * `substring(s_city, 1, 30)`, `count(*)`) or name a column of a select item (`i_brand_id` for
    * `item.i_brand_id as brand_id`) into positional references to the select output. Keys naming an
    * output column are kept as is
    */
  private[engine] def resolveSortItems(s: Sort): List[SortItem] =
    def selectItems(r: Relation): Option[List[Attribute]] =
      r match
        case p: Project =>
          Some(p.selectItems)
        case d: Distinct =>
          selectItems(d.child)
        case _ =>
          None
    selectItems(s.child) match
      case Some(items) if !items.exists {
            case _: AllColumns =>
              true
            case i =>
              itemExpr(i).exists(isStar)
          } =>
        val outNames = items.map(i => nameOf(i).toLowerCase).toSet
        val texts    = items.map(i => itemExpr(i).map(exprText))
        s.orderBy
          .map { k =>
            k.sortKey match
              case _: LongLiteral =>
                k
              case i: Identifier if outNames.contains(i.leafName.toLowerCase) =>
                k
              case key =>
                val text   = exprText(key)
                val byExpr = texts.indexWhere(_.contains(text))
                val idx    =
                  if byExpr >= 0 then
                    byExpr
                  else
                    key match
                      case i: Identifier =>
                        // An unqualified column of a qualified select item
                        val leaf = i.leafName.toLowerCase
                        val hits = items
                          .indices
                          .filter { j =>
                            itemExpr(items(j)) match
                              case Some(d: DotRef) =>
                                d.name.leafName.toLowerCase == leaf
                              case _ =>
                                false
                          }
                        if hits.size == 1 then
                          hits.head
                        else
                          -1
                      case _ =>
                        -1
                if idx >= 0 then
                  k.copy(sortKey = LongLiteral(idx + 1, (idx + 1).toString, key.span))
                else
                  k
          }
      case _ =>
        s.orderBy
    end match

  end resolveSortItems

  // --- filters and subqueries -----------------------------------------------------------

  private[engine] def conjuncts(e: Expression): List[Expression] = factorOrs(splitConjuncts(e))

  /** Factor the common conjuncts out of the `or`s among conjuncts, when FactorOr is enabled */
  private[engine] def factorOrs(conds: List[Expression]): List[Expression] =
    if enabled(Optimization.FactorOr) then
      conds.flatMap(factorOr)
    else
      conds

  /** The conjuncts of a condition, without factoring common conjuncts out of `or`s */
  private[engine] def splitConjuncts(e: Expression): List[Expression] = splitAnd(
    Binder.expandTemplates(e)
  )

  private def splitOr(e: Expression): List[Expression] =
    e match
      case o: Or =>
        splitOr(o.left) ++ splitOr(o.right)
      case p: ParenthesizedExpression =>
        splitOr(p.child)
      case other =>
        List(other)

  /**
    * The key [[factorOr]] matches conjuncts by: their SQL text, with column qualifiers, quoted
    * literals (whitespace kept), and LIKE's ESCAPE. Not [[exprName]], which names `l.b` and `r.b`
    * alike as `b`, nor [[printSql]], which reads `'x  y'` as `'x y'`
    */
  private def exprKey(e: Expression): String = sqlText(e)

  /** (a and b) or (a and c) => a and (b or c), so join keys inside OR branches become usable */
  private def factorOr(e: Expression): List[Expression] =
    e match
      case o: Or =>
        val branches = splitOr(o).map(splitAnd)
        val keySets  = branches.map(_.map(exprKey).toSet)
        val common   = keySets.reduce(_ intersect _)
        if common.isEmpty then
          List(e)
        else
          val commonExprs = branches.head.filter(c => common.contains(exprKey(c)))
          val rest        = branches.map(_.filterNot(c => common.contains(exprKey(c))))
          if rest.exists(_.isEmpty) then
            commonExprs
          else
            commonExprs :+
              rest.map(_.reduce((a, b) => And(a, b, NoSpan))).reduce((a, b) => Or(a, b, NoSpan))
      case other =>
        List(other)

  private def splitAnd(e: Expression): List[Expression] =
    e match
      case a: And =>
        splitAnd(a.left) ++ splitAnd(a.right)
      case p: ParenthesizedExpression =>
        splitAnd(p.child)
      case other =>
        List(other)

  /**
    * Bind one conjunct over `input`, whose correlated scalar subqueries ([[decorrelateScalar]]) and
    * EXISTS under OR / NOT / CASE ([[decorrelateExists]]) become left joins extending it, with
    * their bodies planned by `plan` and `filtered` as in [[planCorrelated]]. Returns the extended
    * input and the condition bound over it
    */
  private[engine] def bindWithSubqueries[P](
      b: JoinPlanBuilder[P] & AggPlanBuilder[P],
      input: P,
      cond: Expression,
      plan: Relation => P,
      filtered: Filter => P
  ): (P, VExpr) =
    val (p, rewritten) = rewriteSubqueries(b, input, cond, plan, filtered)
    (p, Binder(this, BindScope(b.op(p).schema)).bind(rewritten))

  /**
    * Rewrite the correlated scalar subqueries ([[decorrelateScalar]]) and EXISTS
    * ([[decorrelateExists]]) of `expr` over `input` into references to columns of left joins
    * extending it. Returns the extended input and the rewritten expression, which binds over the
    * extended input's schema
    */
  private[engine] def rewriteSubqueries[P](
      b: JoinPlanBuilder[P] & AggPlanBuilder[P],
      input: P,
      expr: Expression,
      plan: Relation => P,
      filtered: Filter => P
  ): (P, Expression) =
    var p                                  = input
    def rewrite(e: Expression): Expression =
      e match
        case s: SubQueryExpression if isCorrelated(s.query, b.op(p).schema) =>
          val (joined, column) = decorrelateScalar(b, p, s.query, plan, filtered)
          p = joined
          UnquotedIdentifier(column, NoSpan)
        case ex: Exists if isCorrelated(subqueryRelation(ex.child), b.op(p).schema) =>
          // EXISTS under OR / NOT / CASE: a match flag column
          val (joined, column) = decorrelateExists(b, p, subqueryRelation(ex.child), plan, filtered)
          p = joined
          UnquotedIdentifier(column, NoSpan)
        case _: SubQueryExpression | _: Exists | _: InRelation | _: NotInRelation =>
          e
        case other =>
          // Top-down, so that subqueries are rewritten as a whole, not from their insides
          other.mapChildExpressions(rewrite)
    val rewritten = rewrite(expr)
    (p, rewritten)

  end rewriteSubqueries

  /** Whether a subquery references columns of the enclosing schema */
  private def isCorrelated(sub: Relation, outer: Schema): Boolean =
    try
      Planner.nested(this).planRelation(sub)
      false
    catch
      case e: ColumnNotFound =>
        true

  /**
    * A filter conjunct that is EXISTS, NOT EXISTS, IN, or NOT IN over a subquery (correlated or
    * not), which the runtime binder turns into a join
    */
  private[engine] def subqueryConjunct(c: Expression): Option[SubqueryConjunct] =
    c match
      case e: Exists =>
        Some(SubqueryConjunct.Exists(subqueryRelation(e.child), negate = false))
      case Not(e: Exists, _) =>
        Some(SubqueryConjunct.Exists(subqueryRelation(e.child), negate = true))
      case in: InRelation =>
        Some(SubqueryConjunct.In(in.a, in.in, negate = false))
      case in: NotInRelation =>
        Some(SubqueryConjunct.In(in.a, in.in, negate = true))
      case Not(in: InRelation, _) =>
        Some(SubqueryConjunct.In(in.a, in.in, negate = true))
      case _ =>
        None

  /** Whether a subquery reads no column of an enclosing relation */
  private[engine] def isUncorrelated(sub: Relation): Boolean = !isCorrelated(sub, Schema.empty)

  private def subqueryRelation(e: Expression): Relation =
    e match
      case s: SubQueryExpression =>
        s.query
      case p: ParenthesizedExpression =>
        subqueryRelation(p.child)
      case r: Relation =>
        r
      case other =>
        throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported EXISTS argument: ${other}")

  /** Peel wrappers that don't change the rows of a subquery */
  private def stripWrappers(r: Relation): Relation =
    r match
      case q: Query =>
        stripWrappers(q.body)
      case b: BracedRelation =>
        stripWrappers(b.child)
      case s: SelectAsAlias =>
        stripWrappers(s.child)
      case other =>
        other

  /**
    * Plan a correlated subquery body `[Project] Filter(child, cond)`: conjuncts referencing only
    * the child stay as a filter (`filtered`, or `plan` of the child when there are none); the rest
    * are returned as correlated conditions, with the select items
    */
  private def planCorrelated[P](
      sub: Relation,
      plan: Relation => P,
      filtered: Filter => P
  ): (P, List[Expression], Option[List[Attribute]]) =
    val (items, body) =
      stripWrappers(sub) match
        case p: Project =>
          (Some(p.selectItems), stripWrappers(p.child))
        case d: Distinct =>
          (Some(d.child.selectItems), stripWrappers(d.child.child))
        case other =>
          (None, other)
    body match
      case f: Filter =>
        // Planning is lazy, so planning the child only to learn its schema is cheap
        val childSchema         = planRelation(f.child).schema
        val (local, correlated) = conjuncts(f.filterExpr).partition(c => bindsIn(c, childSchema))
        val body                =
          if local.isEmpty then
            plan(f.child)
          else
            filtered(Filter(f.child, local.reduce((a, b) => And(a, b, NoSpan)), f.span))
        (body, correlated, items)
      case other =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(s"Unsupported correlated subquery shape: ${other.nodeName}")

  private[engine] def bindsIn(e: Expression, schema: Schema): Boolean =
    try
      Binder(Planner.nested(this), BindScope(schema)).bind(e)
      true
    catch
      case _: ColumnNotFound =>
        false

  /** Split correlated conditions into (inner key, outer key) equalities and the rest */
  private def correlationKeys(
      conds: List[Expression],
      inner: Schema,
      outer: Schema
  ): (List[(Expression, Expression)], List[Expression]) =
    val keys  = List.newBuilder[(Expression, Expression)]
    val other = List.newBuilder[Expression]
    conds.foreach {
      // Wvlet's `x = null` (and `x is null`) is a null check (see Binder), not a key
      case c @ Eq(_, _: NullLiteral, _) =>
        other += c
      case eq: Eq
          if bindsIn(eq.left, inner) && !bindsIn(eq.right, inner) && bindsIn(eq.right, outer) =>
        keys += ((eq.left, eq.right))
      case eq: Eq
          if bindsIn(eq.right, inner) && !bindsIn(eq.left, inner) && bindsIn(eq.left, outer) =>
        keys += ((eq.right, eq.left))
      case c =>
        other += c
    }
    (keys.result(), other.result())

  /** Equi-join keys bound on both sides and cast to common types */
  private def joinKeys(
      lb: Binder,
      rb: Binder,
      pairs: Seq[(Expression, Expression)]
  ): (IndexedSeq[VExpr], IndexedSeq[VExpr]) =
    val bound = pairs.map { (l, r) =>
      val le = lb.bind(l)
      val re = rb.bind(r)
      val t  = PType.common(le.ptype, re.ptype)
      (castExpr(le, t), castExpr(re, t))
    }
    (bound.map(_._1).toIndexedSeq, bound.map(_._2).toIndexedSeq)

  private[engine] def castExpr(e: VExpr, t: PType): VExpr =
    if e.ptype == t then
      e
    else
      CastExpr(e, t)

  /** Residual condition over (outer ++ inner) with inner-first name resolution */
  private def residualBinder(outer: Schema, inner: Schema): Binder = Binder(
    this,
    BindScope(inner, offset = outer.size, parent = Some(BindScope(outer)))
  )

  /**
    * A correlated [NOT] EXISTS / [NOT] IN filter conjunct over `p` as a semi or anti join with the
    * decorrelated subquery body, which `plan` and `filtered` plan as in [[planCorrelated]]. The
    * correlation equalities are hash keys (a nested loop join without any), the other correlated
    * conditions a residual over p ++ body, with inner names resolving first. An IN also joins on
    * the target and the select item.
    *
    * NOT IN keeps SQL semantics: a row passes when no body row of its correlation group makes
    * `target <> value` other than TRUE, that is, an anti join on the correlation keys with the
    * residual `rest and (target = value or target is null or value is null)`
    */
  private[engine] def correlatedJoin[P](
      b: JoinPlanBuilder[P] & AggPlanBuilder[P],
      p: P,
      s: SubqueryConjunct,
      plan: Relation => P,
      filtered: Filter => P
  ): P =
    val outer                      = b.op(p).schema
    val (inner, correlated, items) = planCorrelated(s.sub, plan, filtered)
    val innerSchema                = b.op(inner).schema
    val (pairs, rest)              = correlationKeys(correlated, innerSchema, outer)
    val lb                         = Binder(this, BindScope(outer))
    val (lk, rk) = joinKeys(lb, Binder(this, BindScope(innerSchema)), pairs.map(p => (p._2, p._1)))
    def residual(body: Schema): List[VExpr] = rest.map(residualBinder(outer, body).bind)
    def join(
        body: P,
        kind: JoinKind,
        lk: IndexedSeq[VExpr],
        rk: IndexedSeq[VExpr],
        res: List[VExpr]
    ) =
      // Build on the body unless it is larger than p
      val buildLeft =
        lk.nonEmpty &&
          swapSemiBuild(
            b.buildRows(body),
            b.buildRows(p),
            CardinalityEstimator.aggregates(b.op(body))
          )
      b.join(p, body, kind, lk, rk, res.reduceOption((x, y) => AndExpr(x, y)), buildLeft)
    s match
      case SubqueryConjunct.Exists(_, negate) =>
        val kind =
          if negate then
            JoinKind.Anti
          else
            JoinKind.Semi
        join(inner, kind, lk, rk, residual(innerSchema))
      case SubqueryConjunct.In(a, _, negate) =>
        // The compared value: the single select item, after the body's columns
        val (valued, valueIdx) =
          items match
            case Some(its) =>
              val (exprs, fields) = bindItems(Binder(this, BindScope(innerSchema)), its)
              val proj            = b.project(
                inner,
                innerSchema
                  .fields
                  .indices
                  .map(i => ColRef(i, innerSchema(i).ptype, innerSchema(i).name)) ++ exprs,
                Schema(innerSchema.fields ++ fields.map(f => f.withName(hiddenName("in"))))
              )
              (proj, innerSchema.size)
            case None =>
              (inner, 0)
        val vs     = b.op(valued).schema
        val target = lb.bind(a)
        val t      = PType.common(target.ptype, vs(valueIdx).ptype)
        val x      = castExpr(target, t)
        if !negate then
          val y = castExpr(ColRef(valueIdx, vs(valueIdx).ptype, vs(valueIdx).name), t)
          join(valued, JoinKind.Semi, x +: lk, y +: rk, residual(vs))
        else
          // The value over p ++ body, as the residual reads it
          val y = castExpr(ColRef(outer.size + valueIdx, vs(valueIdx).ptype, vs(valueIdx).name), t)
          val same = OrExpr(
            OrExpr(Compare(CmpOp.Eq, x, y), IsNullExpr(x, negate = false)),
            IsNullExpr(y, negate = false)
          )
          join(valued, JoinKind.Anti, lk, rk, residual(vs) :+ same)
    end match

  end correlatedJoin

  /**
    * A correlated scalar subquery `[Project(agg)] Filter(child, k_inner = k_outer and ...)` as an
    * aggregation grouped by the inner keys, left-joined to the outer input. Returns the joined plan
    * and the name of the column holding the subquery value.
    *
    * An outer row without a group gets the subquery's value over no rows: NULL for most aggregates,
    * but 0 for `count`. When that value is not NULL, it is projected for the rows whose (never NULL
    * when matched) joined key is NULL
    */
  private def decorrelateScalar[P](
      b: JoinPlanBuilder[P] & AggPlanBuilder[P],
      outer: P,
      sub: Relation,
      plan: Relation => P,
      filtered: Filter => P
  ): (P, String) =
    val outerSchema                = b.op(outer).schema
    val (inner, correlated, items) = planCorrelated(sub, plan, filtered)
    val innerSchema                = b.op(inner).schema
    val (pairs, rest)              = correlationKeys(correlated, innerSchema, outerSchema)
    if rest.nonEmpty || pairs.isEmpty then
      throw StatusCode
        .NOT_IMPLEMENTED
        .newException(
          s"Unsupported correlated scalar subquery: only equality correlations are supported: ${rest
              .map(_.toString)
              .mkString(", ")} (outer: ${outerSchema.names.mkString(",")}; inner: ${innerSchema
              .names
              .mkString(",")})"
        )
    val innerScope = BindScope(innerSchema)
    val ib         = Binder(this, innerScope)
    val ob         = Binder(this, BindScope(outerSchema))
    val keyNames   = pairs.indices.map(_ => hiddenName("k"))
    val innerKeys  = pairs.map(p => ib.bind(p._1)).toIndexedSeq
    val aggCtx     = AggContext(innerScope, keyNames, innerKeys)
    val item       = items
      .flatMap(_.headOption)
      .getOrElse {
        throw StatusCode.NOT_IMPLEMENTED.newException("A scalar subquery needs a select item")
      }
    val value     = Binder(this, BindScope(aggCtx.outputSchema), Some(aggCtx)).bind(item)
    val calls     = aggCtx.calls.toIndexedSeq
    val agg       = b.aggregate(inner, innerKeys, calls, aggCtx.outputSchema)
    val valueName = hiddenName("sq")
    val proj      = b.project(
      agg,
      innerKeys.indices.map(i => aggCtx.keyRef(i)).toIndexedSeq :+ value,
      Schema(
        keyNames.indices.map(i => Field(keyNames(i), innerKeys(i).ptype)).toIndexedSeq :+
          Field(valueName, value.ptype)
      )
    )
    val outerKeys = pairs.map(p => ob.bind(p._2)).toIndexedSeq
    val lk = outerKeys.zip(innerKeys).map((o, i) => castExpr(o, PType.common(o.ptype, i.ptype)))
    val rk = innerKeys
      .indices
      .map(i => castExpr(aggCtx.keyRef(i), PType.common(outerKeys(i).ptype, innerKeys(i).ptype)))
    val joined = b.join(outer, proj, JoinKind.Left, lk, rk, None, buildLeft = false)
    emptyGroupValue(innerSchema, innerKeys, calls, aggCtx.outputSchema, value) match
      case None =>
        (joined, valueName)
      case Some(empty) =>
        val js        = b.op(joined).schema
        val keyIdx    = outerSchema.size
        val valueIdx  = keyIdx + innerKeys.size
        val unmatched = IsNullExpr(
          ColRef(keyIdx, js(keyIdx).ptype, js(keyIdx).name),
          negate = false
        )
        val v     = ColRef(valueIdx, js(valueIdx).ptype, js(valueIdx).name)
        val exact = CaseWhen(Seq(unmatched -> empty), Some(v), v.ptype)
        val exprs = js
          .fields
          .indices
          .map(i =>
            if i == valueIdx then
              exact
            else
              ColRef(i, js(i).ptype, js(i).name)
          )
        (b.project(joined, exprs, js), valueName)

  end decorrelateScalar

  /**
    * The value of a scalar subquery over a group without rows, computed at plan time: None when it
    * is NULL
    */
  private def emptyGroupValue(
      inputSchema: Schema,
      keys: IndexedSeq[VExpr],
      calls: IndexedSeq[AggCall],
      aggSchema: Schema,
      value: VExpr
  ): Option[VExpr] =
    val callFields = aggSchema.fields.drop(keys.size)
    val global     = HashAggOp(
      BatchesOp(inputSchema, Seq.empty),
      IndexedSeq.empty,
      calls,
      Schema(callFields)
    )
    // The aggregation's layout (keys, then calls), with NULL keys
    val row = ProjectOp(
      global,
      keys.map(k => Const(null, k.ptype)) ++
        callFields.indices.map(i => ColRef(i, callFields(i).ptype, callFields(i).name)),
      aggSchema
    )
    val result = Operator.materialize(
      ProjectOp(row, IndexedSeq(value), Schema(IndexedSeq(Field("v", value.ptype))))
    )
    val v = result.columns(0)
    if result.size == 0 || v.isNull(0) then
      None
    else
      Some(Const(v.get(0), value.ptype))

  /**
    * A correlated EXISTS as a boolean column: left join `outer` with the distinct correlation keys
    * of the subquery and flag the matched rows (never NULL)
    */
  private def decorrelateExists[P](
      b: JoinPlanBuilder[P] & AggPlanBuilder[P],
      outer: P,
      sub: Relation,
      plan: Relation => P,
      filtered: Filter => P
  ): (P, String) =
    val outerSchema            = b.op(outer).schema
    val (inner, correlated, _) = planCorrelated(sub, plan, filtered)
    val innerSchema            = b.op(inner).schema
    val (pairs, rest)          = correlationKeys(correlated, innerSchema, outerSchema)
    if rest.nonEmpty || pairs.isEmpty then
      throw StatusCode
        .NOT_IMPLEMENTED
        .newException(
          s"Unsupported correlated EXISTS: only equality correlations are supported: ${rest
              .mkString(", ")}"
        )
    val (lk, rk) = joinKeys(
      Binder(this, BindScope(outerSchema)),
      Binder(this, BindScope(innerSchema)),
      pairs.map(p => (p._2, p._1))
    )
    val keyFields = rk.map(k => Field(hiddenName("k"), k.ptype))
    val distinct  = b.aggregate(inner, rk, IndexedSeq.empty, Schema(keyFields))
    val keyRefs   = keyFields.indices.map(i => ColRef(i, keyFields(i).ptype, keyFields(i).name))
    val joined    = b.join(outer, distinct, JoinKind.Left, lk, keyRefs, None, buildLeft = false)
    val markName  = hiddenName("exists")
    val marked    = b.project(
      joined,
      outerSchema.fields.indices.map(i => ColRef(i, outerSchema(i).ptype, outerSchema(i).name)) :+
        IsNullExpr(ColRef(outerSchema.size, keyFields(0).ptype, keyFields(0).name), negate = true),
      Schema(outerSchema.fields :+ Field(markName, PType.Bool))
    )
    (marked, markName)

  end decorrelateExists

  /**
    * Whether `e` calls a non-deterministic function (`random()`), whose calls each draw their own
    * values: merged into one query, equal calls would be computed once
    */
  private def callsVolatile(e: Expression): Boolean =
    var found = false
    e.traverseExpressions {
      case f: FunctionApply
          if ScalarFunctions.volatileFunctions.contains(Binder.functionName(f).toLowerCase) =>
        found = true
    }
    found

  /** Uncorrelated scalar subquery as a constant */
  def bindScalarSubquery(binder: Binder, sub: Relation): VExpr =
    val cached = scalarResults.get(sub)
    if cached != null then
      cached
    else
      val v = evalScalarSubquery(sub)
      scalarResults.put(sub, v)
      v

  private def evalScalarSubquery(sub: Relation): VExpr =
    val op = Planner.nested(this).plan(sub)
    val b  = Operator.materialize(op)
    if b.size > 1 then
      throw StatusCode.INVALID_ARGUMENT.newException("A scalar subquery returned more than one row")
    val t = op.schema.fields.headOption.map(_.ptype).getOrElse(PType.Null)
    val v =
      if b.size == 0 || b.numColumns == 0 then
        null
      else
        b.columns(0).get(0)
    Const(v, t)

  /** IN (subquery) in a non-filter position: an uncorrelated value set */
  def bindInSubquery(binder: Binder, a: Expression, sub: Relation, negate: Boolean): VExpr =
    val op     = Planner.nested(this).plan(sub)
    val b      = Operator.materialize(op)
    val target = binder.bind(a)
    val t      = PType.common(target.ptype, op.schema(0).ptype)
    val col    = Kernels.castTo(b.columns(0), t)
    val values = (0 until b.size).map(col.get)
    InList(castExpr(target, t), values.filter(_ != null).toSet, values.contains(null), negate)

  /** (a, b) [NOT] IN (subquery) over an uncorrelated subquery, with SQL NULL semantics */
  def bindTupleInSubquery(
      binder: Binder,
      tuple: List[Expression],
      sub: Relation,
      negate: Boolean
  ): VExpr =
    val op = Planner.nested(this).planRelation(sub)
    if op.schema.size != tuple.size then
      throw StatusCode
        .INVALID_ARGUMENT
        .newException(s"Tuple size mismatch: ${tuple.size} vs ${op.schema.size} subquery columns")
    val b       = Operator.materialize(op)
    val targets = tuple.map(binder.bind)
    val types   = targets.indices.map(i => PType.common(targets(i).ptype, op.schema(i).ptype))
    val cols    = types.indices.map(i => Kernels.castTo(b.columns(i), types(i)))
    val rows    = (0 until b.size).map(r => cols.map(_.get(r)))
    val exact   = rows.filter(_.forall(_ != null)).toSet
    RowFn(
      if negate then
        "tuple_not_in"
      else
        "tuple_in"
      ,
      targets.indices.map(i => castExpr(targets(i), types(i))),
      PType.Bool,
      a =>
        val t       = a.toIndexedSeq
        val in: Any =
          if t.forall(_ != null) && exact.contains(t) then
            true
          else
            // NULL when some row could match except for NULLs
            val unknown = rows.exists(r =>
              r.indices.forall(i => r(i) == null || t(i) == null || r(i) == t(i))
            )
            if unknown then
              null
            else
              false
        in match
          case b: Boolean if negate =>
            !b
          case other =>
            other
      ,
      nullSafe = true
    )

  end bindTupleInSubquery

  def bindExists(binder: Binder, e: Exists): VExpr =
    val op = Planner.nested(this).plan(subqueryRelation(e.child))
    Const(op.next() != null, PType.Bool)

  def windowRef(binder: Binder, e: Expression): VExpr =
    windowCollector match
      case Some(c) =>
        c.ref(binder, e)
      case None =>
        throw StatusCode.NOT_IMPLEMENTED.newException("Window functions are not supported here")

  // --- joins ----------------------------------------------------------------------------

  private[engine] def isInnerJoin(j: Join): Boolean =
    !j.asof && (
      j.joinType match
        case JoinType.InnerJoin | JoinType.ImplicitJoin | JoinType.CrossJoin =>
          j.cond match
            case _: JoinOnTheSameColumns | _: NaturalJoin =>
              false
            case _ =>
              true
        case _ =>
          false
    )

  /**
    * Leaves and conditions of a tree of inner joins, with the ON conditions split by `split`
    * (factored by [[conjuncts]] by default)
    */
  private[engine] def flattenJoins(
      r: Relation,
      split: Expression => List[Expression] = conjuncts
  ): (List[Relation], List[Expression]) =
    r match
      // A lateral unnest needs its left input: plan it as one leaf
      case j: Join if isInnerJoin(j) && unnestCall(j.right).isEmpty =>
        val (l, lc)  = flattenJoins(j.left, split)
        val (rr, rc) = flattenJoins(j.right, split)
        val cond     =
          j.cond match
            case JoinOn(e, _) =>
              split(e)
            case JoinOnEq(keys, _) =>
              split(Expression.concatWithEq(keys))
            case _ =>
              Nil
        (l ++ rr, lc ++ rc ++ cond)
      case other =>
        (List(other), Nil)

  /**
    * A lateral unnest on the right of `j` (join ... unnest(left.column)): its call and alias,
    * whether it keeps left rows without elements (LEFT JOIN ... ON true), and its ON conjuncts
    */
  private[engine] def lateralUnnest(j: Join): Option[Planner.LateralUnnest] = unnestCall(j.right)
    .map { (call, alias) =>
      val conds =
        (
          j.cond match
            case JoinOn(e, _) =>
              conjuncts(e)
            case JoinOnEq(keys, _) =>
              conjuncts(Expression.concatWithEq(keys))
            case _ =>
              Nil
        ).filterNot(isTrueLiteral)
      val outer =
        j.joinType match
          case JoinType.LeftOuterJoin =>
            if conds.nonEmpty then
              throw StatusCode
                .NOT_IMPLEMENTED
                .newException("LEFT JOIN unnest(...) supports only ON true in the engine")
            true
          case JoinType.InnerJoin | JoinType.ImplicitJoin | JoinType.CrossJoin =>
            false
          case other =>
            throw StatusCode
              .NOT_IMPLEMENTED
              .newException(s"${other} with unnest(...) is not supported by the engine")
      Planner.LateralUnnest(call, alias, outer, conds)
    }

  private[engine] def isTrueLiteral(e: Expression): Boolean = e.isInstanceOf[TrueLiteral]

  /**
    * Replace uncorrelated scalar subqueries with NULL, for deciding which inputs a condition reads
    * without evaluating the subqueries
    */
  private[engine] def withoutScalarSubqueries(e: Expression): Expression = e.transformExpression {
    case s: SubQueryExpression if !isCorrelated(s.query, Schema.empty) =>
      NullLiteral(s.span)
  }

  /**
    * Whether an expression has a correlated scalar subquery or EXISTS outside of other subqueries,
    * which [[rewriteSubqueries]] joins to its input
    */
  private[engine] def hasCorrelatedSubquery(e: Expression): Boolean =
    var found = false
    // The same top-down walk as rewriteSubqueries
    def visit(x: Expression): Expression =
      x match
        case s: SubQueryExpression =>
          found ||= isCorrelated(s.query, Schema.empty)
          x
        case ex: Exists =>
          found ||= isCorrelated(subqueryRelation(ex.child), Schema.empty)
          x
        case _: InRelation | _: NotInRelation =>
          x
        case other =>
          other.mapChildExpressions(visit)
    visit(e)
    found

  private[engine] def hasSubquery(e: Expression): Boolean =
    var found = false
    e.traverseExpressions {
      case _: SubQueryExpression | _: Exists | _: InRelation | _: NotInRelation =>
        found = true
    }
    found

  /** Whether a condition over the inputs of a join tree (by schema) references input i */
  private[engine] def referencesInput(c: Expression, inputs: List[Schema], i: Int): Boolean =
    // A condition references input i when it does not bind without it
    val others = inputs.indices.filter(_ != i).map(inputs(_)).foldLeft(Schema.empty)(_ ++ _)
    !bindsIn(c, others) && bindsIn(c, inputs(i) ++ others)

  private def isEquiBetween(c: Expression, left: Schema, right: Schema): Boolean =
    c match
      case eq: Eq =>
        (
          bindsIn(eq.left, left) && !bindsIn(eq.left, right) && bindsIn(eq.right, right) &&
            !bindsIn(eq.right, left)
        ) || (
          bindsIn(eq.right, left) && !bindsIn(eq.right, right) && bindsIn(eq.left, right) &&
            !bindsIn(eq.left, left)
        )
      case _ =>
        false

  private def equiSides(c: Expression, left: Schema, right: Schema): (Expression, Expression) =
    c match
      case eq: Eq if bindsIn(eq.left, left) && bindsIn(eq.right, right) =>
        (eq.left, eq.right)
      case eq: Eq =>
        (eq.right, eq.left)
      case other =>
        throw IllegalStateException(s"Not an equality: ${other}")

  /** The operator join kind of a join that is not part of an inner join tree */
  private[engine] def joinKind(j: Join): JoinKind =
    j.joinType match
      case JoinType.LeftOuterJoin =>
        JoinKind.Left
      case JoinType.RightOuterJoin =>
        JoinKind.Right
      case JoinType.FullOuterJoin =>
        JoinKind.Full
      case JoinType.CrossJoin =>
        JoinKind.Cross
      case _ =>
        JoinKind.Inner

  /** Whether a join is planned by [[joinOn]]: not ASOF, USING, or NATURAL */
  private[engine] def isJoinOn(j: Join): Boolean =
    !j.asof && (
      j.cond match
        case _: JoinOnTheSameColumns | _: NaturalJoin =>
          false
        case _ =>
          true
    )

  /**
    * The key names of a USING or NATURAL join of inputs with these schemas: the listed columns, or
    * the left columns the right input also has. None for other joins
    */
  private[engine] def usingKeys(j: Join, left: Schema, right: Schema): Option[List[String]] =
    j.cond match
      case u: JoinOnTheSameColumns =>
        Some(u.columns.map(_.leafName))
      case _: NaturalJoin =>
        Some(left.names.filter(n => right.names.exists(_.equalsIgnoreCase(n))).toList)
      case _ =>
        None

  /**
    * A join of planned inputs on its ON condition (or none): equalities between the inputs become
    * hash join keys, and the other conditions a residual
    */
  private[engine] def joinOn[P](
      j: Join,
      kind: JoinKind,
      left: P,
      right: P,
      builder: JoinTreeBuilder[P]
  ): P =
    val ls       = builder.op(left).schema
    val rs       = builder.op(right).schema
    val combined = ls ++ rs
    j.cond match
      case NoJoinCriteria =>
        builder.join(
          left,
          right,
          if kind == JoinKind.Inner then
            JoinKind.Cross
          else
            kind
          ,
          IndexedSeq.empty,
          IndexedSeq.empty,
          None,
          buildLeft = false
        )
      case other =>
        val condExpr =
          other match
            case JoinOn(e, _) =>
              e
            case JoinOnEq(keys, _) =>
              Expression.concatWithEq(keys)
            case x =>
              throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported join condition: ${x}")
        val conds       = conjuncts(condExpr)
        val (eqs, rest) = conds.partition(c => isEquiBetween(c, ls, rs))
        val residual    = Option
          .when(rest.nonEmpty)(rest.reduce((a, b) => And(a, b, NoSpan)))
          .map(r => Binder(this, BindScope(combined)).bind(r))
        if eqs.isEmpty then
          builder.join(left, right, kind, IndexedSeq.empty, IndexedSeq.empty, residual, false)
        else
          val pairs    = eqs.map(c => equiSides(c, ls, rs))
          val (lk, rk) = joinKeys(Binder(this, BindScope(ls)), Binder(this, BindScope(rs)), pairs)
          builder.join(
            left,
            right,
            kind,
            lk,
            rk,
            residual,
            buildLeft = swapsOuterBuild(kind, builder, left, right)
          )
    end match

  end joinOn

  /**
    * Whether an outer hash join builds on its left input, as the right (build) input is estimated
    * to be much larger: by `builder.buildRows`, or by the probe source's rows when the right input
    * aggregates ([[wvlet.lang.engine.runtime.Stats.outerBuildRows]])
    */
  private def swapsOuterBuild[P](
      kind: JoinKind,
      builder: JoinTreeBuilder[P],
      left: P,
      right: P
  ): Boolean =
    def rows(p: P): Double =
      if CardinalityEstimator.aggregates(builder.op(right)) then
        CardinalityEstimator.rows(builder.op(p))
      else
        builder.buildRows(p)
    kind match
      case JoinKind.Left | JoinKind.Right | JoinKind.Full =>
        swapBuild(rows(right), rows(left))
      case _ =>
        false

  /**
    * JOIN ... USING (cols) of planned inputs: a hash join on the keys (building on the smaller
    * input for outer joins), then one output column per key (the right key for RIGHT joins,
    * coalesced for FULL joins), followed by the other left and right columns
    */
  private[engine] def usingJoin[P](
      leftPlan: P,
      rightPlan: P,
      names: List[String],
      kind: JoinKind,
      builder: JoinTreeBuilder[P] & AggPlanBuilder[P]
  ): P =
    val left  = builder.op(leftPlan)
    val right = builder.op(rightPlan)
    val lb    = Binder(this, BindScope(left.schema))
    val rb    = Binder(this, BindScope(right.schema))
    val pairs = names.map(n =>
      (UnquotedIdentifier(n, NoSpan): Expression, UnquotedIdentifier(n, NoSpan): Expression)
    )
    val (lk, rk) = joinKeys(lb, rb, pairs)
    val join     = builder.join(
      leftPlan,
      rightPlan,
      kind,
      lk,
      rk,
      None,
      buildLeft = swapsOuterBuild(kind, builder, leftPlan, rightPlan)
    )
    val lw     = left.schema.size
    val keyIdx = names.map { n =>
      (
        left.schema.names.indexWhere(_.equalsIgnoreCase(n)),
        right.schema.names.indexWhere(_.equalsIgnoreCase(n))
      )
    }
    val keyExprs = keyIdx.map { (li, ri) =>
      val lf = left.schema(li)
      val rf = right.schema(ri)
      val t  = PType.common(lf.ptype, rf.ptype)
      kind match
        case JoinKind.Right =>
          castExpr(ColRef(lw + ri, rf.ptype, rf.name), t)
        case JoinKind.Full =>
          Coalesce(Seq(ColRef(li, lf.ptype, lf.name), ColRef(lw + ri, rf.ptype, rf.name)), t)
        case _ =>
          castExpr(ColRef(li, lf.ptype, lf.name), t)
    }
    val keyFields = keyIdx
      .zip(keyExprs)
      .map { case ((li, ri), e) =>
        Field(
          left.schema(li).name,
          e.ptype,
          left.schema(li).qualifiers ++ right.schema(ri).qualifiers
        )
      }
    val leftRest  = left.schema.fields.indices.filterNot(i => keyIdx.exists(_._1 == i))
    val rightRest = right.schema.fields.indices.filterNot(i => keyIdx.exists(_._2 == i))
    val exprs     =
      keyExprs ++ leftRest.map(i => ColRef(i, left.schema(i).ptype, left.schema(i).name)) ++
        rightRest.map(i => ColRef(lw + i, right.schema(i).ptype, right.schema(i).name))
    val fields = keyFields ++ leftRest.map(left.schema(_)) ++ rightRest.map(right.schema(_))
    builder.project(join, exprs.toIndexedSeq, Schema(fields.toIndexedSeq))

  end usingJoin

  /**
    * The keys and the inequality of an ASOF join of inputs with these schemas, bound over each
    * input
    */
  private[engine] def asOfJoinSpec(
      j: Join,
      kind: JoinKind,
      left: Schema,
      right: Schema
  ): Planner.AsOfJoinSpec =
    val condExpr =
      j.cond match
        case JoinOn(e, _) =>
          e
        case JoinOnEq(keys, _) =>
          Expression.concatWithEq(keys)
        case other =>
          throw StatusCode
            .NOT_IMPLEMENTED
            .newException(s"Unsupported ASOF join condition: ${other}")
    val conds       = conjuncts(condExpr)
    val (eqs, rest) = conds.partition(c => isEquiBetween(c, left, right))
    def sides(l: Expression, r: Expression): Option[Boolean] =
      if bindsIn(l, left) && !bindsIn(l, right) && bindsIn(r, right) then
        Some(true)
      else if bindsIn(r, left) && !bindsIn(r, right) && bindsIn(l, right) then
        Some(false)
      else
        None
    // (left expression, op as `left op right`, right expression)
    val inequality: (Expression, AsOfOp, Expression) =
      rest match
        case List(c) =>
          val (l, r, op) =
            c match
              case x: GreaterThanOrEq =>
                (x.left, x.right, AsOfOp.Ge)
              case x: GreaterThan =>
                (x.left, x.right, AsOfOp.Gt)
              case x: LessThanOrEq =>
                (x.left, x.right, AsOfOp.Le)
              case x: LessThan =>
                (x.left, x.right, AsOfOp.Lt)
              case other =>
                throw StatusCode
                  .NOT_IMPLEMENTED
                  .newException(s"Unsupported ASOF join inequality: ${other}")
          def flip(o: AsOfOp): AsOfOp =
            o match
              case AsOfOp.Ge =>
                AsOfOp.Le
              case AsOfOp.Gt =>
                AsOfOp.Lt
              case AsOfOp.Le =>
                AsOfOp.Ge
              case AsOfOp.Lt =>
                AsOfOp.Gt
          sides(l, r) match
            case Some(true) =>
              (l, op, r)
            case Some(false) =>
              (r, flip(op), l)
            case None =>
              throw StatusCode
                .NOT_IMPLEMENTED
                .newException(s"ASOF join inequality must compare both inputs: ${c}")
        case _ =>
          throw StatusCode
            .NOT_IMPLEMENTED
            .newException("ASOF joins need exactly one inequality condition")
    val lb       = Binder(this, BindScope(left))
    val rb       = Binder(this, BindScope(right))
    val (lk, rk) = joinKeys(lb, rb, eqs.map(c => equiSides(c, left, right)))
    val (lv, rv) = joinKeys(lb, rb, Seq((inequality._1, inequality._3)))
    val asofKind =
      kind match
        case JoinKind.Left =>
          JoinKind.Left
        case JoinKind.Inner | JoinKind.Cross =>
          JoinKind.Inner
        case other =>
          throw StatusCode
            .NOT_IMPLEMENTED
            .newException(s"${other} ASOF joins are not supported by the engine")
    Planner.AsOfJoinSpec(lk, rk, lv.head, rv.head, inequality._2, asofKind)

  end asOfJoinSpec

end Planner

/** Builds join trees for the planner, with the planner's filter conditions (and subqueries) */
private[engine] trait JoinPlanBuilder[P] extends JoinTreeBuilder[P]:
  /** Apply filter conjuncts: subquery conjuncts as joins, then the rest as a filter */
  def conjuncts(p: P, conds: List[Expression]): P

/** Builds aggregations for the planner: runtime nodes with their operators */
private[engine] trait AggPlanBuilder[P]:
  /** The operator form of a plan */
  def op(p: P): Operator

  /**
    * Readers of one evaluation of `in` reduced to the columns at `cols`, computed on first read
    * (the input of grouping sets)
    */
  def sharedInput(in: P, cols: IndexedSeq[Int], label: String): () => P

  /** The rows of every input, whose columns are `schema` */
  def union(inputs: IndexedSeq[P], schema: Schema): P

  /** Rows computed at plan time, whose columns are `schema` */
  def values(schema: Schema, rows: Batch): P

  /** The rows of `p`, computed at plan time */
  def compute(p: P): Batch

  /** Group `in` by `keys`, outputting the keys then the aggregates (`schema`) */
  def aggregate(in: P, keys: IndexedSeq[VExpr], calls: IndexedSeq[AggCall], schema: Schema): P

  /** Rows of `p` for which a condition over its columns is true */
  def filter(p: P, cond: VExpr): P

  /** One output column per expression over `p` */
  def project(p: P, exprs: IndexedSeq[VExpr], schema: Schema): P

  /** The columns of `p`, then one column per window call over them (`schema`) */
  def window(p: P, calls: IndexedSeq[WindowCall], schema: Schema): P

end AggPlanBuilder

/** A filter conjunct over a subquery: [NOT] EXISTS (sub), or target [NOT] IN (sub) */
private[engine] enum SubqueryConjunct:
  case Exists(sub: Relation, negate: Boolean)
  case In(target: Expression, sub: Relation, negate: Boolean)

  def sub: Relation

object Planner:

  /**
    * An aggregation input `rest` filtered by `target in (sub)`, whose subquery aggregates the rows
    * of `rest` (`share_in_subquery_groups`)
    */
  private[engine] case class InShare(rest: Relation, target: Expression, sub: Relation)

  /**
    * The most rows of a shared CTE result computed for planning that its reads take distinct counts
    * and filtered rows from: a pass over 6.6M rows (TPC-DS q95's `ws_wh` at SF1) costs about 15 ms,
    * which a larger result is estimated without
    */
  val MaxResultStatsRows: Long = 1L << 20

  /** Make the filters and hash joins of a physical plan gather their output columns eagerly */
  private[engine] def disableLateGather(op: Operator): Unit =
    op match
      case f: FilterOp =>
        f.lateGather = false
      case j: HashJoinOp =>
        j.lateGather = false
      case s: ScanOp =>
        s.lateGather = false
      case _ =>
    op.children.foreach(disableLateGather)
    op.sharedInputs.foreach(disableLateGather)

  /**
    * Keep every per-row runtime filter of a physical plan's scans in the engine
    * (`scan_join_row_filters`)
    */
  private[engine] def disableSourceJoinFilters(op: Operator): Unit =
    op match
      case s: ScanOp =>
        s.sourceJoinFilters = false
      case _ =>
    op.children.foreach(disableSourceJoinFilters)
    op.sharedInputs.foreach(disableSourceJoinFilters)

  /** Keep the scans' own row filter conjuncts ahead of the per-row join filters */
  private[engine] def disableJoinFiltersFirst(op: Operator): Unit =
    op match
      case s: ScanOp =>
        s.joinFiltersFirst = false
      case _ =>
    op.children.foreach(disableJoinFiltersFirst)
    op.sharedInputs.foreach(disableJoinFiltersFirst)

  /** Estimate per-row join filters' pass rates over the width of the probe key's range */
  private[engine] def disableSparseKeyPassRates(op: Operator): Unit =
    op match
      case s: ScanOp =>
        s.sparseKeyPassRates = false
      case _ =>
    op.children.foreach(disableSparseKeyPassRates)
    op.sharedInputs.foreach(disableSparseKeyPassRates)

  /** Set how the filters of a physical plan evaluate AND / OR (`short_circuit_filters`) */
  private[engine] def configureFilters(
      op: Operator,
      shortCircuit: Boolean,
      reorder: Boolean,
      dictionary: Boolean,
      dictionarySelection: Boolean = true
  ): Unit =
    op match
      case f: FilterOp =>
        f.shortCircuit = shortCircuit
        f.reorderConjuncts = reorder
        f.dictionaryFilters = dictionary
        f.dictionarySelection = dictionarySelection
      case _ =>
    op.children.foreach(configureFilters(_, shortCircuit, reorder, dictionary, dictionarySelection))
    op.sharedInputs
      .foreach(configureFilters(_, shortCircuit, reorder, dictionary, dictionarySelection))

  /** Make the hash joins of a physical plan use hash tables for every key (`join_direct_index`) */
  private[engine] def disableJoinDirectIndex(op: Operator): Unit =
    op match
      case j: HashJoinOp =>
        j.withDirectIndex(false)
      case _ =>
    op.children.foreach(disableJoinDirectIndex)
    op.sharedInputs.foreach(disableJoinDirectIndex)

  /**
    * Set how the aggregations and DISTINCTs of a physical plan key their groups
    * (`stream_pack_agg_keys`, `long_decimal_keys`). The partial aggregates that two-phase
    * aggregations create per part get the options from [[Parallelizer]]
    */
  private[engine] def setAggKeyOptions(op: Operator, pack: Boolean, longDecimals: Boolean): Unit =
    op match
      case a: HashAggOp =>
        a.withKeyOptions(pack, longDecimals)
      case d: DistinctOp =>
        d.withKeyOptions(pack, longDecimals)
      case a: PartitionedAggOp =>
        a.withLongDecimalKeys(longDecimals)
      case _ =>
    op.children.foreach(setAggKeyOptions(_, pack, longDecimals))
    op.sharedInputs.foreach(setAggKeyOptions(_, pack, longDecimals))

  /**
    * Make the hash aggregations of a physical plan, and those of its partitioned aggregations,
    * update each aggregate on its own (`fused_accumulators`). The partial aggregates that two-phase
    * aggregations create per part get the option from [[Parallelizer]]
    */
  private[engine] def disableFusedAccumulators(op: Operator): Unit =
    op match
      case a: HashAggOp =>
        a.withFusedAccumulators(false)
      case a: PartitionedAggOp =>
        a.withFusedAccumulators(false)
      case _ =>
    op.children.foreach(disableFusedAccumulators)
    op.sharedInputs.foreach(disableFusedAccumulators)

  /**
    * Make the fused folds of hash aggregations update every accumulator per row
    * (`column_major_fused_sums`). The partial aggregates that two-phase aggregations create per
    * part take the option from the hash aggregations they split
    */
  private[engine] def disableColumnMajorSums(op: Operator): Unit =
    op match
      case a: HashAggOp =>
        a.withColumnMajorSums(false)
      case a: PartitionedAggOp =>
        a.withColumnMajorSums(false)
      case _ =>
    op.children.foreach(disableColumnMajorSums)
    op.sharedInputs.foreach(disableColumnMajorSums)

  /**
    * Make the hash aggregations of a physical plan gather deferred input columns before reading
    * them (`aggregate_selected_rows`). The partial aggregates that two-phase aggregations create
    * per part get the option from [[Parallelizer]]
    */
  private[engine] def disableSelectedInputs(op: Operator): Unit =
    op match
      case a: HashAggOp =>
        a.withSelectedInputs(false)
      case _ =>
    op.children.foreach(disableSelectedInputs)
    op.sharedInputs.foreach(disableSelectedInputs)

  /**
    * Make the projections and hash aggregations of a plan gather the deferred columns of a filter's
    * output before computing over them (`dense_selected_rows`). Their per-part copies in
    * [[Parallelizer]] take the option from these operators
    */
  private[engine] def disableDenseSelected(op: Operator): Unit =
    op match
      case p: ProjectOp =>
        p.withDenseSelected(false)
      case a: HashAggOp =>
        a.withDenseSelected(false)
      case _ =>
    op.children.foreach(disableDenseSelected)
    op.sharedInputs.foreach(disableDenseSelected)

  /**
    * Make the hash aggregations of a physical plan group single Int64 keys through [[GroupTable]]s
    * (`long_group_tables`). The partial aggregates that two-phase aggregations create per part get
    * the option from [[Parallelizer]]
    */
  private[engine] def disableLongGroupTables(op: Operator): Unit =
    op match
      case a: HashAggOp =>
        a.withLongGroupTables(false)
      case a: PartitionedAggOp =>
        a.withLongGroupTables(false)
      case _ =>
    op.children.foreach(disableLongGroupTables)
    op.sharedInputs.foreach(disableLongGroupTables)

  /**
    * Make the hash aggregations of a physical plan hash every key (`sorted_group_runs`). The
    * partial aggregates that two-phase aggregations create per part get the option from
    * [[Parallelizer]]
    */
  private[engine] def disableSortedGroupRuns(op: Operator): Unit =
    op match
      case a: HashAggOp =>
        a.withSortedGroupRuns(false)
      case a: PartitionedAggOp =>
        a.withSortedGroupRuns(false)
      case _ =>
    op.children.foreach(disableSortedGroupRuns)
    op.sharedInputs.foreach(disableSortedGroupRuns)

  /**
    * Make the partitioned aggregations of a physical plan hash-route every row
    * (`sorted_agg_splits`)
    */
  private[engine] def disableSortedAggSplits(op: Operator): Unit =
    op match
      case a: PartitionedAggOp =>
        a.withSortedSplits(false)
      case _ =>
    op.children.foreach(disableSortedAggSplits)
    op.sharedInputs.foreach(disableSortedAggSplits)

  /**
    * Make the partitioned aggregations of a physical plan copy every column of each partition's
    * rows and pack their keys per partition (`packed_agg_routing`)
    */
  private[engine] def disablePackedAggRouting(op: Operator): Unit =
    op match
      case a: PartitionedAggOp =>
        a.withPackedRouting(false)
      case _ =>
    op.children.foreach(disablePackedAggRouting)
    op.sharedInputs.foreach(disablePackedAggRouting)

  /** Make the hash joins of a physical plan build on the calling thread (`parallel_join_build`) */
  private[engine] def disableParallelJoinBuild(op: Operator): Unit =
    op match
      case j: HashJoinOp =>
        j.withParallelBuild(false)
      case _ =>
    op.children.foreach(disableParallelJoinBuild)
    op.sharedInputs.foreach(disableParallelJoinBuild)

  /** Make the hash joins of a physical plan keep their string columns as they are */
  private[engine] def disableJoinBuildStringCodes(op: Operator): Unit =
    op match
      case j: HashJoinOp =>
        j.withStringCodes(false)
      case _ =>
    op.children.foreach(disableJoinBuildStringCodes)
    op.sharedInputs.foreach(disableJoinBuildStringCodes)

  /**
    * The keys and the inequality of an ASOF join, each bound over its input: a left row matches the
    * right rows with equal keys, and takes the closest one with `leftValue op rightValue`. `kind`
    * is Inner or Left
    */
  case class AsOfJoinSpec(
      leftKeys: IndexedSeq[VExpr],
      rightKeys: IndexedSeq[VExpr],
      leftValue: VExpr,
      rightValue: VExpr,
      op: AsOfOp,
      kind: JoinKind
  )

  /**
    * A lateral unnest (join ... unnest(left.column)): the call and its alias, whether rows without
    * elements are kept (LEFT JOIN ... ON true), and the ON conjuncts
    */
  case class LateralUnnest(
      call: TableFunctionCall,
      alias: Option[AliasedRelation],
      outer: Boolean,
      conds: List[Expression]
  )

  /**
    * About `percentage` percent of the rows of `op`: a filter keeping each row with that
    * probability, drawn from a generator seeded with 0, so the same input gives the same sample
    */
  def sampleFilter(op: Operator, percentage: Double): Operator =
    val rnd = scala.util.Random(0)
    FilterOp(
      op,
      RowFn("sample", Nil, PType.Bool, _ => rnd.nextDouble() * 100 < percentage, nullSafe = true)
    )

  /** A planner for a subquery: shares the catalog, sources, and CTE definitions */
  def nested(p: Planner): Planner = p

end Planner

/** Resolves table names and file paths that are not registered in the catalog */
trait SourceResolver:
  def tableSource(name: String, fullName: String): Option[TableSource]
  def fileSource(path: String): Option[TableSource]
  def parseSql(r: RawSQL): Relation

/** The runtime plan of a query as bound, and after the logical passes */
case class RootPlans(bound: RuntimePlan, optimized: RuntimePlan)
