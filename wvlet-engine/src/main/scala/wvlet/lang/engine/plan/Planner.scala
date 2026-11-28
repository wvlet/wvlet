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
import wvlet.lang.engine.runtime.CTEJoinKeys
import wvlet.lang.engine.runtime.BExpr
import wvlet.lang.engine.runtime.Optimizer
import wvlet.lang.engine.runtime.PlanAccelerator
import wvlet.lang.engine.runtime.PlanValidator
import wvlet.lang.engine.runtime.RuntimePlan
import wvlet.lang.engine.runtime.ShareSubplans
import wvlet.lang.engine.runtime.SharedCTE
import wvlet.lang.engine.vector.*
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.uni.log.LogSupport

import scala.collection.mutable

/**
  * Translates a typed, model-expanded wvlet [[Relation]] into a tree of physical [[Operator]]s.
  * Column names are resolved against the runtime schemas of the operators (not the compiler's
  * types), so queries over files with unknown schemas plan the same way as catalog tables.
  */
class Planner(
    catalog: EngineCatalog,
    sources: SourceResolver,
    private[engine] val options: OptimizerOptions = OptimizerOptions(),
    private[plan] val accelerator: Option[PlanAccelerator] = None
)(using ctx: Context)
    extends LogSupport
    with PhysicalSteps
    with SharedResults
    with PlannerAggregations
    with PlannerJoins
    with PlannerSortKeys
    with PlannerSubqueries:

  private[engine] def enabled(o: Optimization): Boolean = options.isEnabled(o)

  /**
    * The memory budget of the query this planner plans, current while it builds operators so that
    * they capture it, also when plans are built later on another thread
    */
  private[engine] val memory: MemoryContext = MemoryContext.current

  private[plan] var hiddenSeq                            = 0
  private[engine] def hiddenName(prefix: String): String =
    hiddenSeq += 1
    s"__wv_${prefix}${hiddenSeq}"

  private[engine] def isHidden(f: Field): Boolean = f.name.startsWith("__wv_")

  // Named relations of WITH clauses
  private val cteDefs = mutable.HashMap.empty[String, AliasedRelation]
  // How many times each CTE is referenced: one read more than once is shared
  private val cteRefCounts = mutable.HashMap.empty[String, Int]
  // Shared CTEs by name, and every one created, in order
  private val cteRefs            = mutable.HashMap.empty[String, (AliasedRelation, DeferredCTE)]
  private[plan] val deferredCTEs = mutable.ArrayBuffer.empty[DeferredCTE]
  // The shared CTEs in the order their bodies were finalized (readers' bodies first)
  private[plan] val finalizeOrder = mutable.ArrayBuffer.empty[DeferredCTE]
  // The shared CTEs whose bodies are being bound, innermost first
  private var bindingCTEs = List.empty[DeferredCTE]
  // Nesting of plan(): subqueries are planned by this planner too
  private var planDepth = 0
  // Uncorrelated scalar subqueries are evaluated once per query
  private[plan] val scalarResults = java.util.IdentityHashMap[Relation, VExpr]()
  // Scalar subqueries prefetchScalarSubqueries has considered, so nested plans don't retry them
  private val prefetchTried = java.util.IdentityHashMap[Relation, Unit]()
  // Groups computed at plan time that aggregations of an IN subquery over the same input merge
  // (`share_in_subquery_groups`)
  private[plan] val sharedGroups = mutable.ArrayBuffer.empty[SharedGroups]
  // Aggregation inputs whose IN filter couldn't share groups, planned with the filter instead
  private[plan] val inShareDeclined = java.util.IdentityHashMap[Relation, Unit]()
  // Groups computed for an IN filter and the subquery's values, by the input's loose key, keys,
  // calls, and IN target: an aggregation planned again (e.g. for a hidden sort key) reuses them
  private[plan] val inGroupsComputed = mutable.HashMap.empty[Any, (Batch, Batch, PType)]

  /** Window function calls collected while binding a projection */
  private[plan] var windowCollector: Option[WindowCollector] = None

  /**
    * The runtime plans of the outermost relation of the last [[plan]] call, before and after the
    * logical passes, for tools that show them (`WvletEngine.planStages`)
    */
  private[engine] var rootPlans: Option[RootPlans] = None

  /** Plan a relation, then drop columns no operator needs so that scans read fewer columns */
  def plan(rel: Relation): Operator = MemoryContext.withCurrent(memory)(planInScope(rel))

  private def planInScope(rel: Relation): Operator =
    planDepth += 1
    try
      val deduped =
        if planDepth == 1 && enabled(Optimization.ShareDuplicateSubqueries) &&
          enabled(Optimization.ShareCTEs)
        then
          DuplicateSubqueries.rewrite(this, rel)
        else
          rel
      val r =
        if planDepth == 1 && enabled(Optimization.ShareSubqueryScans) &&
          enabled(Optimization.ShareCTEs)
        then
          SharedScans.rewrite(this, deduped)
        else
          deduped
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
      val bound     = cteJoinKeys(runtimeBinder.bind(r))
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
  private[plan] def readsKeptColumns(root: Operator): Boolean =
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
  private[engine] def readsCTE(rel: Relation, cteNames: Set[String]): Boolean =
    var found = false
    rel.traverse {
      case f: Filter if sharedScanOf(f).isDefined =>
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
  private[engine] def inputKey(rel: Relation): Option[Any] = boundInput(rel).flatMap(
    ShareSubplans.key
  )

  /**
    * The loose key of the bound runtime plan of `rel` ([[ShareSubplans.looseKey]]), with the
    * canonical id of each column of `schema`, the schema `rel` plans to: equal for inputs that read
    * the same rows, whatever the order of their inner-join inputs or their table aliases. None when
    * a column doesn't name exactly one column of the bound plan
    */
  private[plan] def looseInputKey(rel: Relation, schema: Schema): Option[(Any, IndexedSeq[Int])] =
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
  private[plan] def boundInput(rel: Relation): Option[RuntimePlan] =
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
  private[plan] lazy val runtimeBinder = RuntimeBinder(this)

  /** The logical passes over runtime plans */
  private[plan] lazy val runtimeOptimizer = Optimizer.default(
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
                c.visibleColumns = c.body.output.size
              finally
                bindingCTEs = bindingCTEs.tail
              c
        bindingCTEs.headOption.foreach(_.reads += cte)
        Some(cte)
      case _ =>
        None

  /**
    * The bound root plan with its equalities between readers of a shared CTE on a string key
    * rewritten to the long key that determines it (`cte_determining_join_keys`, [[CTEJoinKeys]])
    */
  private def cteJoinKeys(bound: RuntimePlan): RuntimePlan =
    if enabled(Optimization.CTEDeterminingJoinKeys) && deferredCTEs.nonEmpty then
      CTEJoinKeys.rewrite(
        bound,
        (cte, position) =>
          cte match
            case d: DeferredCTE =>
              d.extendKey(position)
            case _ =>
              None
        ,
        () => runtimeBinder.newColumnId()
      )
    else
      bound

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
  // reads instead: its other conjuncts over the shared CTE. With `match_copied_subquery_scans`,
  // also the filters with their templates expanded: expanding a condition copies the subqueries
  // in it, so a plan-time subquery is bound as such a copy
  private val sharedScans = PlanMemo[Filter, Relation]()
  // Reads of the shared scans' CTEs, which their tree doesn't show
  private val sharedScanReads = mutable.HashMap.empty[String, Int]

  /** Read the filter `f` as `r`, a filter over a shared scan ([[SharedScans]]) */
  private[engine] def readSharedScan(f: Filter, r: Relation): Unit =
    sharedScans.put(f, r)
    if enabled(Optimization.MatchCopiedSubqueryScans) then
      sharedScans.putCopy(Binder.expandTemplatesIn(f), r)

  /** Count `n` reads of the CTE `name` of a shared scan */
  private[engine] def addCTEReads(name: String, n: Int): Unit =
    sharedScanReads(name.toLowerCase) = n

  /** The relation the filter `f` reads instead, when it reads a shared scan */
  private[engine] def sharedScanOf(f: Filter): Option[Relation] = sharedScans.get(f)

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
  private[engine] def tableSource(name: String, fullName: String): TableSource = withFolderFiles(
    catalog
      .lookup(name, fullName)
      .orElse(sources.tableSource(name, fullName))
      .getOrElse {
        throw StatusCode.TABLE_NOT_FOUND.newException(s"Table not found: ${fullName}")
      }
  )

  /** The source of a file path */
  private[engine] def fileSource(path: String): TableSource = withFolderFiles(
    sources
      .fileSource(path)
      .getOrElse {
        throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported file format: ${path}")
      }
  )

  /**
    * `s`, reading a Parquet folder through uni's dataset reader unless `folder_file_scans` is on,
    * an Iceberg table or schema-evolved folder unless `dataset_file_scans` is on, and giving a
    * folder of partitions no row filter unless `partition_row_filters` is on, and an Iceberg table
    * of plain files by file unless `iceberg_row_groups` is on. Sources are shared by the engines of
    * a catalog, so the toggles apply here, per planner
    */
  private def withFolderFiles(s: TableSource): TableSource =
    s match
      case p: ParquetSource =>
        val folders =
          if enabled(Optimization.FolderFileScans) then
            p
          else
            p.withoutFolderFiles
        val datasets =
          if enabled(Optimization.DatasetFileScans) then
            folders
          else
            folders.withoutDatasetFiles
        val partitions =
          if enabled(Optimization.PartitionRowFilters) then
            datasets
          else
            datasets.withoutPartitionRowFilters
        if enabled(Optimization.IcebergRowGroups) then
          partitions
        else
          partitions.withoutIcebergRowGroups
      case other =>
        other

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
  private[plan] def printSql(e: Expression): String = sqlText(e).replaceAll("\\s+", " ")

  /** SQL text of an expression, with the whitespace inside its string literals kept */
  private[plan] def sqlText(e: Expression): String =
    try
      SqlGenerator(
        CodeFormatterConfig(sqlDBType = DBType.DuckDB, fitToLine = true, maxLineWidth = 10000)
      ).print(e)
    catch
      case _: Exception =>
        e.toString

  private[plan] def isStar(e: Expression): Boolean =
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
  private[plan] def expandGroupingSets(
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

  def windowRef(binder: Binder, e: Expression): VExpr =
    windowCollector match
      case Some(c) =>
        c.ref(binder, e)
      case None =>
        throw StatusCode.NOT_IMPLEMENTED.newException("Window functions are not supported here")

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

object Planner extends PhysicalWalkers:

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
