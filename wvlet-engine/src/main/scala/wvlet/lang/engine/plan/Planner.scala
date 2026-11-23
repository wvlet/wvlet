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
import wvlet.lang.engine.runtime.JoinCondition
import wvlet.lang.engine.runtime.Lowering
import wvlet.lang.engine.runtime.Optimizer
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

/**
  * Translates a typed, model-expanded wvlet [[Relation]] into a tree of physical [[Operator]]s.
  * Column names are resolved against the runtime schemas of the operators (not the compiler's
  * types), so queries over files with unknown schemas plan the same way as catalog tables.
  */
class Planner(
    catalog: EngineCatalog,
    sources: SourceResolver,
    options: OptimizerOptions = OptimizerOptions()
)(using ctx: Context)
    extends LogSupport:

  private[engine] def enabled(o: Optimization): Boolean = options.isEnabled(o)

  /** Whether a join builds on the other input, as this one has much more rows than that */
  private[engine] def swapBuild(rows: Double, otherRows: Double): Boolean = JoinOrderOptimizer
    .swapBuild(options, rows, otherRows)

  /** The physical passes over a lowered plan: column pruning (with scan filters), parallelism */
  private def optimizePhysical(op: Operator): Operator =
    val pruned = ColumnPruner(
      op,
      pruneColumns = enabled(Optimization.PruneColumns),
      scanFilters = enabled(Optimization.PushScanFilters)
    )
    if enabled(Optimization.Parallelize) then
      Parallelizer(pruned, TaskRunner.default)
    else
      pruned

  private var hiddenSeq                                  = 0
  private[engine] def hiddenName(prefix: String): String =
    hiddenSeq += 1
    s"__wv_${prefix}${hiddenSeq}"

  private[engine] def isHidden(f: Field): Boolean = f.name.startsWith("__wv_")

  // Named relations of WITH clauses
  private val cteDefs = mutable.HashMap.empty[String, AliasedRelation]
  // CTEs referenced more than once are computed once and scanned from memory afterwards
  private val cteRefCounts = mutable.HashMap.empty[String, Int]
  private val cteTables    = mutable.HashMap.empty[String, (AliasedRelation, SharedResult)]
  // Shared CTEs of the runtime planner by name, and every one created, in order
  private val cteRefs      = mutable.HashMap.empty[String, (AliasedRelation, DeferredCTE)]
  private val deferredCTEs = mutable.ArrayBuffer.empty[DeferredCTE]
  // The shared CTEs whose bodies are being bound, innermost first
  private var bindingCTEs = List.empty[DeferredCTE]
  // Nesting of plan(): subqueries are planned by this planner too
  private var planDepth = 0
  // Uncorrelated scalar subqueries are evaluated once per query
  private val scalarResults = java.util.IdentityHashMap[Relation, VExpr]()
  // Scalar subqueries prefetchScalarSubqueries has considered, so nested plans don't retry them
  private val prefetchTried = java.util.IdentityHashMap[Relation, Unit]()

  /** Window function calls collected while binding a projection */
  private var windowCollector: Option[WindowCollector] = None

  /** Plan a relation, then drop columns no operator needs so that scans read fewer columns */
  def plan(r: Relation): Operator =
    planDepth += 1
    try
      if enabled(Optimization.PrefetchScalarSubqueries) then
        prefetchScalarSubqueries(r)
      val op = optimizePhysical(planRelation(r))
      if planDepth == 1 then
        finalizeCTEs(op)
      op
    finally
      planDepth -= 1

  /**
    * Evaluate uncorrelated scalar subqueries that aggregate the same input (e.g. the count, avg,
    * and avg of each TPC-DS q09 bucket) as one query, so that the input is read once for all of
    * them. Inputs are the same when their bound runtime plans are equal up to column ids
    * ([[ShareSubplans.key]]). The results fill [[scalarResults]]
    */
  private def prefetchScalarSubqueries(r: Relation): Unit =
    // CTE names are resolved by scope while planning, not here: skip inputs that may read one
    val cteNames = mutable.Set.from(cteDefs.keys)
    r.traverse { case w: WithQuery =>
      w.queryDefs.foreach(d => cteNames += d.alias.leafName.toLowerCase)
    }
    def readsCTE(rel: Relation): Boolean =
      var found = false
      rel.traverse {
        case t: TableRef if cteNames.contains(t.name.leafName.toLowerCase) =>
          found = true
        case t: TableScan if cteNames.contains(t.name.name.toLowerCase) =>
          found = true
      }
      found
    // Binding an input evaluates its subqueries, which the discarded key binding would run once
    // more (and alone, before they are grouped): such inputs have no key
    def hasSubqueries(rel: Relation): Boolean =
      var found = false
      rel.traverseExpressions {
        case _: SubQueryExpression | _: Exists | _: InRelation | _: NotInRelation =>
          found = true
      }
      found
    // The keys' bindings are discarded: restore the hidden-name counter so that names don't shift
    def inputKey(p: Project): Option[Any] =
      val seq = hiddenSeq
      try
        Option
          .unless(hasSubqueries(p.child))(runtimeBinder.bind(p.child))
          .flatMap(ShareSubplans.key)
      catch
        case _: Exception =>
          // Correlated inputs fail on their outer names; they are evaluated on their own
          None
      finally
        hiddenSeq = seq
    val subs = mutable.ArrayBuffer.empty[(Any, Project)]
    r.traverseExpressions { case s: SubQueryExpression =>
      s.query match
        case p: Project
            if p.selectItems.size == 1 && Binder.containsAggregate(p.selectItems.head) &&
              !scalarResults.containsKey(p) && !prefetchTried.containsKey(p) =>
          prefetchTried.put(p, ())
          if !readsCTE(p.child) then
            inputKey(p).foreach(k => subs += k -> p)
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

  /** Binds relations to the engine's runtime plan (with the runtime planner) */
  private lazy val runtimeBinder = RuntimeBinder(this)

  /** The logical passes over runtime plans */
  private lazy val runtimeOptimizer = Optimizer.default(options)

  /**
    * Plan a relation into operators: through the runtime plan (`wvlet.engine.planner=runtime`),
    * whose relations it doesn't cover yet are planned by [[planLegacy]], or directly by
    * [[planLegacy]]
    */
  private[engine] def planRelation(r: Relation): Operator =
    options.planner match
      case PlannerMode.Runtime =>
        Lowering(runtimePlan(r))
      case PlannerMode.Legacy =>
        planLegacy(r)

  /**
    * The optimized runtime plan of a relation, whose relations the runtime binder doesn't cover are
    * planned
    */
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
    * The shared CTE a table name refers to, with the runtime planner: a CTE read more than once,
    * whose body is bound on the first read
    */
  private[engine] def sharedCTE(name: String): Option[SharedCTE] =
    val key = name.toLowerCase
    cteDefs.get(key) match
      case Some(d)
          if options.planner == PlannerMode.Runtime && enabled(Optimization.ShareCTEs) &&
            cteRefCounts.getOrElse(key, 0) > 1 =>
        val cte =
          cteRefs.get(key) match
            case Some((defn, c)) if defn eq d =>
              c
            case _ =>
              val c = DeferredCTE(name)
              cteRefs(key) = (d, c)
              deferredCTEs += c
              bindingCTEs = c :: bindingCTEs
              try c.body = runtimePlan(d)
              finally bindingCTEs = bindingCTEs.tail
              c
        bindingCTEs.headOption.foreach(_.reads += cte)
        Some(cte)
      case _ =>
        None

  /** The definition of a CTE in scope, by name */
  private[engine] def cteDef(name: String): Option[AliasedRelation] = cteDefs.get(name.toLowerCase)

  private[engine] def parseSql(r: RawSQL): Relation = sources.parseSql(r)

  /** Plan a relation with the planner's own recursion (children go through [[planRelation]]) */
  private[engine] def planLegacy(r: Relation): Operator =
    r match
      case q: Query =>
        planRelation(q.body)
      case w: WithQuery =>
        registerCTEs(w)
        planRelation(w.queryBody)
      case b: BracedRelation =>
        planRelation(b.child)
      case s: SelectAsAlias =>
        planRelation(s.child)
      case t: TestRelation =>
        planRelation(t.child)
      case d: Debug =>
        planRelation(d.child)
      case d: Describe =>
        val (schema, batch) = describeRows(planRelation(d.child).schema)
        BatchesOp(schema, Seq(batch))
      case r: RelationInspector =>
        planRelation(r.child)
      case p: PartitioningHint =>
        planRelation(p.child)
      case n: NamedRelation =>
        requalify(planRelation(n.child), n.name.leafName)
      case _: EmptyRelation =>
        SingleRowOp()
      case v: Values =>
        val op = planValues(v, None)
        v.relationType match
          // An inlined table value (val t(a, b) = [...]) is referenced by its name
          case s: wvlet.lang.model.DataType.SchemaType if s.isFullyNamed =>
            requalify(op, s.typeName.name)
          case _ =>
            op
      case a: AliasedRelation =>
        planAliased(a)
      case t: TableScan =>
        planTable(t.name.name, t.name.fullName)
      case t: TableRef =>
        planTable(t.name.leafName, t.name.fullName)
      case f: FileScan =>
        scanOp(fileSource(f.filePath), None)
      case f: FileRef =>
        scanOp(fileSource(f.filePath), None)
      case r: RawSQL =>
        planRelation(sources.parseSql(r))
      case f: Filter =>
        planFilter(f)
      case s: Sort =>
        planSort(s, None)
      case l: Limit =>
        l.child match
          case s: Sort =>
            planSort(s, Some(l.limit.value))
          case Offset(s: Sort, rows, _) =>
            LimitOp(planSort(s, Some(l.limit.value + rows.value)), l.limit.value, rows.value)
          case Offset(child, rows, _) =>
            LimitOp(planRelation(child), l.limit.value, rows.value)
          case child =>
            LimitOp(planRelation(child), l.limit.value, 0)
      case o: Offset =>
        o.child match
          case Limit(child, limit, _) =>
            // LIMIT n OFFSET m in SQL order: the offset applies first
            LimitOp(planRelation(child), limit.value, o.rows.value)
          case child =>
            LimitOp(planRelation(child), Long.MaxValue, o.rows.value)
      case d: Distinct =>
        DistinctOp(planRelation(d.child))
      case d: Dedup =>
        DistinctOp(planRelation(d.child))
      case g: GroupBy =>
        planAggregation(g, None, defaultItems = true)
      case Agg(p: Pivot, _, items, _) =>
        planPivot(p, items)
      case p: Pivot =>
        planPivot(p, Nil)
      case u: Unpivot =>
        planUnpivot(u)
      case r if unnestCall(r).isDefined =>
        val (call, alias) = unnestCall(r).get
        planUnnest(None, call, alias)
      case a: Agg =>
        planAggregation(a.child, Some(a.selectItems), defaultItems = false)
      case c: Count if isAggregationInput(c.child) =>
        // Count the groups
        val groups = planAggregation(c.child, None, defaultItems = false)
        val t      = Aggregates.resultType("count_star", Nil)
        val name   = c.selectItems.headOption.map(nameOf).getOrElse("count")
        HashAggOp(
          groups,
          IndexedSeq.empty,
          IndexedSeq(AggCall("count_star", Nil, distinct = false, filter = None, t)),
          Schema(IndexedSeq(Field(name, t)))
        )
      case c: Count =>
        planAggregation(c.child, Some(c.selectItems), defaultItems = false)
      case p: Project =>
        planProject(p.child, p.selectItems)
      case s: SQLSelect =>
        planSQLSelect(s)
      case a: AddColumnsToRelation =>
        planColumnEdit(a.child, prepend = false, a.newColumns)
      case a: PrependColumnsToRelation =>
        planColumnEdit(a.child, prepend = true, a.newColumns)
      case e: ExcludeColumnsFromRelation =>
        val op = planRelation(e.child)
        projectIndexes(op, excludeIndexes(op.schema, e))
      case r: RenameColumnsFromRelation =>
        val op = planRelation(r.child)
        ProjectOp(
          op,
          op.schema.fields.indices.map(i => ColRef(i, op.schema(i).ptype, op.schema(i).name)),
          renameSchema(op.schema, r)
        )
      case s: ShiftColumns =>
        val op = planRelation(s.child)
        projectIndexes(op, shiftIndexes(op.schema, s))
      case j: Join =>
        planJoinTree(j, Nil)
      case u: Union =>
        val op = UnionAllOp(
          Seq(planRelation(u.left), planRelation(u.right)),
          unionSchema(u.left, u.right)
        )
        if u.isDistinct then
          DistinctOp(op)
        else
          op
      case c: Concat =>
        val inputs = Seq(planRelation(c.left), planRelation(c.right))
        UnionAllOp(inputs, mergeSchemas(inputs.map(_.schema)))
      case i: Intersect =>
        val l  = planRelation(i.left)
        val rr = planRelation(i.right)
        SetOpOp(
          l,
          rr,
          isIntersect = true,
          all = !i.isDistinct,
          mergeSchemas(Seq(l.schema, rr.schema))
        )
      case e: Except =>
        val l  = planRelation(e.left)
        val rr = planRelation(e.right)
        SetOpOp(
          l,
          rr,
          isIntersect = false,
          all = !e.isDistinct,
          mergeSchemas(Seq(l.schema, rr.schema))
        )
      case s: Sample =>
        planSample(s)
      case other =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(s"Unsupported relation in the engine: ${other.nodeName}")

  end planLegacy

  private def batchOf(schema: Schema, rows: Seq[Seq[Any]]): Batch =
    val cols = schema
      .fields
      .indices
      .map { c =>
        Vec.fromValues(schema(c).ptype, rows.map(_(c)))
      }
    Batch(cols.toArray, rows.size)

  private def requalify(op: Operator, q: String): Operator =
    val schema = op.schema.withQualifier(q)
    ProjectOp(
      op,
      schema.fields.indices.map(i => ColRef(i, schema(i).ptype, schema(i).name)),
      schema
    )

  private def projectIndexes(op: Operator, idx: Seq[Int]): Operator = ProjectOp(
    op,
    idx.map(i => ColRef(i, op.schema(i).ptype, op.schema(i).name)).toIndexedSeq,
    Schema(idx.map(op.schema(_)).toIndexedSeq)
  )

  /** Drop hidden helper columns */
  private def dropHidden(op: Operator): Operator =
    if op.schema.fields.exists(isHidden) then
      projectIndexes(op, op.schema.fields.indices.filterNot(i => isHidden(op.schema(i))))
    else
      op

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

  private def unionSchema(l: Relation, r: Relation): Schema = mergeSchemas(
    Seq(planRelation(l).schema, planRelation(r).schema)
  )

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

  private def planTable(name: String, fullName: String): Operator =
    val key = name.toLowerCase
    sharedCTE(name) match
      case Some(c) =>
        // With the runtime planner: a reader of the deferred CTE that runtime plans read too. It
        // reports no filter, so the body stays unfiltered
        c.reader(c.body.schema)
      case None =>
        cteDefs.get(key) match
          case Some(d) if enabled(Optimization.ShareCTEs) && cteRefCounts.getOrElse(key, 0) > 1 =>
            val shared =
              cteTables.get(key) match
                case Some((defn, r)) if defn eq d =>
                  r
                case _ =>
                  val r = SharedResult(optimizePhysical(planAliased(d)))
                  cteTables(key) = (d, r)
                  r
            shared.newReader(s"CTE[${name}]")
          case Some(d) =>
            planAliased(d)
          case None =>
            scanOp(tableSource(name, fullName), Some(name))

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

  private def scanOp(src: TableSource, alias: Option[String]): Operator =
    val schema = alias.map(a => src.schema.withQualifier(a)).getOrElse(src.schema)
    ScanOp(src, schema, None, None)

  private def planAliased(a: AliasedRelation): Operator =
    val child =
      a.child match
        case v: Values =>
          planValues(v, a.columnNames)
        case other =>
          planRelation(other)
    val names  = a.columnNames.map(_.map(_.name.name))
    val schema = Schema(
      child
        .schema
        .fields
        .zipWithIndex
        .map { (f, i) =>
          val n = names.flatMap(_.lift(i)).getOrElse(f.name)
          Field(n, f.ptype, List(a.alias.leafName))
        }
    )
    child match
      case s: ScanOp =>
        s.withSchema(schema)
      case _ =>
        ProjectOp(
          child,
          schema.fields.indices.map(i => ColRef(i, schema(i).ptype, schema(i).name)),
          schema
        )

  private def planValues(
      v: Values,
      columnNames: Option[List[wvlet.lang.model.DataType.NamedType]]
  ): Operator =
    val (schema, batch) = valuesBatch(v, columnNames)
    BatchesOp(schema, Seq(batch), "Values")

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
    * PIVOT with DuckDB semantics: one output column per pivot value (combination), each an
    * aggregate filtered to its value. Without USING aggregates, count(*). Without GROUP BY, rows
    * are grouped by the columns used neither for pivoting nor in the aggregates
    */
  private def planPivot(p: Pivot, aggItems: List[Attribute]): Operator =
    val childOp = planRelation(p.child)
    val schema  = childOp.schema
    val batch   = Operator.materialize(childOp)
    def input   = BatchesOp(schema, Seq(batch))
    val scope   = BindScope(schema)
    val binder  = Binder(this, scope)
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
            val vec = col.eval(batch)
            (0 until batch.size)
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
    HashAggOp(input, keys.map(_._2), aggCalls, outSchema)

  end planPivot

  /** UNPIVOT: one row per (input row, target column) with a non-NULL value */
  private def planUnpivot(u: Unpivot): Operator =
    val child   = planRelation(u.child)
    val batches = Operator.materialize(child)
    val schema  = child.schema
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
    val parts = targets.map { t =>
      val in    = BatchesOp(schema, Seq(batches))
      val value = castExpr(ColRef(t, schema(t).ptype, schema(t).name), valueType)
      val proj  = ProjectOp(
        FilterOp(in, IsNullExpr(ColRef(t, schema(t).ptype, schema(t).name), negate = true)),
        others.map(i => ColRef(i, schema(i).ptype, schema(i).name): VExpr) ++
          IndexedSeq(Const(schema(t).name, PType.Str), value),
        outSchema
      )
      proj: Operator
    }
    UnionAllOp(parts, outSchema)

  end planUnpivot

  private def planSample(s: Sample): Operator =
    val op = planRelation(s.child)
    s.size match
      case SamplingSize.Rows(n) =>
        LimitOp(op, n, 0)
      case SamplingSize.Percentage(p) =>
        Planner.sampleFilter(op, p)
      case other =>
        op

  // --- SQL SELECT blocks ----------------------------------------------------------------

  private def planSQLSelect(s: SQLSelect): Operator = planRelation(desugarSQLSelect(s))

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
  private def printSql(e: Expression): String =
    val sql =
      try
        SqlGenerator(
          CodeFormatterConfig(sqlDBType = DBType.DuckDB, fitToLine = true, maxLineWidth = 10000)
        ).print(e)
      catch
        case _: Exception =>
          e.toString
    sql.replaceAll("\\s+", " ")

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

  private def planProject(child: Relation, items: List[Attribute]): Operator =
    if isAggregation(child, items) then
      planAggregation(child, Some(items), defaultItems = false)
    else if items.exists(i => unnestArg(i).isDefined) then
      projectWithUnnest(planRelation(child), items)
    else
      val op = planRelation(child)
      projectWithWindows(op, items)

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

  /** unnest(list, ...) as a relation; over `left` (a lateral join) when given */
  private def planUnnest(
      left: Option[Operator],
      call: TableFunctionCall,
      alias: Option[AliasedRelation],
      outer: Boolean = false
  ): Operator =
    val input           = left.getOrElse(SingleRowOp())
    val (lists, fields) = unnestLists(input.schema, call, alias)
    val inputFields     =
      if left.isDefined then
        input.schema.fields
      else
        IndexedSeq.empty
    UnnestOp(input, lists, keepInput = left.isDefined, Schema(inputFields ++ fields), outer)

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

  /** select ..., unnest(list), ...: the row repeats per list element */
  private def projectWithUnnest(op: Operator, items: List[Attribute]): Operator =
    val (exprs, fields, listIdx) = bindUnnestItems(op.schema, items)
    val proj                     = ProjectOp(op, exprs, Schema(fields))
    val elemFields               = listIdx.map(unnestElementField(exprs, fields, _))
    val unnest                   = UnnestOp(
      proj,
      listIdx.map(i => ColRef(i, fields(i).ptype, fields(i).name): VExpr),
      keepInput = true,
      Schema(fields ++ elemFields)
    )
    val out = fields
      .indices
      .map { i =>
        listIdx.indexOf(i) match
          case -1 =>
            (ColRef(i, fields(i).ptype, fields(i).name): VExpr, fields(i))
          case k =>
            (ColRef(fields.size + k, elemFields(k).ptype, elemFields(k).name), elemFields(k))
      }
    ProjectOp(unnest, out.map(_._1), Schema(out.map(_._2)))

  end projectWithUnnest

  /** Bind a projection, evaluating window functions first when there are any */
  private def projectWithWindows(op: Operator, items: List[Attribute]): Operator =
    val (in, (exprs, fields)) = withWindows(op, items)((_, b) => bindItems(b, items))
    ProjectOp(in, exprs, Schema(fields))

  /**
    * Bind expressions over `op`. When they contain window functions, a window operator is placed
    * over `op` (input columns first, then the window results), and binding runs against it
    *
    * @return
    *   the operator to evaluate the bound result over, and the result of `bindAll`
    */
  private def withWindows[A](op: Operator, items: List[Expression])(
      bindAll: (Operator, Binder) => A
  ): (Operator, A) =
    val (calls, a) = bindWithWindows(op.schema, items)(bindAll(op, _))
    calls match
      case Some(c) =>
        (c.build(op), a)
      case None =>
        (op, a)

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
  private class SharedResult(val op: Operator):
    lazy val batches: Seq[Batch] =
      debug(s"Materializing a shared result:\n${op.explain()}")
      op.drain()

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
  private class SharedReader(shared: SharedResult, schema: Schema, label: String)
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
        Some(ScanSplits(bs.toIndexedSeq.map(b => () => Iterator.single(sel(b))), () => ()))

    override def explainDetail(indent: Int): Option[String] = shared.sharedPlan(indent)

  end SharedTable

  /**
    * A result read by several runtime-plan readers (`CTERef`s), such as the input of grouping sets:
    * `op`, the operator of `body`, is physically optimized and computed on first read
    */
  private class SharedInput(val name: String, val body: RuntimePlan, op: Operator)
      extends SharedCTE:
    private val shared = SharedResult(optimizePhysical(op))

    override def reader(schema: Schema, filter: Option[JoinCondition]): Operator = shared.newReader(
      name
    )

  /** A shared result of `op` (lowered from `body`) for runtime-plan readers */
  private[engine] def sharedInput(name: String, body: RuntimePlan, op: Operator): SharedCTE =
    SharedInput(name, body, op)

  /**
    * A CTE read more than once, with the runtime planner. Its body is bound when it is first read,
    * and lowered once every reader is planned ([[finalizeCTEs]]), keeping only the columns the
    * readers read, and only the rows they keep when every reader reports its filter. A read before
    * then (a subquery evaluated at plan time) computes every column and row
    */
  private class DeferredCTE(val name: String) extends SharedCTE:
    // Set right after binding: the body can't read its own CTE
    var body: RuntimePlan = null
    // The CTEs that the body reads, which are finalized after this one
    val reads = mutable.LinkedHashSet.empty[DeferredCTE]

    // Positions of the body output that readers read
    private val needed    = mutable.BitSet.empty
    private var needsAll  = false
    private var finalized = Option.empty[(IndexedSeq[Int], SharedResult)]
    // The filter each reader applies right above it, in creation order (None: none or unknown)
    private val readerFilters = mutable.ArrayBuffer.empty[Option[JoinCondition]]
    // Whether the result keeps only the rows the readers' filters agree on
    private var filtered          = false
    private lazy val label        = s"CTE[${name}]"
    private lazy val uniqueNames  = body.schema.names.distinct.size == body.schema.size
    private lazy val bodyPosition = body.schema.names.zipWithIndex.toMap

    def isFinalized: Boolean = finalized.nonEmpty

    /** Record that a reader reads these body output positions */
    def need(positions: Iterable[Int]): Unit = needed ++= positions

    override def reader(schema: Schema, filter: Option[JoinCondition]): Operator =
      if filtered then
        throw StatusCode
          .UNEXPECTED_STATE
          .newException(s"${label} is read after its result was filtered for its other readers")
      if uniqueNames then
        readerFilters += filter
        ScanOp(CTETable(label, body.schema, this), schema, None, None)
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
        new IteratorOp(schema, () => result._2.batches.iterator, label):
          override protected def extraInputs(indent: Int): Seq[String] =
            result._2.sharedPlan(indent).toSeq
          override def sharedInputs: Seq[Operator] = resultOp.toSeq

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
        val out  = body.output
        val keep =
          if needsAll || needed.isEmpty || !enabled(Optimization.PruneColumns) then
            out.indices
          else
            needed.toIndexedSeq.sorted
        val pruned =
          if keep.size == out.size then
            body
          else
            runtimeOptimizer.pruneColumns(body, keep.map(out(_).id).toSet)
        val op       = Lowering(runtimeOptimizer.pushScanFilters(pruned))
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
        finalized = Some((keep, SharedResult(optimizePhysical(selected))))

    /** The result operator, once finalized */
    def resultOp: Option[Operator] = finalized.map(_._2.op)

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

  /**
    * The result of a deferred CTE, scanned as a table whose parts are its batches. Its schema is
    * the body's; readers rename the columns in their own schema
    */
  private class CTETable(val name: String, val schema: Schema, val cte: DeferredCTE)
      extends TableSource:
    def scan(spec: ScanSpec): Iterator[Batch] =
      val sel = cte.selector(spec.columns)
      cte.batches.iterator.map(sel)

    override def canSplit: Boolean = true

    override def scanSplits(spec: ScanSpec): Option[ScanSplits] =
      val bs = cte.batches.filter(_.size > 0)
      if bs.size <= 1 then
        None
      else
        val sel = cte.selector(spec.columns)
        Some(ScanSplits(bs.toIndexedSeq.map(b => () => Iterator.single(sel(b))), () => ()))

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

  private def planColumnEdit(
      child: Relation,
      prepend: Boolean,
      newColumns: List[Attribute]
  ): Operator =
    val input = planRelation(child)
    // Window functions in new columns are evaluated first; their results follow the input columns
    val (op, (exprs, fields)) =
      withWindows(input, newColumns) { (op, binder) =>
        bindItems(binder, newColumns)
      }
    val all = columnEditItems(input.schema, prepend, exprs, fields)
    ProjectOp(op, all.map(_._1), Schema(all.map(_._2)))

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

  private def planAggregation(
      input: Relation,
      items: Option[List[Attribute]],
      defaultItems: Boolean
  ): Operator = aggregation(input, items, defaultItems, planRelation, operatorPlans)

  /**
    * Plan an aggregation of `input` (a GROUP BY, possibly with HAVING, or a global aggregation)
    * selecting `items`, whose input is planned with `plan`. With grouping sets, one aggregation per
    * set reads a shared evaluation of the input, and a union combines them
    */
  private[engine] def aggregation[P](
      input: Relation,
      items: Option[List[Attribute]],
      defaultItems: Boolean,
      plan: Relation => P,
      builder: AggPlanBuilder[P]
  ): P =
    val (groupBy, having) =
      input match
        case g: GroupBy =>
          (Some(g), None)
        case Filter(g: GroupBy, cond, _) =>
          (Some(g), Some(cond))
        case _ =>
          (None, None)
    val source                  = groupBy.map(_.child).getOrElse(input)
    val in                      = plan(source)
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
    def bindSelectItems(): IndexedSeq[(VExpr, Field)] =
      val boundItems = mutable.ArrayBuffer.empty[(VExpr, Field)]
      selectItems.foreach {
        case a: AllColumns =>
          // `select *` over a grouping: every key
          baseKeyExprs
            .indices
            .foreach(i =>
              boundItems +=
                ((aggCtx.keyRef(i), Field(baseKeyExprs(i)._1, baseKeyExprs(i)._2.ptype)))
            )
        case attr =>
          val e = aggBinder.bind(attr)
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
    val firstPass =
      try bindSelectItems()
      finally windowCollector = saved
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
    val havingExpr = having.map(h => aggBinder.bind(h))
    val aggOp: P   =
      val keys  = keyExprs.map(_._2)
      val calls = aggCtx.calls.toIndexedSeq
      groupingSets match
        case None =>
          builder.aggregate(in, keys, calls, aggCtx.outputSchema)
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
          val m    = Array.fill(inOp.schema.size)(-1)
          cols.indices.foreach(i => m(cols(i)) = i)
          val reader      = builder.sharedInput(in, cols, "GroupingSetInput")
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
        val items2 = firstPass ++ extraItems
        builder.project(filtered, items2.map(_._1), Schema(items2.map(_._2)))

  end aggregation

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

  private[engine] def planSort(s: Sort, limit: Option[Long]): Operator =
    val op      = planRelation(s.child)
    val orderBy = resolveSortItems(s)
    val outKeys = orderBy.map { k =>
      try
        Right(sortKeys(Binder(this, BindScope(op.schema)), List(k), op.schema).head)
      catch
        case e @ (_: ColumnNotFound | _: WvletLangException) =>
          Left(e)
    }
    if outKeys.forall(_.isRight) then
      SortOp(op, outKeys.map(_.toOption.get), limit)
    else
      s.child match
        case p: Project =>
          // ORDER BY an expression outside the select list (an input column, or an aggregate
          // over a grouping): evaluate it as a hidden select item
          val hidden = orderBy
            .zip(outKeys)
            .collect { case (k, Left(_)) =>
              k -> hiddenName("sort")
            }
          val proj = planRelation(
            p.copy(selectItems =
              p.selectItems ++
                hidden.map((k, name) => SingleColumn(NameExpr.fromString(name), k.sortKey, NoSpan))
            )
          )
          val hiddenRefs =
            hidden
              .map { (k, name) =>
                val i = proj.schema.fields.indexWhere(_.name == name)
                sortKey(k, ColRef(i, proj.schema(i).ptype, name))
              }
              .iterator
          val keys = outKeys.map(_.getOrElse(hiddenRefs.next()))
          dropHidden(SortOp(proj, keys, limit))
        case _ =>
          throw outKeys
            .collectFirst { case Left(e) =>
              e
            }
            .get
    end if

  end planSort

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

  private def exprKey(e: Expression): String = exprName(e)

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

  private def planFilter(f: Filter): Operator =
    f.child match
      case g: GroupBy =>
        // HAVING without a selection
        planAggregation(f, None, defaultItems = true)
      case j: Join if isInnerJoin(j) =>
        planJoinTree(j, conjuncts(f.filterExpr))
      case j: Join if j.joinType == JoinType.LeftOuterJoin && !j.asof =>
        // Conditions on the preserved (left) side filter before the join, so that an inner join
        // tree under left joins can use them for its join order and pushdown
        val conds          = conjuncts(f.filterExpr)
        val leftSchema     = planRelation(j.left).schema
        val (pushed, rest) = conds.partition(c => !hasSubquery(c) && bindsIn(c, leftSchema))
        if pushed.isEmpty then
          applyConjuncts(planRelation(j), conds)
        else
          val left = Filter(j.left, pushed.reduce((a, b) => And(a, b, NoSpan)), f.span)
          applyConjuncts(planRelation(j.copy(left = left)), rest)
      case child =>
        applyConjuncts(planRelation(child), conjuncts(f.filterExpr))

  /** Apply filter conjuncts, turning subqueries into joins where possible */
  private[engine] def applyConjuncts(input: Operator, conds: List[Expression]): Operator =
    val originalWidth = input.schema.size
    var op            = input
    val plain         = mutable.ArrayBuffer.empty[Expression]
    conds.foreach { c =>
      if sys.env.contains("ENGINE_DEBUG") then
        warn(s"conjunct: ${c.getClass.getSimpleName}: ${c}")
      rewriteSubqueryConjunct(op, c) match
        case Some((newOp, residual)) =>
          op = newOp
          residual.foreach(plain += _)
        case None =>
          plain += c
    }
    if plain.nonEmpty then
      // Subquery joins may extend the input, so bind every condition after all joins are added
      val bound = mutable.ArrayBuffer.empty[(Expression, VExpr)]
      plain.foreach { c =>
        val (newOp, e) = bindWithSubqueries(op, c)
        if newOp ne op then
          // Earlier conditions were bound to the narrower input, whose columns keep their positions
          op = newOp
        bound += ((c, e))
      }
      val cond = bound.map(_._2).reduce((a, b) => AndExpr(a, b))
      op = FilterOp(op, cond)
    if op.schema.size != originalWidth then
      projectIndexes(op, 0 until originalWidth)
    else
      op

  end applyConjuncts

  /**
    * Bind one conjunct whose correlated scalar subqueries become left joins; returns the (possibly
    * extended) input and the bound condition
    */
  private def bindWithSubqueries(input: Operator, cond: Expression): (Operator, VExpr) =
    bindWithSubqueries(operatorPlans, input, cond, planRelation, planFilter)

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
    val rewritten = rewrite(cond)
    (p, Binder(this, BindScope(b.op(p).schema)).bind(rewritten))

  end bindWithSubqueries

  /** Whether a subquery references columns of the enclosing schema */
  private def isCorrelated(sub: Relation, outer: Schema): Boolean =
    try
      Planner.nested(this).planRelation(sub)
      false
    catch
      case e: ColumnNotFound =>
        true

  /**
    * EXISTS / NOT EXISTS / IN / NOT IN conjuncts as semi and anti joins. Returns the new input and
    * any condition left to evaluate
    */
  private def rewriteSubqueryConjunct(
      op: Operator,
      c: Expression
  ): Option[(Operator, Option[Expression])] = subqueryConjunct(c).flatMap {
    case SubqueryConjunct.Exists(sub, negate) =>
      existsJoin(op, sub, negate)
    case SubqueryConjunct.In(a, sub, negate) =>
      inJoin(op, a, sub, negate)
  }

  /**
    * A filter conjunct that is EXISTS, NOT EXISTS, IN, or NOT IN over a subquery (correlated or
    * not), which [[rewriteSubqueryConjunct]] turns into a join
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

  private def existsJoin(
      op: Operator,
      sub: Relation,
      negate: Boolean
  ): Option[(Operator, Option[Expression])] =
    if !isCorrelated(sub, op.schema) then
      // Uncorrelated: a constant condition
      val rows = Operator.materialize(Planner.nested(this).plan(sub)).size
      val keep = (rows > 0) != negate
      Some(
        (
          if keep then
            op
          else
            FilterOp(op, Const(false, PType.Bool))
          ,
          None
        )
      )
    else
      val conj = SubqueryConjunct.Exists(sub, negate)
      Some((correlatedJoin(operatorPlans, op, conj, planRelation, planFilter), None))

  end existsJoin

  /**
    * A semi / anti hash join of `op` with a subquery's rows. When the subquery side is estimated to
    * be much larger, its rows become the probe side instead (RIGHT SEMI / RIGHT ANTI): the hash
    * table holds `op`'s rows, the large input is probed in parallel parts marking the matched rows,
    * and `op`'s matched (or unmatched) rows are emitted once at the end
    */
  private def semiJoin(
      op: Operator,
      inner: Operator,
      lk: IndexedSeq[VExpr],
      rk: IndexedSeq[VExpr],
      kind: JoinKind,
      residual: Option[VExpr]
  ): Operator = JoinOrderOptimizer.semiJoinOp(
    op,
    inner,
    lk,
    rk,
    kind,
    residual,
    buildLeft = swapBuild(CardinalityEstimator.rows(inner), CardinalityEstimator.rows(op))
  )

  private def inJoin(
      op: Operator,
      a: Expression,
      sub: Relation,
      negate: Boolean
  ): Option[(Operator, Option[Expression])] =
    if isCorrelated(sub, op.schema) then
      val conj = SubqueryConjunct.In(a, sub, negate)
      return Some((correlatedJoin(operatorPlans, op, conj, planRelation, planFilter), None))
    val inner    = Planner.nested(this).plan(sub)
    val lb       = Binder(this, BindScope(op.schema))
    val target   = lb.bind(a)
    val valueRef = ColRef(0, inner.schema(0).ptype, inner.schema(0).name)
    val t        = PType.common(target.ptype, valueRef.ptype)
    val lk       = IndexedSeq(castExpr(target, t))
    val rk       = IndexedSeq(castExpr(valueRef, t))
    if !negate then
      Some((semiJoin(op, inner, lk, rk, JoinKind.Semi, None), None))
    else
      // NOT IN: no row passes when the subquery has a NULL; NULL targets never pass
      val values  = Operator.materialize(inner)
      val v       = values.columns(0)
      val hasNull = (0 until values.size).exists(v.isNull)
      if hasNull then
        Some((FilterOp(op, Const(false, PType.Bool)), None))
      else
        val src  = BatchesOp(inner.schema, Seq(values))
        val anti = HashJoinOp(op, src, lk, rk, JoinKind.Anti, None, op.schema)
        Some((FilterOp(anti, IsNullExpr(target, negate = true)), None))

  end inJoin

  /**
    * A correlated [NOT] EXISTS / [NOT] IN filter conjunct over `p` as a semi or anti join with the
    * decorrelated subquery body, which `plan` and `filtered` plan as in [[planCorrelated]]: the
    * planner's operators, or runtime-plan nodes. The correlation equalities are hash keys (a nested
    * loop join without any), the other correlated conditions a residual over p ++ body, with inner
    * names resolving first. An IN also joins on the target and the select item.
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
      // Build on the body unless it is much larger than p
      val buildLeft =
        lk.nonEmpty &&
          swapBuild(CardinalityEstimator.rows(b.op(body)), CardinalityEstimator.rows(b.op(p)))
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

  /**
    * Plan inner joins and filter conditions together: single-input conditions are pushed to their
    * inputs, equalities between inputs become hash join keys, and inputs are joined greedily,
    * building on the smaller side
    */
  private[engine] def planJoinTree(j: Join, filters: List[Expression]): Operator =
    lateralUnnest(j) match
      case Some(Planner.LateralUnnest(call, alias, outer, conds)) =>
        // A lateral unnest over the left input: join ... unnest(left.column)
        val op = planUnnest(Some(planRelation(j.left)), call, alias, outer)
        return applyConjuncts(op, conds ++ filters)
      case None =>
    end match
    if !isInnerJoin(j) then
      return applyConjuncts(planOuterJoin(j), filters)
    innerJoinTree(j, filters)

  end planJoinTree

  /** Builds the planner's own join trees: operators */
  private object operatorPlans
      extends JoinPlanBuilder[Operator]
      with AggPlanBuilder[Operator]
      with OperatorTreeBuilder:
    override def sharedInput(in: Operator, cols: IndexedSeq[Int], label: String): () => Operator =
      val shared = SharedResult(optimizePhysical(projectIndexes(in, cols)))
      () => shared.newReader(label)

    override def union(inputs: IndexedSeq[Operator], schema: Schema): Operator = UnionAllOp(
      inputs,
      schema
    )

    override def aggregate(
        in: Operator,
        keys: IndexedSeq[VExpr],
        calls: IndexedSeq[AggCall],
        schema: Schema
    ): Operator = HashAggOp(in, keys, calls, schema)

    override def project(p: Operator, exprs: IndexedSeq[VExpr], schema: Schema): Operator =
      ProjectOp(p, exprs, schema)

    override def window(p: Operator, calls: IndexedSeq[WindowCall], schema: Schema): Operator =
      WindowOp(p, calls, schema)

    override def conjuncts(p: Operator, conds: List[Expression]): Operator = applyConjuncts(
      p,
      conds
    )

  end operatorPlans

  /**
    * Plan a tree of inner joins (without a lateral unnest on top) and filter conditions together:
    * single-input conditions are pushed to their inputs, equalities between inputs become hash join
    * keys, and [[JoinOrderOptimizer]] orders the joins. The runtime planner orders them in its
    * `ReorderJoins` pass instead
    */
  private def innerJoinTree(j: Join, filters: List[Expression]): Operator =
    val builder             = operatorPlans
    val (leaves, joinConds) = flattenJoins(j)
    val inputs              = leaves.map(planRelation)
    val all                 = (joinConds ++ filters).filterNot(isTrueLiteral)
    // Conditions with subqueries are applied after joining, except for uncorrelated scalar
    // subqueries, which are constants (e.g., d_month_seq = (select ... from date_dim))
    val (withSubqueries, conds) = all
      .map(c => c -> withoutScalarSubqueries(c))
      .partition((_, e) => hasSubquery(e))
    val subqueryConds = withSubqueries.map(_._1)
    val graph         = JoinGraph.build(
      inputs,
      builder.op,
      conds,
      // Which inputs a condition references
      refs = e => inputs.indices.filter(i => referencesInput(e, inputs.map(_.schema), i)).toSet,
      push = builder.conjuncts,
      selectivity = selectivity
    )
    var (current, remaining) = JoinOrderOptimizer(options).plan(
      graph,
      JoinOrderOptimizer.expressionSearch(builder, joinBinding)
    )
    if remaining.nonEmpty then
      current = builder.conjuncts(current, remaining)
    if subqueryConds.nonEmpty then
      current = builder.conjuncts(current, subqueryConds)
    current

  end innerJoinTree

  /** Binds join conditions for [[JoinOrderOptimizer]] */
  private val joinBinding: JoinBinding =
    new JoinBinding:
      override def joinKeys(
          left: Schema,
          right: Schema,
          pairs: Seq[(Expression, Expression)]
      ): (IndexedSeq[VExpr], IndexedSeq[VExpr]) = Planner.this.joinKeys(
        Binder(Planner.this, BindScope(left)),
        Binder(Planner.this, BindScope(right)),
        pairs
      )

      override def bind(schema: Schema, e: Expression): VExpr = Binder(
        Planner.this,
        BindScope(schema)
      ).bind(e)

  private[engine] def isTrueLiteral(e: Expression): Boolean = e.isInstanceOf[TrueLiteral]

  /**
    * Replace uncorrelated scalar subqueries with NULL, for deciding which inputs a condition reads
    * without evaluating the subqueries
    */
  private[engine] def withoutScalarSubqueries(e: Expression): Expression = e.transformExpression {
    case s: SubQueryExpression if !isCorrelated(s.query, Schema.empty) =>
      NullLiteral(s.span)
  }

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

  /** Estimated fraction of the rows of op passing a condition */
  private def selectivity(op: Operator, c: Expression): Double = CardinalityEstimator.selectivity(
    op,
    c,
    e => Binder(this, BindScope(op.schema)).bind(e)
  )

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

  private def planOuterJoin(j: Join): Operator =
    val left  = planRelation(j.left)
    val right = planRelation(j.right)
    val kind  = joinKind(j)
    if j.asof then
      return planAsOfJoin(j, left, right, kind)
    j.cond match
      case u: JoinOnTheSameColumns =>
        val names = u.columns.map(_.leafName)
        usingJoin(left, right, names, kind)
      case _: NaturalJoin =>
        val names = left.schema.names.filter(n => right.schema.names.exists(_.equalsIgnoreCase(n)))
        usingJoin(left, right, names.toList, kind)
      case _ =>
        joinOn(j, kind, left, right, operatorPlans)

  end planOuterJoin

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
            buildLeft = swapsOuterBuild(kind, builder.op(left), builder.op(right))
          )
    end match

  end joinOn

  /**
    * An outer hash join, building on the smaller input: when the right (build) input is estimated
    * to be much larger than the left one, the inputs swap sides (LEFT becomes RIGHT and back) and a
    * projection restores the left-then-right column order. The larger input then becomes the probe
    * side, which runs in parallel parts, and the hash table holds the smaller one
    */
  private def outerHashJoin(
      left: Operator,
      right: Operator,
      lk: IndexedSeq[VExpr],
      rk: IndexedSeq[VExpr],
      kind: JoinKind,
      residual: Option[VExpr],
      combined: Schema
  ): Operator = JoinOrderOptimizer.joinOp(
    left,
    right,
    kind,
    lk,
    rk,
    residual,
    swapsOuterBuild(kind, left, right),
    combined
  )

  /**
    * Whether an outer hash join builds on its left input, as the right (build) input is estimated
    * to be much larger
    */
  private def swapsOuterBuild(kind: JoinKind, left: Operator, right: Operator): Boolean =
    kind match
      case JoinKind.Left | JoinKind.Right | JoinKind.Full =>
        swapBuild(CardinalityEstimator.rows(right), CardinalityEstimator.rows(left))
      case _ =>
        false

  /** JOIN ... USING (cols): one output column per key (coalesced for outer joins) */
  private def usingJoin(
      left: Operator,
      right: Operator,
      names: List[String],
      kind: JoinKind
  ): Operator =
    val lb    = Binder(this, BindScope(left.schema))
    val rb    = Binder(this, BindScope(right.schema))
    val pairs = names.map(n =>
      (UnquotedIdentifier(n, NoSpan): Expression, UnquotedIdentifier(n, NoSpan): Expression)
    )
    val (lk, rk) = joinKeys(lb, rb, pairs)
    val combined = left.schema ++ right.schema
    val join     = outerHashJoin(left, right, lk, rk, kind, None, combined)
    val lw       = left.schema.size
    val keyIdx   = names.map { n =>
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
    ProjectOp(join, exprs.toIndexedSeq, Schema(fields.toIndexedSeq))

  end usingJoin

  /**
    * ASOF join: equalities between the inputs become keys, and the single remaining comparison
    * between the inputs selects the closest right row
    */
  private def planAsOfJoin(j: Join, left: Operator, right: Operator, kind: JoinKind): Operator =
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
    val (eqs, rest) = conds.partition(c => isEquiBetween(c, left.schema, right.schema))
    def sides(l: Expression, r: Expression): Option[Boolean] =
      if bindsIn(l, left.schema) && !bindsIn(l, right.schema) && bindsIn(r, right.schema) then
        Some(true)
      else if bindsIn(r, left.schema) && !bindsIn(r, right.schema) && bindsIn(l, right.schema) then
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
    val lb       = Binder(this, BindScope(left.schema))
    val rb       = Binder(this, BindScope(right.schema))
    val (lk, rk) = joinKeys(lb, rb, eqs.map(c => equiSides(c, left.schema, right.schema)))
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
    AsOfJoinOp(
      left,
      right,
      lk,
      rk,
      lv.head,
      rv.head,
      inequality._2,
      asofKind,
      left.schema ++ right.schema
    )

  end planAsOfJoin

end Planner

/** Builds join trees for the planner, with the planner's filter conditions (and subqueries) */
private[engine] trait JoinPlanBuilder[P] extends JoinTreeBuilder[P]:
  /** Apply filter conjuncts as `Planner.applyConjuncts` does */
  def conjuncts(p: P, conds: List[Expression]): P

/** Builds aggregations for the planner: operators, or runtime nodes with their operators */
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

  /** Group `in` by `keys`, outputting the keys then the aggregates (`schema`) */
  def aggregate(in: P, keys: IndexedSeq[VExpr], calls: IndexedSeq[AggCall], schema: Schema): P

  /** Rows of `p` for which a condition over its columns is true */
  def filter(p: P, cond: VExpr): P

  /** One output column per expression over `p` */
  def project(p: P, exprs: IndexedSeq[VExpr], schema: Schema): P

  /** The columns of `p`, then one column per window call over them (`schema`) */
  def window(p: P, calls: IndexedSeq[WindowCall], schema: Schema): P

/** A filter conjunct over a subquery: [NOT] EXISTS (sub), or target [NOT] IN (sub) */
private[engine] enum SubqueryConjunct:
  case Exists(sub: Relation, negate: Boolean)
  case In(target: Expression, sub: Relation, negate: Boolean)

  def sub: Relation

object Planner:

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

/** Resolves table names and file paths that are not registered in the catalog */
trait SourceResolver:
  def tableSource(name: String, fullName: String): Option[TableSource]
  def fileSource(path: String): Option[TableSource]
  def parseSql(r: RawSQL): Relation
