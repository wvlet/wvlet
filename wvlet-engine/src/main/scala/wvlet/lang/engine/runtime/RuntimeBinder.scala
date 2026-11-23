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

import wvlet.lang.api.Span.NoSpan
import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.engine.exec.AggCall
import wvlet.lang.engine.exec.Aggregates
import wvlet.lang.engine.exec.FilterOp
import wvlet.lang.engine.exec.HashAggOp
import wvlet.lang.engine.exec.ProjectOp
import wvlet.lang.engine.exec.JoinKind
import wvlet.lang.engine.exec.Operator
import wvlet.lang.engine.exec.UnionAllOp
import wvlet.lang.engine.exec.WindowCall
import wvlet.lang.engine.exec.WindowOp
import wvlet.lang.engine.expr.AndExpr
import wvlet.lang.engine.expr.Const
import wvlet.lang.engine.expr.IsNullExpr
import wvlet.lang.engine.expr.ColRef
import wvlet.lang.engine.expr.OrExpr
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.BindScope
import wvlet.lang.engine.plan.CardinalityEstimator
import wvlet.lang.engine.plan.AggPlanBuilder
import wvlet.lang.engine.plan.Binder
import wvlet.lang.engine.plan.JoinOrderOptimizer
import wvlet.lang.engine.plan.JoinPlanBuilder
import wvlet.lang.engine.plan.ColumnNotFound
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.Planner
import wvlet.lang.engine.plan.SubqueryConjunct
import wvlet.lang.engine.vector.Batch
import wvlet.lang.engine.vector.PType
import wvlet.lang.engine.vector.Field
import wvlet.lang.engine.vector.Schema
import wvlet.lang.model.DataType
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*

/**
  * Binds a typed, model-expanded [[Relation]] to a [[RuntimePlan]]: names resolve to column ids
  * once, here. It covers every relation the engine plans, and calls the [[Planner]] for the parts
  * shared through its plan builders (aggregation, joins, decorrelation, pivot, unpivot).
  *
  * Inputs bind in a fixed order, since binding changes the planner's state (shared CTEs, hidden
  * column names, plan-time subquery results). A relation that fails to bind is an error.
  */
class RuntimeBinder(planner: Planner):
  private var lastId = 0

  private def newColumn(f: Field): Column =
    lastId += 1
    Column(ColumnId(lastId), f.name, f.ptype, f.qualifiers)

  private def columnsOf(s: Schema): IndexedSeq[Column] = s.fields.map(newColumn)

  def bind(r: Relation): RuntimePlan =
    // The case order decides which path a relation takes (e.g., Count over an aggregation input)
    r match
      case q: Query =>
        bind(q.body)
      case w: WithQuery =>
        planner.registerCTEs(w)
        bind(w.queryBody)
      case b: BracedRelation =>
        bind(b.child)
      case s: SelectAsAlias =>
        bind(s.child)
      case t: TestRelation =>
        bind(t.child)
      case d: Debug =>
        bind(d.child)
      case d: Describe =>
        // The child binds for its columns only, as the planner plans it
        val (schema, batch) = planner.describeRows(bind(d.child).schema)
        RuntimePlan.Values(columnsOf(schema), batch)
      case r: RelationInspector =>
        bind(r.child)
      case p: PartitioningHint =>
        bind(p.child)
      case n: NamedRelation =>
        requalify(bind(n.child), n.name.leafName)
      case e: EmptyRelation =>
        // One row without columns
        RuntimePlan.Values(IndexedSeq.empty, Batch.ofRows(1))
      case v: Values =>
        val p = values(v, None)
        v.relationType match
          // An inlined table value (val t(a, b) = [...]) is referenced by its name
          case s: DataType.SchemaType if s.isFullyNamed =>
            requalify(p, s.typeName.name)
          case _ =>
            p
      case a: AliasedRelation =>
        aliased(a)
      case t: TableScan =>
        table(t.name.name, t.name.fullName)
      case t: TableRef =>
        table(t.name.leafName, t.name.fullName)
      case f: FileScan =>
        scan(planner.fileSource(f.filePath), None)
      case f: FileRef =>
        scan(planner.fileSource(f.filePath), None)
      case r: RawSQL =>
        bind(planner.parseSql(r))
      case f: Filter =>
        filter(f)
      case s: Sort =>
        sort(s, None)
      case l: Limit =>
        val n = l.limit.value
        l.child match
          case s: Sort =>
            sort(s, Some(n))
          case Offset(s: Sort, rows, _) =>
            RuntimePlan.Limit(sort(s, Some(n + rows.value)), n, rows.value)
          case Offset(child, rows, _) =>
            RuntimePlan.Limit(bind(child), n, rows.value)
          case child =>
            RuntimePlan.Limit(bind(child), n, 0)
      case o: Offset =>
        o.child match
          case Limit(child, limit, _) =>
            // LIMIT n OFFSET m in SQL order: the offset applies first
            RuntimePlan.Limit(bind(child), limit.value, o.rows.value)
          case child =>
            RuntimePlan.Limit(bind(child), Long.MaxValue, o.rows.value)
      case d: Distinct =>
        RuntimePlan.Distinct(bind(d.child))
      case d: Dedup =>
        RuntimePlan.Distinct(bind(d.child))
      case g: GroupBy =>
        aggregation(g, None, defaultItems = true)
      case Agg(p: Pivot, _, items, _) =>
        planner.pivot(p, items, r => lowered(bind(r)), plans).plan
      case p: Pivot =>
        planner.pivot(p, Nil, r => lowered(bind(r)), plans).plan
      case u: Unpivot =>
        planner.unpivot(u, r => lowered(bind(r)), plans).plan
      case r if planner.unnestCall(r).isDefined =>
        val (call, alias) = planner.unnestCall(r).get
        // Over one row without columns, as the planner plans it
        unnest(RuntimePlan.Values(IndexedSeq.empty, Batch.ofRows(1)), call, alias, outer = false)
      case a: Agg =>
        aggregation(a.child, Some(a.selectItems), defaultItems = false)
      case c: Count if planner.isAggregationInput(c.child) =>
        countGroups(c)
      case c: Count =>
        aggregation(c.child, Some(c.selectItems), defaultItems = false)
      case p: Project if isPlainProjection(p) =>
        project(p)
      case p: Project if planner.isAggregation(p.child, p.selectItems) =>
        aggregation(p.child, Some(p.selectItems), defaultItems = false)
      case p: Project if p.selectItems.exists(i => planner.unnestArg(i).isDefined) =>
        // Unnest before windows: windows next to unnest fail in `bindUnnestItems`
        unnestProject(p)
      case p: Project =>
        windowProject(p)
      case s: SQLSelect =>
        bind(planner.desugarSQLSelect(s))
      // New columns with aggregates or unnest fail in the shared Binder
      case a: AddColumnsToRelation =>
        columnEdit(a.child, prepend = false, a.newColumns)
      case a: PrependColumnsToRelation =>
        columnEdit(a.child, prepend = true, a.newColumns)
      case e: ExcludeColumnsFromRelation =>
        val in = bind(e.child)
        projectIndexes(in, planner.excludeIndexes(in.schema, e))
      case r: RenameColumnsFromRelation =>
        val in = bind(r.child)
        renamed(in, planner.renameSchema(in.schema, r).fields.map(newColumn))
      case s: ShiftColumns =>
        val in = bind(s.child)
        projectIndexes(in, planner.shiftIndexes(in.schema, s))
      case j: Join =>
        join(j, Nil)
      case u: Union =>
        val all = union(Seq(u.left, u.right))
        if u.isDistinct then
          RuntimePlan.Distinct(all)
        else
          all
      case c: Concat =>
        union(Seq(c.left, c.right))
      case i: Intersect =>
        setOp(i.left, i.right, intersect = true, all = !i.isDistinct)
      case e: Except =>
        setOp(e.left, e.right, intersect = false, all = !e.isDistinct)
      case s: Sample =>
        sample(s)
      case other =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(s"Unsupported relation in the engine: ${other.nodeName}")

  end bind

  /**
    * A sample as the planner plans it: a row count as a limit, a percentage as a
    * [[RuntimePlan.Sample]], and a percentage given by an expression keeps every row
    */
  private def sample(s: Sample): RuntimePlan =
    val in = bind(s.child)
    s.size match
      case SamplingSize.Rows(n) =>
        RuntimePlan.Limit(in, n, 0)
      case SamplingSize.Percentage(p) =>
        RuntimePlan.Sample(in, p)
      case SamplingSize.PercentageExpr(_) =>
        in

  /** The output columns of a set operation over `inputs`, as the planner builds its schema */
  private def setOpColumns(inputs: Seq[RuntimePlan]): IndexedSeq[Column] = columnsOf(
    planner.mergeSchemas(inputs.map(_.schema))
  )

  private def union(rs: Seq[Relation]): RuntimePlan =
    val inputs = rs.map(bind).toIndexedSeq
    RuntimePlan.Union(inputs, setOpColumns(inputs))

  private def setOp(l: Relation, r: Relation, intersect: Boolean, all: Boolean): RuntimePlan =
    val left  = bind(l)
    val right = bind(r)
    RuntimePlan.SetOp(left, right, intersect, all, setOpColumns(Seq(left, right)))

  private def scan(src: wvlet.lang.engine.catalog.TableSource, alias: Option[String]): RuntimePlan =
    val schema = alias.map(a => src.schema.withQualifier(a)).getOrElse(src.schema)
    RuntimePlan.Scan(src, columnsOf(schema))

  private def table(name: String, fullName: String): RuntimePlan =
    planner.cteDef(name) match
      case Some(d) =>
        planner.sharedCTE(name) match
          case Some(cte) =>
            RuntimePlan.CTERef(cte, columnsOf(cte.body.schema))
          case None =>
            // A CTE read once (or every CTE, with share_ctes off) is inlined: its body binds in
            // place, so that the passes see through it
            aliased(d)
      case None =>
        scan(planner.tableSource(name, fullName), Some(name))

  private def values(v: Values, names: Option[List[DataType.NamedType]]): RuntimePlan =
    val (schema, batch) = planner.valuesBatch(v, names)
    RuntimePlan.Values(columnsOf(schema), batch)

  /** The same columns under new names and qualifiers, as a projection */
  private def renamed(child: RuntimePlan, columns: IndexedSeq[Column]): RuntimePlan = RuntimePlan
    .Project(
      child,
      child
        .output
        .indices
        .map(i => BExpr(ColRef(child.output(i).id.value, columns(i).ptype, columns(i).name))),
      columns
    )

  private def requalify(p: RuntimePlan, q: String): RuntimePlan = renamed(
    p,
    p.output.map(c => newColumn(Field(c.name, c.ptype, List(q))))
  )

  private def aliased(a: AliasedRelation): RuntimePlan =
    val child =
      a.child match
        case v: Values =>
          values(v, a.columnNames)
        case other =>
          bind(other)
    val names  = a.columnNames.map(_.map(_.name.name))
    val fields = child
      .output
      .zipWithIndex
      .map { (c, i) =>
        Field(names.flatMap(_.lift(i)).getOrElse(c.name), c.ptype, List(a.alias.leafName))
      }
    child match
      case s: RuntimePlan.Scan =>
        s.copy(columns = fields.map(newColumn))
      case RuntimePlan.CTERef(cte, _) =>
        RuntimePlan.CTERef(cte, fields.map(newColumn))
      case _ =>
        renamed(child, fields.map(newColumn))

  private def filter(f: Filter): RuntimePlan =
    f.child match
      case _: GroupBy =>
        // HAVING without a selection
        aggregation(f, None, defaultItems = true)
      case j: Join if planner.isInnerJoin(j) =>
        // The join tree takes the conditions for its join order and pushdown. Common conjuncts
        // of `or`s are factored by the Normalize pass
        join(j, planner.splitConjuncts(f.filterExpr))
      case j: Join
          if j.joinType == JoinType.LeftOuterJoin && !j.asof && !(
            isOuterJoinOn(j) && !planner.splitConjuncts(f.filterExpr).exists(planner.hasSubquery)
          ) =>
        // Conditions on the preserved (left) side filter before the join, so that an inner join
        // tree under left joins can use them for its join order and pushdown. As the planner
        // does, the left input is bound once for its schema, then again under the filter. A
        // filter without subqueries over a join on an ON condition binds as a filter over the
        // join instead, and PushDownFilters moves those conditions
        val conds          = planner.conjuncts(f.filterExpr)
        val leftSchema     = bind(j.left).schema
        val (pushed, rest) = conds.partition(c =>
          !planner.hasSubquery(c) && planner.bindsIn(c, leftSchema)
        )
        if pushed.isEmpty then
          plans.conjuncts(lowered(bind(j)), conds).plan
        else
          val left = Filter(j.left, pushed.reduce((a, b) => And(a, b, NoSpan)), f.span)
          plans.conjuncts(lowered(bind(j.copy(left = left))), rest).plan
      case child =>
        // Common conjuncts of `or`s are factored by the Normalize pass
        val conds = planner.splitConjuncts(f.filterExpr)
        if conds.exists(planner.hasSubquery) then
          // Subqueries become joins over the optimized input, factored as the planner does:
          // semi and anti joins for [NOT] EXISTS / [NOT] IN conjuncts (plan-time constants for
          // uncorrelated ones with Decorrelate off), left joins for correlated scalar subqueries
          // and EXISTS under OR / NOT / CASE
          plans.conjuncts(lowered(bind(child)), planner.conjuncts(f.filterExpr)).plan
        else
          filterOver(bind(child), conds)

  /** Conditions without subqueries over `in`, as one filter */
  private def filterOver(in: RuntimePlan, conds: List[Expression]): RuntimePlan =
    val schema = in.schema
    val bound  = conds.map(c => BExpr.of(Binder(planner, BindScope(schema)).bind(c), in.output))
    RuntimePlan.Filter(
      in,
      BExpr(bound.map(_.expr).reduce((a, b) => AndExpr(a, b))),
      conds.zip(bound).map((c, b) => literalCounts(c, b.expr)).reduce(_ ++ _)
    )

  /**
    * unnest(list, ...) over the rows of `in`, keeping their columns with `keepInput`
    */
  private def unnest(
      in: RuntimePlan,
      call: TableFunctionCall,
      alias: Option[AliasedRelation],
      outer: Boolean,
      keepInput: Boolean = false
  ): RuntimePlan =
    val (lists, fields) = planner.unnestLists(in.schema, call, alias)
    RuntimePlan.Unnest(
      in,
      lists.map(BExpr.of(_, in.output)),
      keepInput,
      outer,
      fields.map(newColumn)
    )

  /** A join and filter conditions over it */
  private def join(j: Join, filters: List[Expression]): RuntimePlan =
    planner.lateralUnnest(j) match
      case Some(u) =>
        lateralUnnest(j, u, filters)
      case None =>
        nonLateralJoin(j, filters)

  /**
    * A lateral unnest over the left input and the conditions over it (its ON conjuncts, then the
    * filters)
    */
  private def lateralUnnest(
      j: Join,
      u: Planner.LateralUnnest,
      filters: List[Expression]
  ): RuntimePlan =
    val p     = unnest(bind(j.left), u.call, u.alias, u.outer, keepInput = true)
    val conds = u.conds ++ filters
    if conds.isEmpty then
      p
    else if conds.exists(planner.hasSubquery) then
      plans.conjuncts(lowered(p), planner.factorOrs(conds)).plan
    else
      // Common conjuncts of `or`s are factored by the Normalize pass
      filterOver(p, conds.flatMap(planner.splitConjuncts))

  private def nonLateralJoin(j: Join, filters: List[Expression]): RuntimePlan =
    if j.asof then
      // Not an inner join tree, so it takes no filters: those over it bind above it
      asOfJoin(j)
    else if !planner.isJoinOn(j) then
      // Filters over a USING or NATURAL join bind above it (only inner join trees take them)
      usingJoin(j)
    else if planner.isInnerJoin(j) then
      innerJoins(j, filters)
    else if isOuterJoinOn(j) && filters.isEmpty then
      outerJoin(j)
    else
      val left  = lowered(bind(j.left))
      val right = lowered(bind(j.right))
      plans.conjuncts(planner.joinOn(j, planner.joinKind(j), left, right, plans), filters).plan

  /**
    * An ASOF join: a [[RuntimePlan.AsOfJoin]] on the keys and the inequality that
    * `Planner.asOfJoinSpec` finds
    */
  private def asOfJoin(j: Join): RuntimePlan =
    val left  = bind(j.left)
    val right = bind(j.right)
    val spec  = planner.asOfJoinSpec(j, planner.joinKind(j), left.schema, right.schema)
    RuntimePlan.AsOfJoin(
      left,
      right,
      spec.kind,
      spec.leftKeys.map(BExpr.of(_, left.output)),
      spec.rightKeys.map(BExpr.of(_, right.output)),
      BExpr.of(spec.leftValue, left.output),
      BExpr.of(spec.rightValue, right.output),
      spec.op
    )

  /**
    * A USING or NATURAL join: a [[RuntimePlan.Join]] on the keys under a [[RuntimePlan.Project]]
    * merging them, as `Planner.usingJoin` plans it
    */
  private def usingJoin(j: Join): RuntimePlan =
    val left  = lowered(bind(j.left))
    val right = lowered(bind(j.right))
    val names = planner
      .usingKeys(j, left.op.schema, right.op.schema)
      .getOrElse(throw StatusCode.UNEXPECTED_STATE.newException(s"Not a USING join: ${j.cond}"))
    planner.usingJoin(left, right, names, planner.joinKind(j), plans).plan

  /**
    * Whether a join is a LEFT, RIGHT, or FULL join on an ON condition (or none) without subqueries,
    * which binds as a [[RuntimePlan.Join]] over unoptimized inputs
    */
  private def isOuterJoinOn(j: Join): Boolean =
    !planner.isInnerJoin(j) && planner.isJoinOn(j) && planner.unnestCall(j.right).isEmpty &&
      onCondition(j).forall(e => !planner.hasSubquery(e))

  private def onCondition(j: Join): Option[Expression] =
    j.cond match
      case JoinOn(e, _) =>
        Some(e)
      case JoinOnEq(keys, _) =>
        Some(Expression.concatWithEq(keys))
      case _ =>
        None

  /**
    * An outer join on its ON condition, with the condition bound unfactored as the residual: the
    * Normalize pass factors its `or`s, PushDownFilters turns the equalities between the inputs into
    * hash join keys, and ReorderJoins chooses the build side, as `Planner.joinOn` does
    */
  private def outerJoin(j: Join): RuntimePlan =
    val left     = bind(j.left)
    val right    = bind(j.right)
    val residual = onCondition(j).map { e =>
      val out = left.output ++ right.output
      val b   = Binder(planner, BindScope(Schema(out.map(_.toField)))).bind(
        planner.splitConjuncts(e).reduce((a, b) => And(a, b, NoSpan))
      )
      BExpr.of(b, out)
    }
    RuntimePlan.Join(
      left,
      right,
      planner.joinKind(j),
      IndexedSeq.empty,
      IndexedSeq.empty,
      residual,
      buildLeft = false
    )

  /**
    * A tree of inner joins (without a lateral unnest on top) and filter conditions over it, as one
    * [[RuntimePlan.InnerJoins]] node that [[ReorderJoins]] orders. Conditions with subqueries apply
    * over the ordered tree, through the planner, except for uncorrelated scalar subqueries, which
    * are constants (e.g., d_month_seq = (select ... from date_dim)), and uncorrelated IN conditions
    * reading one input over a subquery no larger than it, which join that input
    * ([[placedSemiJoin]]).
    *
    * Conditions bind in a fixed order, as they may evaluate scalar subqueries: the single-input
    * ones input by input, then the others. They bind unfactored: the Normalize pass factors their
    * `or`s, except for conditions with subqueries, which are factored as they are applied
    */
  private def innerJoins(j: Join, filters: List[Expression]): RuntimePlan =
    val (leaves, joinConds) = planner.flattenJoins(j, planner.splitConjuncts)
    val inputs              = leaves.map(bind).toIndexedSeq
    // Each condition with the form it takes without its scalar subqueries
    val all = (joinConds ++ filters)
      .flatMap { c =>
        val e = planner.withoutScalarSubqueries(c)
        if planner.hasSubquery(e) then
          // Factored in place, so that its conjuncts without subqueries join the others
          planner.factorOrs(List(c)).map(f => f -> planner.withoutScalarSubqueries(f))
        else
          List(c -> e)
      }
      .filterNot((c, _) => planner.isTrueLiteral(c))
    val (withSubqueries, conds) = all.partition((_, e) => planner.hasSubquery(e))
    val output                  = inputs.flatMap(_.output)
    val schemas                 = inputs.map(_.schema).toList
    // The inputs each condition reads, without evaluating its scalar subqueries
    val refs  = conds.map((_, e) => schemas.indices.filter(planner.referencesInput(e, schemas, _)))
    val bound = Array.ofDim[BExpr](conds.size)
    def bindAt(k: Int, over: IndexedSeq[Column]): Unit =
      val b = Binder(planner, BindScope(Schema(over.map(_.toField)))).bind(conds(k)._1)
      bound(k) = BExpr.of(b, over)
    // Single-input conditions first, input by input and over their input alone, as the planner
    // pushes them; then the others over every input
    inputs
      .indices
      .foreach(i => conds.indices.filter(refs(_) == Seq(i)).foreach(bindAt(_, inputs(i).output)))
    conds.indices.filter(bound(_) == null).foreach(bindAt(_, output))
    // Then the subqueries, in condition order. An uncorrelated IN condition reading one input
    // joins that input when its subquery is estimated no larger than the input: the semi join
    // then builds on the subquery and filters the input as it streams. Over a larger subquery (e.g., a CTE of unknown size), it joins the
    // ordered tree instead, whose joins may have reduced the input's rows, as without placement.
    // The other conditions with subqueries (NOT IN, which rarely removes many rows, among them)
    // apply over the ordered tree
    val placed = List.newBuilder[(Int, SubqueryConjunct.In, Lowered)]
    val lifted = List.newBuilder[(SubqueryConjunct.In, Lowered)]
    val above  = List.newBuilder[Expression]
    withSubqueries
      .map(_._1)
      .foreach { c =>
        singleInputIn(c, schemas) match
          case Some((i, in)) =>
            val inner = lowered(bind(in.sub))
            if CardinalityEstimator.rows(inner.op) <= Stats.rows(inputs(i)) then
              placed += ((i, in, inner))
            else
              lifted += in -> inner
          case None =>
            above += c
      }
    // Semi and anti joins output their input's columns, so the conditions keep their bindings
    val onInputs = placed.result()
    val joined   = inputs
      .indices
      .map(i =>
        onInputs
          .collect { case (`i`, in, inner) =>
            (in, inner)
          }
          .foldLeft(inputs(i))((p, s) => placedSemiJoin(p, s._1, s._2))
      )
    val node = RuntimePlan.InnerJoins(
      joined,
      conds.indices.map(k => JoinCondition(bound(k), literalCounts(conds(k)._1, bound(k).expr)))
    )
    val overTree = lifted.result()
    val others   = above.result()
    if overTree.isEmpty && others.isEmpty then
      node
    else
      val tree = overTree.foldLeft(lowered(node))((p, s) => plans.inJoin(p, s._1.target, s._2))
      plans.conjuncts(tree, others).plan

  end innerJoins

  /**
    * The input of an inner join tree that an uncorrelated IN condition reads alone, with the
    * condition, when [[Optimization.PlaceSemiJoins]] (and [[Optimization.Decorrelate]]) is on
    */
  private def singleInputIn(
      c: Expression,
      schemas: List[Schema]
  ): Option[(Int, SubqueryConjunct.In)] =
    if !planner.enabled(Optimization.Decorrelate) || !planner.enabled(Optimization.PlaceSemiJoins)
    then
      None
    else
      planner.subqueryConjunct(c) match
        case Some(in: SubqueryConjunct.In)
            if !in.negate && !planner.hasSubquery(in.target) && planner.isUncorrelated(in.sub) =>
          schemas.indices.filter(planner.referencesInput(in.target, schemas, _)) match
            case Seq(i) =>
              Some(i -> in)
            case _ =>
              None
        case _ =>
          None

  /**
    * Rows of an inner join tree input for which an uncorrelated IN is true: a semi join with the
    * lowered subquery `inner`, building on it
    */
  private def placedSemiJoin(p: RuntimePlan, in: SubqueryConjunct.In, inner: Lowered): RuntimePlan =
    val target   = Binder(planner, BindScope(p.schema)).bind(in.target)
    val (lk, rk) = inKeys(target, inner.op.schema(0))
    RuntimePlan.Join(
      p,
      inner.plan,
      JoinKind.Semi,
      IndexedSeq(BExpr.of(lk, p.output)),
      IndexedSeq(BExpr.of(rk, inner.plan.output)),
      None,
      buildLeft = false
    )

  /**
    * The keys of a semi or anti join of `target in (subquery)`: the target and the subquery's first
    * column `v`, cast to their common type
    */
  private def inKeys(target: VExpr, v: Field): (VExpr, VExpr) =
    val t = PType.common(target.ptype, v.ptype)
    (planner.castExpr(target, t), planner.castExpr(ColRef(0, v.ptype, v.name), t))

  /**
    * The values of a NOT IN subquery, a constant decided at plan time, or None when one is NULL (no
    * row passes then)
    */
  private def notInValues(sub: Relation): Option[(Schema, Batch)] =
    val op     = planner.plan(sub)
    val values = Operator.materialize(op)
    val v      = values.columns(0)
    Option.unless((0 until values.size).exists(v.isNull))((op.schema, values))

  /**
    * The literal counts of a condition and its `and`/`or` sub-conditions
    * ([[CardinalityEstimator.literalCount]], decided on the query's expressions), by the bound
    * signature of each, walking the condition and its binding together
    */
  private def literalCounts(e: Expression, bound: VExpr): Map[String, Int] =
    val counts                              = Map.newBuilder[String, Int]
    def loop(e: Expression, b: VExpr): Unit =
      (e, b) match
        case (p: ParenthesizedExpression, _) =>
          loop(p.child, b)
        case (a: And, AndExpr(l, r)) =>
          loop(a.left, l)
          loop(a.right, r)
        case (o: Or, OrExpr(l, r)) =>
          loop(o.left, l)
          loop(o.right, r)
        case _ =>
          CardinalityEstimator.literalCount(e).foreach(n => counts += b.sig -> n)
    loop(e, bound)
    counts.result()

  /** An aggregation, through the shared `Planner.aggregation` */
  private def aggregation(
      input: Relation,
      items: Option[List[Attribute]],
      defaultItems: Boolean
  ): RuntimePlan =
    planner.aggregation(input, items, defaultItems, r => lowered(bind(r)), plans).plan

  /** Count the groups of an aggregation input: a global count over the grouping */
  private def countGroups(c: Count): RuntimePlan =
    val groups = aggregation(c.child, None, defaultItems = false)
    val t      = Aggregates.resultType("count_star", Nil)
    val name   = c.selectItems.headOption.map(planner.nameOf).getOrElse("count")
    RuntimePlan.Aggregate(
      groups,
      IndexedSeq.empty,
      IndexedSeq(AggregateCall("count_star", Nil, distinct = false, filter = None, t)),
      IndexedSeq(newColumn(Field(name, t)))
    )

  /** A runtime plan with its operators, from which join planning reads schemas and estimates */
  private case class Lowered(plan: RuntimePlan, op: Operator)

  /**
    * Join planning reads schemas and estimates from the operators, so inputs are optimized first,
    * and join order sees their rewritten conditions
    */
  private def lowered(p: RuntimePlan): Lowered =
    val o = planner.optimizeRuntime(p)
    if o ne p then
      boundInputs.put(o, p)
    Lowered(o, Lowering(o))

  // Inputs optimized while binding: the optimized plan -> the bound plan it came from
  private val boundInputs = java.util.IdentityHashMap[RuntimePlan, RuntimePlan]()

  /**
    * The bound plan of an input the binder optimized while binding (an aggregation, pivot, or
    * subquery input), whose joins are already ordered in `optimized`
    */
  private[engine] def boundInput(optimized: RuntimePlan): Option[RuntimePlan] = Option(
    boundInputs.get(optimized)
  )

  /** Forget the inputs recorded for [[boundInput]], once the root plan is done */
  private[engine] def clearBoundInputs(): Unit = boundInputs.clear()

  /** Builds runtime-plan joins, aggregations, filters, and projections for the shared helpers */
  private object plans extends JoinPlanBuilder[Lowered] with AggPlanBuilder[Lowered]:
    override def op(p: Lowered): Operator = p.op

    override def join(
        left: Lowered,
        right: Lowered,
        kind: JoinKind,
        leftKeys: IndexedSeq[VExpr],
        rightKeys: IndexedSeq[VExpr],
        residual: Option[VExpr],
        buildLeft: Boolean
    ): Lowered =
      val l = left.plan
      val r = right.plan
      Lowered(
        RuntimePlan.Join(
          l,
          r,
          kind,
          leftKeys.map(BExpr.of(_, l.output)),
          rightKeys.map(BExpr.of(_, r.output)),
          residual.map(BExpr.of(_, l.output ++ r.output)),
          buildLeft
        ),
        JoinOrderOptimizer.joinOp(
          left.op,
          right.op,
          kind,
          leftKeys,
          rightKeys,
          residual,
          buildLeft,
          left.op.schema ++ right.op.schema
        )
      )

    end join

    override def sharedInput(in: Lowered, cols: IndexedSeq[Int], label: String): () => Lowered =
      val out    = in.plan.output
      val schema = Schema(cols.map(out(_).toField))
      val body   = RuntimePlan.Project(in.plan, cols.map(i => BExpr.ref(out(i))), columnsOf(schema))
      // The operator over the input's own operators, so that its plan is lowered only once
      val op  = ProjectOp(in.op, cols.map(i => ColRef(i, out(i).ptype, out(i).name)), schema)
      val cte = planner.sharedInput(label, body, op)
      () => Lowered(RuntimePlan.CTERef(cte, columnsOf(schema)), cte.reader(schema))

    override def values(schema: Schema, rows: Batch): Lowered =
      val p = RuntimePlan.Values(columnsOf(schema), rows)
      Lowered(p, Lowering(p))

    override def union(inputs: IndexedSeq[Lowered], schema: Schema): Lowered = Lowered(
      RuntimePlan.Union(inputs.map(_.plan), columnsOf(schema)),
      UnionAllOp(inputs.map(_.op), schema)
    )

    override def aggregate(
        in: Lowered,
        keys: IndexedSeq[VExpr],
        calls: IndexedSeq[AggCall],
        schema: Schema
    ): Lowered = Lowered(
      RuntimePlan.Aggregate(
        in.plan,
        keys.map(BExpr.of(_, in.plan.output)),
        calls.map(AggregateCall.of(_, in.plan.output)),
        columnsOf(schema)
      ),
      HashAggOp(in.op, keys, calls, schema)
    )

    override def project(p: Lowered, exprs: IndexedSeq[VExpr], schema: Schema): Lowered = Lowered(
      RuntimePlan.Project(p.plan, exprs.map(BExpr.of(_, p.plan.output)), columnsOf(schema)),
      ProjectOp(p.op, exprs, schema)
    )

    override def window(p: Lowered, calls: IndexedSeq[WindowCall], schema: Schema): Lowered =
      Lowered(windowOver(p.plan, calls, schema), WindowOp(p.op, calls, schema))

    override def filter(p: Lowered, cond: VExpr): Lowered = Lowered(
      RuntimePlan.Filter(p.plan, BExpr.of(cond, p.plan.output)),
      FilterOp(p.op, cond)
    )

    override def conjuncts(p: Lowered, conds: List[Expression]): Lowered =
      if conds.isEmpty then
        p
      else
        subqueryJoins(p, conds)

    /**
      * Conjuncts over p: [NOT] EXISTS / [NOT] IN conjuncts become semi and anti joins in conjunct
      * order (correlated ones with their bodies on the runtime plan), then the other conjuncts
      * filter their result, their correlated scalar subqueries and EXISTS under OR / NOT / CASE
      * joined by `Planner.bindWithSubqueries`. Uncorrelated subqueries inside them are constants,
      * as are uncorrelated [NOT] EXISTS / [NOT] IN conjuncts with [[Optimization.Decorrelate]] off
      */
    private def subqueryJoins(p: Lowered, conds: List[Expression]): Lowered =
      // With Decorrelate off, uncorrelated conjuncts are plain conditions, whose subqueries the
      // binder evaluates as plan-time constants (a value set for [NOT] IN)
      val decorrelate = planner.enabled(Optimization.Decorrelate)
      val subs        = conds.map(c =>
        planner.subqueryConjunct(c).filter(s => decorrelate || !planner.isUncorrelated(s.sub))
      )
      val plain = conds
        .zip(subs)
        .collect { case (c, None) =>
          c
        }
      val joined = subs.flatten.foldLeft(p)(subqueryJoin)
      if plain.isEmpty then
        joined
      else
        // Scalar subqueries and EXISTS marks extend the input, so bind every conjunct after them;
        // earlier conjuncts bound to the narrower input keep their positions
        var current = joined
        val bound   = plain.map { c =>
          val (extended, e) = planner.bindWithSubqueries(
            this,
            current,
            c,
            r => lowered(bind(r)),
            f => lowered(bind(f))
          )
          current = extended
          e
        }
        val filtered = filter(current, bound.reduce((a, b) => AndExpr(a, b)))
        val width    = p.op.schema.size
        val schema   = filtered.op.schema
        if schema.size == width then
          filtered
        else
          project(
            filtered,
            (0 until width).map(i => ColRef(i, schema(i).ptype, schema(i).name)),
            Schema(schema.fields.take(width))
          )
      end if

    end subqueryJoins

    /** Rows of p for which an EXISTS / NOT EXISTS / IN / NOT IN is true */
    private def subqueryJoin(p: Lowered, s: SubqueryConjunct): Lowered =
      def none: Lowered = filter(p, Const(false, PType.Bool))
      s match
        case _ if !planner.isUncorrelated(s.sub) =>
          // The decorrelated body binds on the runtime plan, joined as the planner joins it
          planner.correlatedJoin(this, p, s, r => lowered(bind(r)), f => lowered(bind(f)))
        case SubqueryConjunct.Exists(sub, negate) =>
          // A constant condition, decided at plan time
          val rows = Operator.materialize(planner.plan(sub)).size
          if (rows > 0) != negate then
            p
          else
            none
        case SubqueryConjunct.In(a, sub, false) =>
          inJoin(p, a, lowered(bind(sub)))
        case SubqueryConjunct.In(a, sub, true) =>
          // NOT IN: no row passes when the subquery has a NULL, and NULL targets never pass. The
          // subquery's values are a constant, decided at plan time
          notInValues(sub) match
            case None =>
              none
            case Some((schema, rows)) =>
              // The value list as a Values leaf, which lowers to the planner's BatchesOp
              val target   = Binder(planner, BindScope(p.op.schema)).bind(a)
              val inner    = values(schema, rows)
              val (lk, rk) = inKeys(target, inner.op.schema(0))
              val anti     = semiJoin(
                p,
                inner,
                JoinKind.Anti,
                IndexedSeq(lk),
                IndexedSeq(rk),
                buildLeft = false
              )
              filter(anti, IsNullExpr(target, negate = true))
      end match

    end subqueryJoin

    /**
      * Rows of p whose value of `a` is in the first column of the lowered subquery `inner`: a semi
      * join on the two cast to their common type, building on the subquery unless it is much larger
      * than p
      */
    def inJoin(p: Lowered, a: Expression, inner: Lowered): Lowered =
      val target   = Binder(planner, BindScope(p.op.schema)).bind(a)
      val (lk, rk) = inKeys(target, inner.op.schema(0))
      semiJoin(
        p,
        inner,
        JoinKind.Semi,
        IndexedSeq(lk),
        IndexedSeq(rk),
        planner.swapBuild(CardinalityEstimator.rows(inner.op), CardinalityEstimator.rows(p.op))
      )

    /** A semi or anti join of p with `inner`, on keys over each one's columns */
    private def semiJoin(
        p: Lowered,
        inner: Lowered,
        kind: JoinKind,
        lk: IndexedSeq[VExpr],
        rk: IndexedSeq[VExpr],
        buildLeft: Boolean
    ): Lowered = Lowered(
      RuntimePlan.Join(
        p.plan,
        inner.plan,
        kind,
        lk.map(BExpr.of(_, p.plan.output)),
        rk.map(BExpr.of(_, inner.plan.output)),
        None,
        buildLeft
      ),
      JoinOrderOptimizer.semiJoinOp(p.op, inner.op, lk, rk, kind, None, buildLeft)
    )

  end plans

  /** A projection without aggregates, windows, or unnest */
  private def isPlainProjection(p: Project): Boolean =
    !planner.isAggregationInput(p.child) &&
      p.selectItems
        .forall { i =>
          !Binder.containsAggregate(i) && !Binder.containsWindow(i) && isPlainItem(i)
        }

  /**
    * select ..., unnest(list), ...: the items with each list in place of its unnest, the lists
    * unnested, then the items in order with each list's elements
    */
  private def unnestProject(p: Project): RuntimePlan =
    val in                       = bind(p.child)
    val (exprs, fields, listIdx) = planner.bindUnnestItems(in.schema, p.selectItems)
    val proj  = RuntimePlan.Project(in, exprs.map(BExpr.of(_, in.output)), fields.map(newColumn))
    val elems = listIdx.map(i => newColumn(planner.unnestElementField(exprs, fields, i)))
    val u     = RuntimePlan.Unnest(
      proj,
      listIdx.map(i => BExpr.ref(proj.output(i))),
      keepInput = true,
      outer = false,
      elems
    )
    val out = proj
      .output
      .indices
      .map { i =>
        listIdx.indexOf(i) match
          case -1 =>
            proj.output(i)
          case k =>
            elems(k)
      }
    RuntimePlan.Project(u, out.map(BExpr.ref), out.map(c => newColumn(c.toField)))

  /**
    * An item without unnest, which the binder evaluates in a projection. Uncorrelated subqueries in
    * items bind as plan-time constants (`Planner.bindScalarSubquery`, `bindInSubquery`,
    * `bindExists`), and correlated ones fail
    */
  private def isPlainItem(i: Attribute): Boolean = planner.unnestArg(i).isEmpty

  /** The window calls over `in`, whose output is the window `schema` (input columns first) */
  private def windowOver(
      in: RuntimePlan,
      calls: IndexedSeq[WindowCall],
      schema: Schema
  ): RuntimePlan.Window = RuntimePlan.Window(
    in,
    calls.map(WindowFunction.of(_, in.output)),
    columnsOf(Schema(schema.fields.drop(in.output.size)))
  )

  /**
    * New columns added to (or, with `prepend`, prepended to) `child`, over the window calls of the
    * new columns
    */
  private def columnEdit(child: Relation, prepend: Boolean, items: List[Attribute]): RuntimePlan =
    val in                 = bind(child)
    val (w, exprs, fields) = withWindows(in, items)
    val all                = planner.columnEditItems(in.schema, prepend, exprs, fields)
    RuntimePlan.Project(w, all.map(e => BExpr.of(e._1, w.output)), all.map(e => newColumn(e._2)))

  /** The input columns at positions `idx`, as a projection */
  private def projectIndexes(in: RuntimePlan, idx: IndexedSeq[Int]): RuntimePlan = RuntimePlan
    .Project(
      in,
      idx.map(i => BExpr(ColRef(in.output(i).id.value, in.output(i).ptype, in.output(i).name))),
      idx.map(i => newColumn(in.output(i).toField))
    )

  /**
    * Bind `items` over `in`, evaluating their window calls first: the window over `in` (or `in`
    * itself without window calls), and the items over its output (the input columns first)
    */
  private def withWindows(
      in: RuntimePlan,
      items: List[Attribute]
  ): (RuntimePlan, IndexedSeq[VExpr], IndexedSeq[Field]) =
    val (calls, (exprs, fields)) =
      planner.bindWithWindows(in.schema, items)(planner.bindItems(_, items))
    (calls.map(c => windowOver(in, c.windowCalls, c.schema)).getOrElse(in), exprs, fields)

  /** A projection over the window calls of its items */
  private def windowProject(p: Project): RuntimePlan =
    val (w, exprs, fields) = withWindows(bind(p.child), p.selectItems)
    RuntimePlan.Project(w, exprs.map(BExpr.of(_, w.output)), fields.map(newColumn))

  private def project(p: Project): RuntimePlan =
    val in              = bind(p.child)
    val binder          = Binder(planner, BindScope(in.schema))
    val (exprs, fields) = planner.bindItems(binder, p.selectItems)
    RuntimePlan.Project(in, exprs.map(BExpr.of(_, in.output)), fields.map(newColumn))

  /**
    * A sort. Subqueries in sort keys bind as plan-time constants through the same `Binder`, as in
    * select items
    */
  private def sort(s: Sort, limit: Option[Long]): RuntimePlan =
    val in      = bind(s.child)
    val schema  = in.schema
    val orderBy = planner.resolveSortItems(s)
    // Keys over the input's positions
    val outKeys = orderBy.map { k =>
      try
        Right(planner.sortKeys(Binder(planner, BindScope(schema)), List(k), schema).head)
      catch
        case e @ (_: ColumnNotFound | _: WvletLangException) =>
          Left(e)
    }
    def orderKey(k: wvlet.lang.engine.exec.SortKey, over: RuntimePlan): OrderKey = OrderKey(
      BExpr.of(k.expr, over.output),
      k.ascending,
      k.nullsFirst
    )
    if outKeys.forall(_.isRight) then
      RuntimePlan.Sort(in, outKeys.map(k => orderKey(k.toOption.get, in)).toIndexedSeq, limit)
    else
      s.child match
        case p: Project =>
          // ORDER BY an expression outside the select list (an input column, or an aggregate
          // over a grouping): evaluate it as a hidden select item
          val hidden = orderBy
            .zip(outKeys)
            .collect { case (k, Left(_)) =>
              k -> planner.hiddenName("sort")
            }
          val proj = bind(
            p.copy(selectItems =
              p.selectItems ++
                hidden.map((k, name) => SingleColumn(NameExpr.fromString(name), k.sortKey, NoSpan))
            )
          )
          val out        = proj.output
          val hiddenRefs =
            hidden
              .map { (k, name) =>
                val c  = out.find(_.name == name).get
                val sk = planner.sortKey(k, ColRef(c.id.value, c.ptype, name))
                OrderKey(BExpr(sk.expr), sk.ascending, sk.nullsFirst)
              }
              .iterator
          // The select items keep their positions in the extended projection
          val keys = outKeys.map {
            case Right(k) =>
              orderKey(k, proj)
            case Left(_) =>
              hiddenRefs.next()
          }
          val sorted = RuntimePlan.Sort(proj, keys.toIndexedSeq, limit)
          if out.exists(c => planner.isHidden(c.toField)) then
            val keep = out.filterNot(c => planner.isHidden(c.toField))
            RuntimePlan.Project(sorted, keep.map(BExpr.ref), keep.map(c => newColumn(c.toField)))
          else
            sorted
        case _ =>
          throw outKeys
            .collectFirst { case Left(e) =>
              e
            }
            .get
    end if

  end sort

end RuntimeBinder
