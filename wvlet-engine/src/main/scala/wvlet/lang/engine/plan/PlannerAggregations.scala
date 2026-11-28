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
import wvlet.lang.engine.catalog.*
import wvlet.lang.engine.exec.*
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.runtime.ShareSubplans
import wvlet.lang.engine.vector.*
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*

import scala.collection.mutable
import scala.util.control.NonFatal

/**
  * The aggregations of [[Planner]]: GROUP BY and global aggregations, grouping sets (chained or
  * over a shared input), the groups shared with an IN subquery over the same input
  * (`share_in_subquery_groups`), and the group totals computed at plan time
  */
trait PlannerAggregations:
  this: Planner =>

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
        case NonFatal(e) if !MemoryContext.exceededLimit(e) =>
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
  private[plan] def inSubqueryShare(source: Relation): Option[Planner.InShare] =
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
  private[plan] def groupsFilteredByIn[P](
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
  private[plan] def groupsFromShared[P](
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
  private[plan] def computeGroupTotals[P](
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
  private[plan] def groupTotal(
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
  private[plan] case class CorrelatedItems(
      error: ColumnNotFound,
      items: List[Attribute],
      subqueries: List[(String, Expression)]
  )

  /**
    * `e` with each correlated scalar subquery (or EXISTS) in place of a hidden name, and the
    * subqueries by name: the same top-down walk as `rewriteSubqueries`
    */
  private[plan] def extractCorrelatedSubqueries(
      e: Expression
  ): (Expression, List[(String, Expression)]) =
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
  private[plan] def chainGroupingSets[P](
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
  private[plan] def groupByVaryingKeys[P](
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

end PlannerAggregations
