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
import wvlet.lang.engine.catalog.*
import wvlet.lang.engine.exec.*
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*

/**
  * The filters and subqueries of [[Planner]]: conjunct splitting and OR factoring, IN / EXISTS /
  * scalar subqueries bound at plan time, and their decorrelation into joins ([[correlatedJoin]],
  * `decorrelateExists`, `decorrelateScalar`)
  */
trait PlannerSubqueries:
  this: Planner =>

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

  private[plan] def splitOr(e: Expression): List[Expression] =
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
  private[plan] def exprKey(e: Expression): String =
    if enabled(Optimization.PlanningMemo) then
      // A subquery's conditions are factored each time it is planned, as copies (`planning_memo`):
      // equal expressions print the same SQL text
      exprKeys.getOrElseUpdate(e, sqlText(e))
    else
      sqlText(e)

  // The keys exprKey computed so far, by expression
  private[plan] val exprKeys = scala.collection.mutable.HashMap.empty[Expression, String]

  /** (a and b) or (a and c) => a and (b or c), so join keys inside OR branches become usable */
  private[plan] def factorOr(e: Expression): List[Expression] =
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

  private[plan] def splitAnd(e: Expression): List[Expression] =
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
  private[plan] def isCorrelated(sub: Relation, outer: Schema): Boolean =
    def check(): Boolean =
      correlationChecks += 1
      try
        Planner.nested(this).planRelation(sub)
        false
      catch
        case e: ColumnNotFound =>
          true
    if enabled(Optimization.PlanningMemo) then
      // Asked several times per subquery while planning a query (`planning_memo`)
      val known = correlatedSubqueries.get(sub)
      if known != null then
        known
      else
        val c = check()
        correlatedSubqueries.put(sub, c)
        c
    else
      check()

  // Whether each subquery planned so far is correlated, by identity
  private[plan] val correlatedSubqueries = java.util.IdentityHashMap[Relation, java.lang.Boolean]()

  /** How many times this planner planned a subquery to tell whether it is correlated (for tests) */
  private[engine] var correlationChecks: Int = 0

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

  private[plan] def subqueryRelation(e: Expression): Relation =
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
  private[plan] def stripWrappers(r: Relation): Relation =
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
  private[plan] def planCorrelated[P](
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
  private[plan] def correlationKeys(
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
  private[plan] def joinKeys(
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
  private[plan] def residualBinder(outer: Schema, inner: Schema): Binder = Binder(
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
  private[plan] def decorrelateScalar[P](
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
  private[plan] def emptyGroupValue(
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
  private[plan] def decorrelateExists[P](
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
  private[plan] def callsVolatile(e: Expression): Boolean =
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

  private[plan] def evalScalarSubquery(sub: Relation): VExpr =
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

end PlannerSubqueries
