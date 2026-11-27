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
import wvlet.lang.engine.runtime.Stats
import wvlet.lang.engine.vector.*
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*

/**
  * The joins of [[Planner]]: flattening inner joins for the join search, join kinds and conditions,
  * equi-join keys, USING joins, lateral unnest, and ASOF joins
  */
trait PlannerJoins:
  this: Planner =>

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

  private[plan] def isEquiBetween(c: Expression, left: Schema, right: Schema): Boolean =
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

  private[plan] def equiSides(
      c: Expression,
      left: Schema,
      right: Schema
  ): (Expression, Expression) =
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
  private[plan] def swapsOuterBuild[P](
      kind: JoinKind,
      builder: JoinTreeBuilder[P],
      left: P,
      right: P
  ): Boolean =
    val aggregates         = CardinalityEstimator.aggregates(builder.op(right))
    def rows(p: P): Double =
      if aggregates then
        CardinalityEstimator.rows(builder.op(p))
      else
        builder.buildRows(p)
    kind match
      case JoinKind.Left | JoinKind.Right | JoinKind.Full =>
        JoinOrderOptimizer.swapOuterBuild(options, rows(right), rows(left), aggregates)
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

end PlannerJoins
