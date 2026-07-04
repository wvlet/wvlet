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
package wvlet.lang.runner

import wvlet.lang.api.Span
import wvlet.lang.api.Span.NoSpan
import wvlet.lang.api.StatusCode
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*

/**
  * Lowers flow operators inside stage bodies into a plain SQL-expressible body plus orchestration
  * metadata, keeping the flow executor engine-agnostic:
  *
  *   - `fork { stage ... }` — nested stages are flattened into top-level scheduler stages; the fork
  *     node itself becomes a pass-through of its input
  *   - `route { case cond -> target; else -> other }` — the route stage materializes its input
  *     unchanged; each *target* stage's read of the route stage is filtered with the case predicate
  *     (percentage cases become deterministic bucket predicates over the `by` expression)
  *   - `wait('30 minutes')` — recorded as a pre-materialization delay
  *   - `activate('target')` — recorded as an activation of the materialized stage output (local
  *     stub until sink connectors exist)
  *   - `end()` — pass-through terminal marker
  *   - `-> OtherFlow` (jump) — recorded as a control-only jump target; the executor triggers the
  *     target flow as a new run after the jumping stage's flow completes
  */
object FlowLowering:

  /**
    * A stage with flow operators stripped from its body
    *
    * @param stage
    *   The original stage definition (fork-nested stages appear as their own entries)
    * @param body
    *   The SQL-expressible stage body (pass-through where flow operators were removed)
    * @param waitMillis
    *   Delay to apply before materializing the stage
    * @param activateTargets
    *   External activation targets to notify with the materialized output
    * @param jumpTargets
    *   Flows to trigger as new runs when this stage succeeds (control-only transfer)
    */
  case class LoweredStage(
      stage: StageDef,
      body: Option[Relation],
      waitMillis: Option[Long] = None,
      activateTargets: List[String] = Nil,
      jumpTargets: List[String] = Nil
  ):
    def name: String = stage.name.name

  /**
    * A flow with all stage bodies lowered
    *
    * @param flow
    *   The original flow definition
    * @param stages
    *   Lowered stages in scheduling order (fork-nested stages follow their parent)
    * @param routeFilters
    *   Predicate to apply when `readerStage` reads the output of `routeStage`, keyed by
    *   (readerStage, routeStage)
    */
  case class LoweredFlow(
      flow: FlowDef,
      stages: List[LoweredStage],
      routeFilters: Map[(String, String), Expression]
  ):
    def stageNames: Set[String] = stages.map(_.name).toSet

  def lower(flow: FlowDef): LoweredFlow =
    // Flatten fork-nested stages right after their declaring stage
    val flatStages: List[StageDef] = flow
      .stages
      .flatMap { s =>
        val nested = List.newBuilder[StageDef]
        s.body
          .foreach {
            _.traverse { case f: FlowFork =>
              nested ++= f.stages
            }
          }
        s :: nested.result()
      }

    val routeFilters = Map.newBuilder[(String, String), Expression]

    val lowered = flatStages.map { s =>
      var waitMillis: Option[Long] = None
      val activations              = List.newBuilder[String]
      val jumps                    = List.newBuilder[String]
      val newBody                  = s
        .body
        .map {
          _.transformUp {
              case r: FlowRoute =>
                routePredicatesOf(r).foreach { (target, pred) =>
                  routeFilters += (target, s.name.name) -> pred
                }
                r.child
              case f: FlowFork =>
                f.child
              case w: FlowWait =>
                waitMillis = Some(waitMillis.getOrElse(0L) + waitDurationMillis(w))
                w.child
              case a: FlowActivate =>
                activations += activationTargetOf(a)
                a.child
              case e: FlowEnd =>
                e.child
              case j: FlowJump =>
                jumps += j.targetFlow.fullName
                j.child
            }
            .asInstanceOf[Relation]
        }
      LoweredStage(s, newBody, waitMillis, activations.result(), jumps.result())
    }
    LoweredFlow(flow, lowered, routeFilters.result())

  end lower

  /** Compute the per-target routing predicates of a route operator */
  private def routePredicatesOf(r: FlowRoute): List[(String, Expression)] =
    val conditionCases  = r.cases.filter(_.condition.isDefined)
    val percentageCases = r.cases.filter(_.percentage.isDefined)

    val fromConditions: List[(String, Expression)] = conditionCases.map { c =>
      c.target.fullName -> resolveContextRefs(c.condition.get)
    }

    val fromPercentages: List[(String, Expression)] =
      if percentageCases.isEmpty then
        Nil
      else
        val byExpr = r
          .byExpr
          .getOrElse(
            throw StatusCode
              .NOT_IMPLEMENTED
              .newException(
                "Percentage-based route requires a deterministic partitioning key: route by <expr> { ... }"
              )
          )
        // bucket = ((byExpr % 100) + 100) % 100, robust against negative values
        def lit100 = LongLiteral(100L, "100", NoSpan)
        val bucket = ArithmeticBinaryExpr(
          BinaryExprType.Modulus,
          ArithmeticBinaryExpr(
            BinaryExprType.Add,
            ArithmeticBinaryExpr(BinaryExprType.Modulus, byExpr, lit100, NoSpan),
            lit100,
            NoSpan
          ),
          lit100,
          NoSpan
        )
        var cumulative = 0L
        percentageCases.map { c =>
          val start = cumulative
          val end   = cumulative + c.percentage.get
          cumulative = end
          val pred =
            if start == 0 then
              LessThan(bucket, LongLiteral(end, end.toString, NoSpan), NoSpan)
            else
              And(
                GreaterThanOrEq(bucket, LongLiteral(start, start.toString, NoSpan), NoSpan),
                LessThan(bucket, LongLiteral(end, end.toString, NoSpan), NoSpan),
                NoSpan
              )
          c.target.fullName -> pred
        }

    val explicit = fromConditions ++ fromPercentages

    // The else target receives everything not matched by any explicit case
    val fromElse: List[(String, Expression)] =
      r.elseTarget
        .map { t =>
          val negated: Expression = explicit
            .map(_._2)
            .reduceLeftOption[Expression]((l, rr) => Or(l, rr, NoSpan))
            .map(all => Not(all, NoSpan))
            .getOrElse(TrueLiteral(NoSpan))
          t.fullName -> negated
        }
        .toList

    explicit ++ fromElse

  end routePredicatesOf

  /**
    * Route case predicates reference the input row as `_.column`. When the predicate is applied as
    * a filter over the materialized route table, the underscore context resolves to that table's
    * row, so `_.column` becomes a plain column reference
    */
  private def resolveContextRefs(e: Expression): Expression = e.transformUpExpression {
    case d: DotRef if d.qualifier.isInstanceOf[ContextInputRef] =>
      d.name
  }

  /** Parse the wait duration: a duration literal (30s) or a string like '30 minutes' */
  private def waitDurationMillis(w: FlowWait): Long =
    w.duration match
      case d: DurationLiteral =>
        d.toMillis
      case s: StringLiteral =>
        parseDurationString(s.unquotedValue.trim)
      case other =>
        throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported wait duration: ${other}")

  private val durationPattern = """(\d+)\s*([a-zA-Z]+)""".r

  private def parseDurationString(s: String): Long =
    s match
      case durationPattern(num, unit) =>
        val n = num.toLong
        unit.toLowerCase match
          case "ms" | "millisecond" | "milliseconds" =>
            n
          case "s" | "sec" | "second" | "seconds" =>
            n * 1000L
          case "m" | "min" | "minute" | "minutes" =>
            n * 60000L
          case "h" | "hour" | "hours" =>
            n * 3600000L
          case "d" | "day" | "days" =>
            n * 86400000L
          case other =>
            throw StatusCode.NOT_IMPLEMENTED.newException(s"Unknown duration unit: ${other}")
      case _ =>
        throw StatusCode.NOT_IMPLEMENTED.newException(s"Cannot parse wait duration: '${s}'")

  private def activationTargetOf(a: FlowActivate): String =
    a.target match
      case s: StringLiteral =>
        s.unquotedValue
      case other =>
        other.toString

end FlowLowering
