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
  *   - `activate('target', param: value, ...)` — recorded as an activation of the materialized
  *     stage output, delivered by the executor to the matching [[ActivationSink]]
  *   - `end()` — pass-through terminal marker
  *   - `-> OtherFlow` (jump) — recorded as a control-only jump target; the executor triggers the
  *     target flow as a new run after the jumping stage's flow completes
  */
/** An activation target with its named parameters */
case class ActivationSpec(target: String, params: Map[String, String] = Map.empty)

object ActivationSpec:
  /**
    * Extract an activation spec from an `activate('target', name: value, ...)` expression used as a
    * flow config value (e.g. `on_failure: activate('webhook', url: '...')`)
    */
  def fromExpression(e: Expression): Option[ActivationSpec] =
    e match
      case f: FunctionApply =>
        val isActivate =
          f.base match
            case n: NameExpr =>
              n.fullName == "activate"
            case _ =>
              false
        val target = f
          .args
          .collectFirst {
            case arg: FunctionArg if arg.name.isEmpty =>
              arg.value match
                case s: StringLiteral =>
                  s.unquotedValue
                case other =>
                  other.toString
          }
        if isActivate then
          val params =
            f.args
              .collect {
                case arg: FunctionArg if arg.name.isDefined =>
                  val value =
                    arg.value match
                      case s: StringLiteral =>
                        s.unquotedValue
                      case l: Literal =>
                        l.stringValue
                      case other =>
                        other.toString
                  arg.name.get.name -> value
              }
              .toMap
          target.map(ActivationSpec(_, params))
        else
          None
      case _ =>
        None

end ActivationSpec

/**
  * Flow-level notification hooks extracted from the flow's `with { ... }` block: activation specs
  * fired after a run reaches its terminal state. `on_failure` fires for failed runs, `on_success`
  * for successful runs, and `on_finish` for both
  */
case class FlowNotifyConfig(
    onFailure: List[ActivationSpec] = Nil,
    onSuccess: List[ActivationSpec] = Nil,
    onFinish: List[ActivationSpec] = Nil
):
  def isEmpty: Boolean = onFailure.isEmpty && onSuccess.isEmpty && onFinish.isEmpty

  /** The hooks that apply to a run outcome */
  def hooksFor(failed: Boolean): List[ActivationSpec] =
    (
      if failed then
        onFailure
      else
        onSuccess
    ) ++ onFinish

object FlowNotifyConfig:
  def fromFlow(flow: FlowDef): FlowNotifyConfig =
    flow
      .config
      .foldLeft(FlowNotifyConfig()) { (c, item) =>
        def spec = ActivationSpec.fromExpression(item.value)
        item.key.unquotedValue match
          case "on_failure" =>
            spec.fold(c)(s => c.copy(onFailure = c.onFailure :+ s))
          case "on_success" =>
            spec.fold(c)(s => c.copy(onSuccess = c.onSuccess :+ s))
          case "on_finish" =>
            spec.fold(c)(s => c.copy(onFinish = c.onFinish :+ s))
          case _ =>
            c
      }

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
    *   External activation targets (with named parameters) to deliver the materialized output to
    * @param jumpTargets
    *   Flows to trigger as new runs when this stage succeeds (control-only transfer)
    */
  case class LoweredStage(
      stage: StageDef,
      body: Option[Relation],
      waitMillis: Option[Long] = None,
      activateTargets: List[ActivationSpec] = Nil,
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
      val activations              = List.newBuilder[ActivationSpec]
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
                activations += activationSpecOf(a)
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

  /** The activation target and its named parameters, e.g. activate('file', path: 'out.csv') */
  private def activationSpecOf(a: FlowActivate): ActivationSpec =
    val target =
      a.target match
        case s: StringLiteral =>
          s.unquotedValue
        case other =>
          other.toString
    val params =
      a.params
        .collect {
          case arg: FunctionArg if arg.name.isDefined =>
            val value =
              arg.value match
                case s: StringLiteral =>
                  s.unquotedValue
                case l: Literal =>
                  l.stringValue
                case other =>
                  other.toString
            arg.name.get.name -> value
        }
        .toMap
    ActivationSpec(target, params)

end FlowLowering
