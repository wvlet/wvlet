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

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.TestRelation

/**
  * Evaluate a `test` statement against the preceding query result. Pure expression evaluation over
  * the materialized [[QueryResult]] — no engine access — so it is shared by the JVM `QueryExecutor`
  * and the cross-platform `PlanExecutor` and behaves identically on JVM, Node.js, and Native.
  * (Extracted verbatim from `QueryExecutor.executeTest`.)
  */
object TestEvaluator:

  def evaluate(test: TestRelation, lastResult: QueryResult, workEnv: WorkEnv)(using
      context: Context
  ): QueryResult =

    given unit: CompilationUnit = context.compilationUnit

    def isShortString(x: Any): Boolean =
      def fitToSingleLine(x: String): Boolean = x != null && x.length < 30 && !x.contains("\n")

      x match
        case s: String =>
          fitToSingleLine(s)
        case null =>
          true
        case x if fitToSingleLine(x.toString) =>
          true
        case _ =>
          false

    def pp(x: Any): String =
      x match
        case s: Seq[?] =>
          s"[${s.map(pp).mkString(", ")}]"
        case null =>
          "null"
        case _ =>
          x.toString

    def cmpMsg(op: String, l: Any, r: Any): String =
      (l, r) match
        case (l: Any, r: Any) if isShortString(l) && isShortString(r) =>
          s"${pp(l)} ${op} ${pp(r)}"
        case _ =>
          s"${pp(l)}\n${op}\n${pp(r)}"

    def eval(e: Expression): QueryResult =
      e match
        case ShouldExpr(TestType.ShouldBe, left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          if leftValue != rightValue then
            TestFailure(cmpMsg("was not equal to", leftValue, rightValue), e.sourceLocation)
          else
            TestSuccess(cmpMsg("was equal to", leftValue, rightValue), e.sourceLocation)
        case Eq(left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          if leftValue != rightValue then
            TestFailure(cmpMsg("was not equal to", leftValue, rightValue), e.sourceLocation)
          else
            TestSuccess(cmpMsg("was equal to", leftValue, rightValue), e.sourceLocation)
        case ShouldExpr(TestType.ShouldNotBe, left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          if leftValue == rightValue then
            TestFailure(cmpMsg("was equal to", leftValue, rightValue), e.sourceLocation)
          else
            TestSuccess(cmpMsg("was not equal to", leftValue, rightValue), e.sourceLocation)
        case NotEq(left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          if leftValue == rightValue then
            TestFailure(cmpMsg("was equal to", leftValue, rightValue), e.sourceLocation)
          else
            TestSuccess(cmpMsg("was not equal to", leftValue, rightValue), e.sourceLocation)
        case IsNull(left, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = null
          if leftValue != rightValue then
            TestFailure(s"${pp(leftValue)} was not null", e.sourceLocation)
          else
            TestSuccess(s"${pp(leftValue)} was null", e.sourceLocation)
        case IsNotNull(left, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = null
          if leftValue == rightValue then
            TestFailure(s"${pp(leftValue)} was null", e.sourceLocation)
          else
            TestSuccess(s"${pp(leftValue)} was not null", e.sourceLocation)
        case ShouldExpr(TestType.ShouldContain, left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          (leftValue, rightValue) match
            case (l: String, r: String) =>
              if l.contains(r) then
                TestSuccess(cmpMsg("contained", leftValue, rightValue), e.sourceLocation)
              else
                TestFailure(cmpMsg("did not contain", leftValue, rightValue), e.sourceLocation)
            case (l: List[?], r: Any) =>
              if l.contains(r) then
                TestSuccess(cmpMsg("contained", leftValue, rightValue), e.sourceLocation)
              else
                TestFailure(cmpMsg("did not contain", leftValue, rightValue), e.sourceLocation)
            case _ =>
              WarningResult(
                s"`contain` operator is not supported for: ${leftValue} and ${rightValue}",
                e.sourceLocation
              )
        case ShouldExpr(TestType.ShouldNotContain, left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          (leftValue, rightValue) match
            case (l: String, r: String) =>
              if l.contains(r) then
                TestFailure(cmpMsg("contained", leftValue, rightValue), e.sourceLocation)
              else
                TestSuccess(cmpMsg("did not contain", leftValue, rightValue), e.sourceLocation)
            case (l: List[?], r: Any) =>
              if l.contains(r) then
                TestFailure(cmpMsg("contained", leftValue, rightValue), e.sourceLocation)
              else
                TestSuccess(cmpMsg("did not contain", leftValue, rightValue), e.sourceLocation)
            case _ =>
              WarningResult(
                s"`contain` operator is not supported for: ${leftValue} and ${rightValue}",
                e.sourceLocation
              )
        case LessThanOrEq(left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          cmpAny(leftValue, rightValue)
            .map {
              case x if x <= 0 =>
                TestSuccess(
                  cmpMsg("was less than or equal to", leftValue, rightValue),
                  e.sourceLocation
                )
              case _ =>
                TestFailure(
                  cmpMsg("was not less than or equal to", leftValue, rightValue),
                  e.sourceLocation
                )
            }
            .getOrElse {
              WarningResult(s"Can't compare ${leftValue} and ${rightValue}", e.sourceLocation)
            }
        case LessThan(left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          cmpAny(leftValue, rightValue)
            .map {
              case x if x < 0 =>
                TestSuccess(cmpMsg("was less than", leftValue, rightValue), e.sourceLocation)
              case _ =>
                TestFailure(cmpMsg("was not less than", leftValue, rightValue), e.sourceLocation)
            }
            .getOrElse {
              WarningResult(s"Can't compare ${leftValue} and ${rightValue}", e.sourceLocation)
            }
        case GreaterThanOrEq(left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          cmpAny(leftValue, rightValue)
            .map {
              case x if x >= 0 =>
                TestSuccess(
                  cmpMsg("was greater than or equal to", leftValue, rightValue),
                  e.sourceLocation
                )
              case _ =>
                TestFailure(
                  cmpMsg("was not greater than or equal to", leftValue, rightValue),
                  e.sourceLocation
                )
            }
            .getOrElse {
              WarningResult(s"Can't compare ${leftValue} and ${rightValue}", e.sourceLocation)
            }
        case GreaterThan(left, right, _) =>
          val leftValue  = trim(evalOp(left))
          val rightValue = trim(evalOp(right))
          cmpAny(leftValue, rightValue)
            .map {
              case x if x > 0 =>
                TestSuccess(cmpMsg("was greater than", leftValue, rightValue), e.sourceLocation)
              case _ =>
                TestFailure(cmpMsg("was not greater than", leftValue, rightValue), e.sourceLocation)
            }
            .getOrElse {
              WarningResult(s"Can't compare ${leftValue} and ${rightValue}", e.sourceLocation)
            }
        case _ =>
          WarningResult(s"Unsupported test expression: ${e}", e.sourceLocation)

    def cmpAny(l: Any, r: Any): Option[Int] =
      (l, r) match
        case (l: String, r: String) =>
          Some(l.compareTo(r))
        case (l: Int, r: Int) =>
          Some(l.compareTo(r))
        case (l: Int, r: Long) =>
          Some(l.toLong.compareTo(r))
        case (l: Long, r: Long) =>
          Some(l.compareTo(r))
        case (l: Long, r: Int) =>
          Some(l.compareTo(r.toLong))
        case (l: Float, r: Float) =>
          Some(l.compareTo(r))
        case (l: Double, r: Double) =>
          Some(l.compareTo(r))
        case (l: Boolean, r: Boolean) =>
          Some(l.compareTo(r))
        case (l: BigDecimal, r: BigDecimal) =>
          Some(l.compareTo(r))
        case _ =>
          None

    def evalOp(e: Expression): Any =
      e match
        case DotRef(i: Identifier, name, _, _) if i.fullName == "_" =>
          name.leafName match
            case "output" =>
              lastResult.toPrettyBox()
            case "columns" =>
              lastResult match
                case t: TableRows =>
                  t.schema.fields.map(_.name.name).toList
                case _ =>
                  List.empty
            case "size" =>
              lastResult match
                case t: TableRows =>
                  t.totalRows
                case _ =>
                  0
            case "json" =>
              lastResult match
                case t: TableRows =>
                  t.toJsonLines
                case _ =>
                  ""
            case "rows" =>
              lastResult match
                case t: TableRows =>
                  t.rows.map(_.values.toList).toList
                case _ =>
                  List.empty
            case other =>
              throw StatusCode
                .TEST_FAILED
                .newException(s"Unsupported result inspection function: _.${other}")
          end match
        case l: StringLiteral =>
          l.unquotedValue
        case l: LongLiteral =>
          l.value
        case d: DoubleLiteral =>
          d.value
        case b: BooleanLiteral =>
          b.booleanValue
        case d: DecimalLiteral =>
          d.value
        case n: NullLiteral =>
          null
        case a: ArrayConstructor =>
          a.values.map(evalOp)
        case m: MapValue =>
          m.entries
            .map { x =>
              evalOp(x.key) -> evalOp(x.value)
            }
            .toMap
        case other =>
          workEnv.warn(s"Test expression ${e} is not supported yet.")
          ()

    def trim(v: Any): Any =
      v match
        case s: String =>
          s.trim
        case _ =>
          v

    try
      eval(test.testExpr)
    catch
      case e: WvletLangException =>
        TestFailure(e.getMessage, test.sourceLocation)

  end evaluate

end TestEvaluator
