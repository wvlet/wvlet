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

import wvlet.lang.api.StatusCode
import wvlet.lang.engine.exec.*
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*
import wvlet.lang.model.expr.*

import scala.collection.mutable

/**
  * Collects the window function calls of a projection. The first binding pass registers calls;
  * after [[build]], a frozen collector maps each call to its column of the window operator
  */
class WindowCollector(
    input: Operator,
    frozenOp: Option[Operator] = None,
    known: IndexedSeq[WindowCall] = IndexedSeq.empty
):
  private val calls = mutable.ArrayBuffer.from(known)

  def frozen(op: Operator): WindowCollector = WindowCollector(input, Some(op), calls.toIndexedSeq)

  def build(): Operator =
    val fields = calls.zipWithIndex.map((c, i) => Field(s"__wv_win${i}", c.ptype))
    WindowOp(input, calls.toIndexedSeq, Schema(input.schema.fields ++ fields))

  def ref(binder: Binder, e: Expression): VExpr =
    val call = toCall(binder, e)
    val i    = calls.indexWhere(_.sig == call.sig)
    val idx  =
      if i >= 0 then
        i
      else if frozenOp.isDefined then
        throw StatusCode.INVALID_ARGUMENT.newException(s"Unknown window call: ${call.sig}")
      else
        calls += call
        calls.size - 1
    ColRef(input.schema.size + idx, call.ptype, s"__wv_win${idx}")

  private def inputBinder(binder: Binder): Binder = binder

  /** The function call of a window base, expanding `sql"..."` templates (e.g., stdlib rank()) */
  private def baseFunction(e: Expression): Option[FunctionApply] =
    e match
      case f: FunctionApply =>
        Some(f)
      case t: TypedExpression =>
        baseFunction(t.child)
      case p: ParenthesizedExpression =>
        baseFunction(p.child)
      case s: InterpolatedString if Binder.isSqlTemplate(s) =>
        baseFunction(Binder.expandTemplates(s))
      case _ =>
        None

  private def toCall(binder: Binder, e: Expression): WindowCall =
    val (f, window, nullTreatment) =
      e match
        case w: WindowApply =>
          baseFunction(w.base) match
            case Some(f) =>
              (f, w.window, w.nullTreatment)
            case None =>
              val other = w.base
              throw StatusCode
                .NOT_IMPLEMENTED
                .newException(s"Unsupported window expression: ${other}")
        case f: FunctionApply =>
          (f, f.window.get, None)
        case other =>
          throw StatusCode.NOT_IMPLEMENTED.newException(s"Unsupported window expression: ${other}")
    // Bind against the window input (the first pass scope is the input schema)
    val b    = Binder(binder.plannerRef, BindScope(input.schema))
    val name = Binder.functionName(f)
    val args = f
      .args
      .filter(a => !a.value.isInstanceOf[AllColumns] && !a.value.isInstanceOf[Wildcard])
      .map(a => b.bind(a.value))
    val fname =
      if name == "count" && args.isEmpty then
        "count_star"
      else
        name
    val partitionBy = window.partitionBy.map(b.bind)
    val orderBy     = window
      .orderBy
      .map { s =>
        SortKey(
          b.bind(s.sortKey),
          !s.ordering.contains(SortOrdering.Descending),
          s.nullOrdering.contains(NullOrdering.NullIsFirst)
        )
      }
    val frame = window
      .frame
      .map { fr =>
        def bound(x: FrameBound): Option[Long] =
          x match
            case FrameBound.UnboundedPreceding | FrameBound.UnboundedFollowing =>
              None
            case FrameBound.CurrentRow =>
              Some(0L)
            case FrameBound.Preceding(n) =>
              Some(-n)
            case FrameBound.Following(n) =>
              Some(n)
        FrameSpec(fr.frameType == FrameType.RowsFrame, bound(fr.start), bound(fr.end))
      }
    val ignoreNulls = nullTreatment.contains(NullTreatment.IgnoreNulls)
    WindowCall(
      fname,
      args,
      partitionBy,
      orderBy,
      frame,
      ignoreNulls,
      WindowCall.resultType(fname, args.map(_.ptype))
    )

  end toCall

end WindowCollector
