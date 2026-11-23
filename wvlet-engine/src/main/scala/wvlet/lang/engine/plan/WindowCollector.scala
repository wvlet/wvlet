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
  * Collects the window function calls of a projection over an input of schema `input`. The first
  * binding pass registers calls; a [[frozen]] collector maps each call to its column of the window
  * output (the input columns, then one column per call, [[schema]])
  */
class WindowCollector(
    input: Schema,
    isFrozen: Boolean = false,
    known: IndexedSeq[WindowCall] = IndexedSeq.empty
):
  private val calls = mutable.ArrayBuffer.from(known)

  /** The collector after the first pass: binding an unknown call fails */
  def frozen: WindowCollector = WindowCollector(input, true, calls.toIndexedSeq)

  /** The collected calls over another input (e.g., the aggregation built after the first pass) */
  def rebase(schema: Schema): WindowCollector = WindowCollector(schema, false, calls.toIndexedSeq)

  /** The registered calls, in the order of their output columns */
  def windowCalls: IndexedSeq[WindowCall] = calls.toIndexedSeq

  /** The output of the windows: the input columns, then one column per call */
  def schema: Schema = Schema(
    input.fields ++ calls.zipWithIndex.map((c, i) => Field(s"__wv_win${i}", c.ptype))
  )

  /** The window operator over `op`, whose schema is the collector's input */
  def build(op: Operator): Operator = WindowOp(op, calls.toIndexedSeq, schema)

  def ref(binder: Binder, e: Expression): VExpr =
    val call = toCall(binder, e)
    val i    = calls.indexWhere(_.sig == call.sig)
    val idx  =
      if i >= 0 then
        i
      else if isFrozen then
        throw StatusCode.INVALID_ARGUMENT.newException(s"Unknown window call: ${call.sig}")
      else
        calls += call
        calls.size - 1
    ColRef(input.size + idx, call.ptype, s"__wv_win${idx}")

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
    // Bind against the window input (the first pass scope is the input schema). Over aggregated
    // rows, the aggregation binder maps keys and aggregate calls to the aggregation output
    val b =
      if binder.aggContext.isDefined then
        binder
      else
        Binder(binder.plannerRef, BindScope(input))
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
