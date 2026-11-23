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

import wvlet.lang.api.StatusCode

/**
  * Checks the invariants of a [[RuntimePlan]] that passes and [[Lowering]] rely on: every column an
  * expression reads is an output of the node's input, with the same type, and no node outputs a
  * column id twice. It runs between passes in tests and debugging
  * (`OptimizerOptions.validatePlans`)
  */
object PlanValidator:
  import RuntimePlan.*

  /** Throws an INVALID_ARGUMENT error describing the first violation */
  def validate(p: RuntimePlan): Unit = violations(p)
    .headOption
    .foreach { v =>
      throw StatusCode.INVALID_ARGUMENT.newException(s"Invalid runtime plan: ${v}\n${p.explain}")
    }

  /** Every violation, from the leaves up */
  def violations(p: RuntimePlan): Seq[String] =
    val b                          = Seq.newBuilder[String]
    def loop(n: RuntimePlan): Unit =
      n.children.foreach(loop)
      val out = n.output.map(_.id)
      if out.distinct.size != out.size then
        b += s"duplicate output column ids in ${n.output.mkString(", ")}"
      n match
        case Scan(source, columns) =>
          if columns.size != source.schema.size then
            b += s"Scan[${source.name}] has ${columns.size} columns for ${source.schema.size}"
        case Values(columns, rows) =>
          if columns.size != rows.columns.length then
            b += s"Values has ${columns.size} columns for ${rows.columns.length}"
        case Filter(child, cond, _) =>
          checkExprs(b, "Filter", Seq(cond), child.output)
        case Project(child, exprs, columns) =>
          if exprs.size != columns.size then
            b += s"Project has ${exprs.size} expressions for ${columns.size} columns"
          checkExprs(b, "Project", exprs, child.output)
        case Sort(child, keys, _) =>
          checkExprs(b, "Sort", keys.map(_.expr), child.output)
        case Limit(_, limit, offset) =>
          if limit < 0 || offset < 0 then
            b += s"Limit(${limit}, offset=${offset}) is negative"
        case j @ Join(left, right, _, lk, rk, residual, _) =>
          if lk.size != rk.size then
            b += s"Join has ${lk.size} left keys for ${rk.size} right keys"
          checkExprs(b, "Join", lk, left.output)
          checkExprs(b, "Join", rk, right.output)
          checkExprs(b, "Join", residual.toSeq, j.output)
        case Aggregate(child, keys, calls, columns) =>
          if columns.size != keys.size + calls.size then
            b +=
              s"Aggregate has ${columns.size} columns for ${keys.size} keys and ${calls.size} calls"
          checkExprs(b, "Aggregate", keys ++ calls.flatMap(_.exprs), child.output)
        case j @ InnerJoins(inputs, conds) =>
          if inputs.size < 2 then
            b += s"InnerJoins has ${inputs.size} inputs"
          checkExprs(b, "InnerJoins", conds.map(_.cond), j.output)
        case Union(inputs, columns) =>
          inputs.foreach(i => checkWidth(b, "Union", i, columns))
        case SetOp(left, right, _, _, columns) =>
          Seq(left, right).foreach(i => checkWidth(b, "SetOp", i, columns))
        case Distinct(_)                   =>
        case Window(child, calls, columns) =>
          if columns.size != calls.size then
            b += s"Window has ${columns.size} columns for ${calls.size} calls"
          else if columns.map(_.ptype) != calls.map(_.ptype) then
            b += s"Window columns ${columns.mkString(", ")} differ from the call types"
          checkExprs(b, "Window", calls.flatMap(_.exprs), child.output)
        case Legacy(op, columns) =>
          if columns.map(_.ptype) != op.schema.types then
            b += s"Legacy[${op.name}] columns ${columns.mkString(", ")} differ from ${op.schema}"
        case CTERef(cte, columns) =>
          if columns.map(_.ptype) != cte.body.output.map(_.ptype) then
            b +=
              s"CTERef[${cte.name}] columns ${columns.mkString(", ")} differ from ${cte
                  .body
                  .output
                  .mkString(", ")}"
      end match
    end loop
    loop(p)
    b.result()

  end violations

  private def checkWidth(
      b: collection.mutable.Builder[String, Seq[String]],
      node: String,
      input: RuntimePlan,
      columns: IndexedSeq[Column]
  ): Unit =
    if input.output.size != columns.size then
      b += s"${node} has an input of ${input.output.size} columns for ${columns.size}"

  private def checkExprs(
      b: collection.mutable.Builder[String, Seq[String]],
      node: String,
      exprs: Seq[BExpr],
      input: IndexedSeq[Column]
  ): Unit =
    val byId = input.map(c => c.id.value -> c).toMap
    exprs
      .flatMap(_.colRefs)
      .foreach { r =>
        byId.get(r.index) match
          case None =>
            b += s"${node} reads ${r.name}#${r.index}, which its input doesn't output"
          case Some(c) if c.ptype != r.ptype =>
            b += s"${node} reads ${c} as ${r.ptype}, but its type is ${c.ptype}"
          case _ =>
      }

end PlanValidator
