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
package wvlet.lang.engine.logical

import wvlet.lang.api.StatusCode

/**
  * Checks the invariants of a [[LogicalPlan]] that passes and [[Lowering]] rely on: every column an
  * expression reads is an output of the node's input, with the same type, and no node outputs a
  * column id twice. It runs between passes in tests and debugging
  * (`OptimizerOptions.validatePlans`)
  */
object PlanValidator:
  import LogicalPlan.*

  /** Throws an INVALID_ARGUMENT error describing the first violation */
  def validate(p: LogicalPlan): Unit = violations(p)
    .headOption
    .foreach { v =>
      throw StatusCode.INVALID_ARGUMENT.newException(s"Invalid logical plan: ${v}\n${p.explain}")
    }

  /** Every violation, from the leaves up */
  def violations(p: LogicalPlan): Seq[String] =
    val b                          = Seq.newBuilder[String]
    def loop(n: LogicalPlan): Unit =
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
        case Filter(child, cond) =>
          checkExprs(b, "Filter", Seq(cond), child)
        case Project(child, exprs, columns) =>
          if exprs.size != columns.size then
            b += s"Project has ${exprs.size} expressions for ${columns.size} columns"
          checkExprs(b, "Project", exprs, child)
        case Sort(child, keys, _) =>
          checkExprs(b, "Sort", keys.map(_.expr), child)
        case Limit(_, limit, offset) =>
          if limit < 0 || offset < 0 then
            b += s"Limit(${limit}, offset=${offset}) is negative"
        case Legacy(op, columns) =>
          if columns.map(_.ptype) != op.schema.types then
            b += s"Legacy[${op.name}] columns ${columns.mkString(", ")} differ from ${op.schema}"
      end match
    end loop
    loop(p)
    b.result()

  end violations

  private def checkExprs(
      b: collection.mutable.Builder[String, Seq[String]],
      node: String,
      exprs: Seq[BExpr],
      input: LogicalPlan
  ): Unit =
    val byId = input.output.map(c => c.id.value -> c).toMap
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
