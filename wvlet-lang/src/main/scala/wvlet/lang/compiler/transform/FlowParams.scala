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
package wvlet.lang.compiler.transform

import wvlet.lang.api.StatusCode
import wvlet.lang.model.DataType
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*

/**
  * Binds run-time flow arguments (`run flow F(segment = 'a')`, `wvlet flow run "F(segment = 'a')"`)
  * to the parameters declared on the flow definition (`flow F(segment: string) = ...`).
  *
  * Binding resolves positional and named arguments against `FlowDef.params`, fills in declared
  * default values, and rejects unknown, duplicate, missing, or mistyped arguments with clear
  * errors. Substitution then replaces parameter references in the stage bodies with the bound
  * expressions, so that the lowered stage SQL contains concrete values. Parameter names shadow
  * columns of the same name inside stage bodies (the same semantics as model arguments), while
  * structural names (table/stage references in `from`/`merge`, route targets, and `->` jump
  * targets) are never treated as parameter references.
  */
object FlowParams:

  /**
    * Resolve the given arguments against the flow's declared parameters, returning the complete
    * parameter-name-to-expression binding with defaults applied. Throws INVALID_ARGUMENT with a
    * descriptive message for unknown/duplicate/missing/mistyped arguments
    */
  def bind(flow: FlowDef, args: List[FunctionArg]): Map[String, Expression] =
    val flowName                   = flow.name.name
    def fail(msg: String): Nothing = throw StatusCode.INVALID_ARGUMENT.newException(msg)
    def signature: String          =
      if flow.params.isEmpty then
        s"flow ${flowName} takes no parameters"
      else
        s"flow ${flowName}(${flow
            .params
            .map(p => s"${p.name.name}: ${p.givenDataType.typeDescription}")
            .mkString(", ")})"

    if args.size > flow.params.size then
      fail(s"Too many arguments for flow '${flowName}': ${args.size} given, but ${signature}")

    var bound     = Map.empty[String, Expression]
    var seenNamed = false
    args
      .zipWithIndex
      .foreach { (arg, index) =>
        arg.name match
          case Some(n) =>
            seenNamed = true
            if !flow.params.exists(_.name.name == n.name) then
              fail(s"Unknown parameter '${n.name}' for flow '${flowName}': ${signature}")
            if bound.contains(n.name) then
              fail(s"Duplicate argument for parameter '${n.name}' of flow '${flowName}'")
            bound += n.name -> arg.value
          case None =>
            if seenNamed then
              fail(
                s"Positional argument after a named argument in the arguments of flow '${flowName}'"
              )
            bound += flow.params(index).name.name -> arg.value
      }

    flow
      .params
      .map { p =>
        bound.get(p.name.name) match
          case Some(v) =>
            if !typeCompatible(p.givenDataType, v) then
              fail(
                s"Parameter '${p.name.name}' of flow '${flowName}' expects ${p
                    .givenDataType
                    .typeDescription}, but got ${v.dataTypeName}"
              )
            p.name.name -> v
          case None =>
            p.defaultValue match
              case Some(d) =>
                p.name.name -> d
              case None =>
                fail(
                  s"Missing argument for parameter '${p.name.name}: ${p
                      .givenDataType
                      .typeDescription}' of flow '${flowName}'"
                )
      }
      .toMap
  end bind

  /**
    * Replace parameter references in the stage bodies of the flow with the bound expressions.
    * Structural name positions (from/merge sources, route targets, jump targets, fork stage
    * metadata) are left untouched
    */
  def substitute(flow: FlowDef, bindings: Map[String, Expression]): FlowDef =
    if bindings.isEmpty then
      flow
    else
      val rule: PartialFunction[Expression, Expression] =
        case i: Identifier if bindings.contains(i.leafName) =>
          bindings(i.leafName)

      val newStages = flow
        .stages
        .map { s =>
          s.body.map(b => substituteInBody(b, rule)) match
            case Some(nb) if s.body.exists(b => !(nb eq b)) =>
              val ns = s.copy(body = Some(nb))
              ns.copyMetadataFrom(s)
              ns
            case _ =>
              s
        }
      val nf = flow.copy(stages = newStages)
      nf.copyMetadataFrom(flow)
      nf

  /**
    * Bind the arguments and substitute parameter references in one step, returning the flow ready
    * for lowering and execution
    */
  def apply(flow: FlowDef, args: List[FunctionArg]): FlowDef = substitute(flow, bind(flow, args))

  private def substituteInBody(
      body: Relation,
      rule: PartialFunction[Expression, Expression]
  ): Relation = body
    .transformUp {
      case t: TableRef =>
        // A `from <name>` source is a table or stage reference, never a parameter
        t
      case m: FlowMerge =>
        // Merge sources are stage names
        m
      case j: FlowJump =>
        // `-> Flow` targets are flow names
        j
      case s: StageDef =>
        // A fork-nested stage: its body has already been transformed by the recursion; its
        // inputRefs/trigger reference stage names and must not be substituted
        s
      case r: FlowRoute =>
        // Substitute inside routing predicates only; case targets and elseTarget are stage names
        val newBy    = r.byExpr.map(_.transformUpExpression(rule))
        val newCases = r
          .cases
          .map(c => c.copy(condition = c.condition.map(_.transformUpExpression(rule))))
        val changed =
          r.byExpr.zip(newBy).exists((a, b) => !(a eq b)) ||
            r.cases
              .zip(newCases)
              .exists((a, b) => a.condition.zip(b.condition).exists((x, y) => !(x eq y)))
        if changed then
          val nr = r.copy(byExpr = newBy, cases = newCases)
          nr.copyMetadataFrom(r)
          nr
        else
          r
      case p: LogicalPlan =>
        p.transformChildExpressions { case e: Expression =>
          e.transformUpExpression(rule)
        }
    }
    .asInstanceOf[Relation]

  /**
    * A lightweight compatibility check between a declared parameter type and a literal argument.
    * Non-literal arguments and non-primitive declared types are accepted as-is
    */
  private def typeCompatible(declared: DataType, value: Expression): Boolean =
    value match
      case _: NullLiteral =>
        true
      case l: Literal =>
        declared match
          case DataType.StringType | DataType.VarcharType(_) | DataType.CharType(_) =>
            l.isInstanceOf[StringLiteral]
          case n if n.isNumeric =>
            l.dataType.isNumeric
          case DataType.BooleanType =>
            l.isInstanceOf[BooleanLiteral]
          case _ =>
            true
      case _ =>
        true

end FlowParams
