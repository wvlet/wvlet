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
      val newStages = flow
        .stages
        .map { s =>
          s.body.map(b => substituteInBody(b, bindings)) match
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

  /**
    * Collect the identifier instances of the body that occupy name positions rather than value
    * positions — table/stage/flow references, qualified-name members (`t.segment`), output aliases
    * (`select x as segment`, `as t(...)`), config keys, and lambda parameters. The substitution
    * rule skips these instances (matched by object identity), so a flow parameter sharing a name
    * with one of them never corrupts the plan structure. The walk follows the same product-based
    * recursion as `transformUpExpressions`, so nested plans inside expressions (subqueries) are
    * covered
    */
  private def collectProtectedNames(tree: Any): java.util.IdentityHashMap[Expression, Unit] =
    val protectedNames               = java.util.IdentityHashMap[Expression, Unit]()
    def protect(e: Expression): Unit = protectedNames.put(e, ())
    def walk(arg: Any): Unit         =
      arg match
        case e: Expression =>
          e match
            case d: DotRef =>
              protect(d.name)
            case s: SingleColumn =>
              protect(s.nameExpr)
            case a: Alias =>
              protect(a.nameExpr)
            case c: ConfigItem =>
              protect(c.key)
            case l: LambdaExpr =>
              l.args.foreach(protect)
            case _ =>
          e.productIterator.foreach(walk)
        case p: LogicalPlan =>
          p match
            case t: TableRef =>
              protect(t.name)
            case a: AliasedRelation =>
              protect(a.alias)
            case m: FlowMerge =>
              m.sources.foreach(protect)
            case j: FlowJump =>
              protect(j.targetFlow)
            case r: RunFlow =>
              protect(r.flowName)
            case r: FlowRoute =>
              r.cases.foreach(c => protect(c.target))
              r.elseTarget.foreach(protect)
            case s: StageDef =>
              s.inputRefs.foreach(protect)
              s.dependsOn.foreach(protect)
            case _ =>
          p.productIterator.foreach(walk)
        case Some(x) =>
          walk(x)
        case s: Seq[?] =>
          s.foreach(walk)
        case r: FlowRouteCase =>
          walk(r.condition)
        case _ =>
    walk(tree)
    protectedNames

  end collectProtectedNames

  /**
    * Substitute parameter references in a single expression tree with its own protected-name
    * collection. Identifiers are leaf nodes, so the bottom-up transform applies the rule to the
    * original instances and the identity-based protection stays valid
    */
  private def substituteExpr(e: Expression, bindings: Map[String, Expression]): Expression =
    val protectedNames = collectProtectedNames(e)
    e.transformUpExpression {
      case i: Identifier if bindings.contains(i.leafName) && !protectedNames.containsKey(i) =>
        bindings(i.leafName)
    }

  private def substituteInBody(body: Relation, bindings: Map[String, Expression]): Relation =
    val protectedNames                                = collectProtectedNames(body)
    val rule: PartialFunction[Expression, Expression] =
      case i: Identifier if bindings.contains(i.leafName) && !protectedNames.containsKey(i) =>
        bindings(i.leafName)

    // The generic expression pass runs first, while the collected identities are still valid:
    // identifiers are leaves, so the bottom-up transform applies the rule to the original
    // instances (any tree pass based on transformUp/mapChildren clones expression nodes
    // unconditionally via Expression.transformPlan and would invalidate the identity set)
    val generic = body.transformUpExpressions(rule).asInstanceOf[Relation]

    // FlowRouteCase is opaque to the generic plan/expression traversal (it is neither an
    // Expression nor a LogicalPlan), so routing predicates are substituted in a second pass,
    // re-collecting the protected names per condition expression
    var hasRoute = false
    generic.traverse { case _: FlowRoute =>
      hasRoute = true
    }
    if !hasRoute then
      generic
    else
      generic
        .transformUp { case r: FlowRoute =>
          val newCases = r
            .cases
            .map(c => c.copy(condition = c.condition.map(e => substituteExpr(e, bindings))))
          val changed = r
            .cases
            .zip(newCases)
            .exists((a, b) => a.condition.zip(b.condition).exists((x, y) => !(x eq y)))
          if changed then
            val nr = r.copy(cases = newCases)
            nr.copyMetadataFrom(r)
            nr
          else
            r
        }
        .asInstanceOf[Relation]

  end substituteInBody

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
