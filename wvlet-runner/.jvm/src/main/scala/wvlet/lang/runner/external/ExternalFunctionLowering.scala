/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package wvlet.lang.runner.external

import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.MethodSymbolInfo
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.analyzer.FunctionInliner
import wvlet.lang.ext.ScalarFunction
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.NamedType
import wvlet.lang.model.DataType.SchemaType
import wvlet.lang.model.RelationType
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*
import wvlet.uni.log.LogSupport

/**
  * Lowers calls of code-backed scalar functions (`def f(x: string): string = native` with an
  * implementation in the function registry) that appear inside expressions. Engines cannot call out
  * to user code, so each call is moved below the operator that uses it:
  *
  * {{{
  *   Filter[geocode(address) != ''](C)
  *     =>
  *   Exclude[__wv_fn_1](
  *     Filter[__wv_fn_1 != ''](
  *       ExternalApply[geocode -> __wv_fn_1](Add[address as __wv_arg_1_0](C))))
  * }}}
  *
  * The engine computes the argument expressions as helper columns, the runner evaluates the
  * function once per row ([[ExternalApply]] in scalar-map shape), and the original call becomes a
  * column reference. Nested calls lower inside-out over repeated passes.
  */
object ExternalFunctionLowering extends LogSupport:
  private val maxPasses = 100

  def lower(relation: Relation, registry: ExternalFunctionRegistry)(using ctx: Context): Relation =
    if registry.isEmpty then
      relation
    else
      var counter       = 0
      def nextId(): Int =
        counter += 1
        counter

      // Each pass lowers the innermost calls of every operator; nested calls take further passes
      def hasCalls(r: Relation): Boolean =
        var found = false
        r.traverse {
          case op: UnaryRelation if containsCall(op, registry) =>
            found = true
        }
        found

      var current = relation
      var passes  = 0
      while hasCalls(current) do
        passes += 1
        if passes > maxPasses then
          throw StatusCode
            .UNEXPECTED_STATE
            .newException("External function lowering did not converge")
        current = current
          .transformUp {
            case op: UnaryRelation if containsCall(op, registry) =>
              lowerOperator(op, registry, () => nextId())
          }
          .asInstanceOf[Relation]
      current

  // A scalar external call whose arguments contain no further external call (innermost first)
  private def isLowerable(e: Expression, registry: ExternalFunctionRegistry)(using
      ctx: Context
  ): Boolean =
    e match
      case fa: FunctionApply if fa.window.isEmpty =>
        scalarFunction(fa, registry).isDefined && !fa.args.exists(a => containsCall(a, registry))
      case _ =>
        false

  private def containsCall(e: Expression, registry: ExternalFunctionRegistry)(using
      ctx: Context
  ): Boolean =
    var found = false
    e.traverseExpressions {
      case fa: FunctionApply if scalarFunction(fa, registry).isDefined =>
        found = true
    }
    found

  private def containsCall(op: UnaryRelation, registry: ExternalFunctionRegistry)(using
      ctx: Context
  ): Boolean = op.childExpressions.exists(containsCall(_, registry))

  // The return type of the `def ... = native` scalar function called here, if it has an
  // implementation in the registry
  private def scalarFunction(fa: FunctionApply, registry: ExternalFunctionRegistry)(using
      ctx: Context
  ): Option[(String, DataType)] =
    fa.base match
      case id: Identifier =>
        val name = id.leafName
        ctx
          .findSymbolByName(Name.termName(name))
          .map(_.symbolInfo)
          .collect {
            case m: MethodSymbolInfo
                if m.body.exists(_.isInstanceOf[NativeExpression]) &&
                  FunctionInliner.externalReturnType(m.ft.returnType).isEmpty =>
              m.ft.returnType
          }
          .filter { _ =>
            registry.find(name) match
              case Some(ExternalFunctionImpl.Jvm(_: ScalarFunction)) |
                  Some(_: ExternalFunctionImpl.NodeModule) =>
                true
              case _ =>
                false
          }
          .map(name -> _)
      case _ =>
        None

  private def lowerOperator(
      op: UnaryRelation,
      registry: ExternalFunctionRegistry,
      nextId: () => Int
  )(using ctx: Context): Relation =
    op match
      case _: Project | _: Filter | _: AddColumnsToRelation =>
      case other                                            =>
        throw StatusCode
          .NOT_IMPLEMENTED
          .newException(
            s"External functions are supported in select, add, and where; compute the value with `add` first and use the column in ${other
                .nodeName}",
            other.sourceLocation
          )

    var input: Relation = op.child
    val outputs         = List.newBuilder[String]
    // Identical calls within the operator share one evaluation
    val lowered = scala.collection.mutable.Map.empty[FunctionApply, String]

    val rewritten = op.transformChildExpressions { case e: Expression =>
      e.transformUpExpression {
        case fa: FunctionApply if isLowerable(fa, registry) =>
          val out = lowered.getOrElseUpdate(
            fa, {
              val (name, retType) = scalarFunction(fa, registry).get
              val id              = nextId()
              val out             = s"__wv_fn_${id}"
              val argColumns      = fa.args.indices.map(i => s"__wv_arg_${id}_${i}").toList
              val withArgs        =
                if fa.args.isEmpty then
                  input
                else
                  AddColumnsToRelation(
                    input,
                    fa.args
                      .zip(argColumns)
                      .map { (arg, col) =>
                        SingleColumn(DoubleQuotedIdentifier(col, arg.span), arg.value, arg.span)
                      },
                    fa.span
                  )
              val schema = SchemaType(
                None,
                Name.typeName(RelationType.newRelationTypeName),
                input.relationType.fields :+ NamedType(Name.termName(out), retType)
              )
              input = ExternalApply(
                withArgs,
                Name.termName(name),
                Nil,
                NativeExpression(name, Some(retType), fa.span),
                schema,
                fa.span,
                outputColumn = Some(out),
                argColumns = argColumns
              )
              outputs += out
              out
            }
          )
          DoubleQuotedIdentifier(out, fa.span)
      }
    }

    // Operators passing every input column through would leak the helper result columns
    val (result: Relation, leaks: Boolean) =
      rewritten match
        case p: Project =>
          (p.copy(child = input), p.selectItems.exists(_.isInstanceOf[AllColumns]))
        case f: Filter =>
          (f.copy(child = input), true)
        case a: AddColumnsToRelation =>
          (a.copy(child = input), true)
        case other =>
          throw StatusCode.UNEXPECTED_STATE.newException(s"Unexpected operator: ${other.nodeName}")
    if leaks then
      ExcludeColumnsFromRelation(
        result,
        outputs.result().map(c => DoubleQuotedIdentifier(c, op.span)),
        op.span
      )
    else
      result

  end lowerOperator

end ExternalFunctionLowering
