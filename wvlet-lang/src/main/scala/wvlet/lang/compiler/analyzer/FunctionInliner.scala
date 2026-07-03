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
package wvlet.lang.compiler.analyzer

import wvlet.lang.api.StatusCode
import wvlet.lang.compiler.Context
import wvlet.lang.compiler.ContextLogSupport
import wvlet.lang.compiler.MethodSymbolInfo
import wvlet.lang.compiler.MultipleSymbolInfo
import wvlet.lang.compiler.Name
import wvlet.lang.compiler.PartialQuerySymbolInfo
import wvlet.lang.compiler.Symbol
import wvlet.lang.compiler.TermName
import wvlet.lang.compiler.TypeName
import wvlet.lang.compiler.TypeSymbolInfo
import wvlet.lang.compiler.ContextUtil.*
import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.TypeParameter
import wvlet.lang.model.DataType.UnknownType
import wvlet.lang.model.DataType.VarArgType
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.*

/**
  * Stack-based inlining of function bodies and partial queries with cycle detection and depth
  * guards.
  *
  * The active inlining path is threaded explicitly as a stack of symbols (innermost first) because
  * callers may run inside traversals that do not push per-frame Contexts.
  */
object FunctionInliner extends ContextLogSupport:

  private def lookupType(name: Name, context: Context): Option[Symbol] = context.findSymbolByName(
    name
  )

  /**
    * The maximum depth of nested function/partial-query inlining, used as a safety net against
    * runaway expansion. Mirrors the model expansion depth limit in GenSQL.
    */
  inline val maxInlineExpansionDepth = 100

  /**
    * Find a corresponding MethodSymbolInfo for the given function expression
    * @param f
    * @param context
    * @return
    */
  def findFunctionDef(f: Expression, knownArgs: List[FunctionArg] = Nil)(using
      context: Context
  ): Option[MethodSymbolInfo] =

    f match
      case fa: FunctionApply =>
        findFunctionDef(fa.base, fa.args)
      case i: Identifier =>
        lookupType(i.toTermName, context)
          .map(_.symbolInfo)
          .collect {
            case m: MethodSymbolInfo =>
              m
            case m: MultipleSymbolInfo =>
              // TODO resolve one of the function type
              throw StatusCode
                .SYNTAX_ERROR
                .newException(s"Ambiguous function call for ${i}", context.sourceLocationAt(i.span))
          }
      case d @ DotRef(qual, method: Identifier, _, _) =>
        val methodName = method.toTermName
        val qualType   =
          if qual.dataType.isResolved then
            qual.dataType
          else
            // The new Typer assigns types to the tpe field instead of rewriting nodes with
            // resolved dataTypes, so fall back to tpe before giving up
            qual.tpe match
              case dt: DataType if dt.isResolved =>
                dt
              case _ =>
                DataType.AnyType

        def memberOf(baseType: DataType): Option[MethodSymbolInfo] = lookupType(
          baseType.typeName,
          context
        ).map { sym =>
            // TODO: Resolve member with different arg types
            sym.symbolInfo.findMember(methodName).symbolInfo
          }
          .collect {
            case m: MethodSymbolInfo =>
              m
            case m: MultipleSymbolInfo =>
              // TODO resolve one of the function type
              throw StatusCode
                .SYNTAX_ERROR
                .newException(
                  s"Ambiguous function call for ${method}",
                  context.sourceLocationAt(d.span)
                )
          }
          .map { mi =>
            // Find the owner type of the method to build generic type mappings
            lookupType(mi.owner.dataType.typeName, context).map(_.symbolInfo) match
              case Some(ownerType: TypeSymbolInfo) =>
                val typeMap: Map[TypeName, DataType] = ownerType
                  .typeParams
                  .zipAll(baseType.typeParams, UnknownType, UnknownType)
                  .collect { case (t1: TypeParameter, t2) =>
                    t1.typeName -> t2
                  }
                  .toMap[TypeName, DataType]
                if typeMap.isEmpty then
                  mi
                else
                  val bounded = mi.bind(typeMap)
                  context.logTrace(s"Bind ${mi} with typeMap: ${typeMap} => ${bounded}")
                  bounded
              case _ =>
                mi
          }
          .map { m =>
            context.logTrace(s"Resolved method ${baseType}.${methodName} => ${m}")
            m
          }

        // Retry with the any type when the method is not a member of the qualifier's type: with
        // an unresolved qualifier this lookup has always consulted the any type, so typing the
        // qualifier (as the new Typer does) must not narrow what could be resolved before
        val m: Option[MethodSymbolInfo] = memberOf(qualType).orElse {
          if qualType != DataType.AnyType then
            memberOf(DataType.AnyType)
          else
            None
        }

        if m.isEmpty then
          context.logTrace(s"Failed to find function `${methodName}` for ${qual}:${qual.dataType}")

        m
      case _ =>
        trace(s"Failed to find function definition for ${f}")
        None
    end match
  end findFunctionDef

  /**
    * Find a native (compile-time evaluated) function body for the given unresolved identifier name,
    * e.g. ulid_string
    */
  def findNativeFunction(context: Context, name: String): Option[NativeExpression] = context
    .findTermSymbolByName(name)
    // A symbol whose lazy completion is in progress (e.g., a self-referencing model) cannot be a
    // native function; skip it instead of re-forcing its completer
    .filterNot(_.isCompleting)
    .map(_.symbolInfo)
    .collect { case m: MethodSymbolInfo =>
      m.body
    }
    .flatten
    .collect { case n: NativeExpression =>
      n
    }

  /**
    * Inline the body of the function definition, substituting `this` and function arguments.
    * `activeFunctions` tracks the function symbols currently being inlined (innermost first) so
    * that recursive references are reported as user errors instead of looping forever.
    */
  def inlineFunctionBody(
      base: Expression,
      m: MethodSymbolInfo,
      resolvedArgs: List[(TermName, Expression)],
      activeFunctions: List[Symbol] = Nil
  )(using context: Context): Expression =
    val fnSym = m.symbol
    if !fnSym.isNoSymbol && activeFunctions.exists(_ eq fnSym) then
      val cyclePath = (activeFunctions.reverse.map(_.name.name) :+ m.name.name).mkString(" -> ")
      throw StatusCode
        .RECURSIVE_FUNCTION_REFERENCE
        .newException(
          s"Recursive function reference detected: ${cyclePath}",
          context.sourceLocationAt(base.span)
        )
    if activeFunctions.size >= maxInlineExpansionDepth then
      // A safety net distinct from RECURSIVE_FUNCTION_REFERENCE: the eq-based check above already
      // catches every cycle, so this only fires for a legitimately deep chain of distinct functions
      throw StatusCode
        .INLINE_EXPANSION_LIMIT_EXCEEDED
        .newException(
          s"Function inlining for ${m.name} exceeded the maximum depth ${maxInlineExpansionDepth}",
          context.sourceLocationAt(base.span)
        )

    val newExpr: Expression =
      m.body match
        case Some(methodBody) =>
          // Substitute-first: bind `this` and the arguments resolved in the caller's frame
          val substituted = methodBody.transformUpExpression {
            case th: This =>
              base match
                case d: DotRef =>
                  // Replace ${this} with ${qual} in DotRef(qual, method)
                  d.qualifier
                case _ =>
                  th
            case i: Identifier =>
              // Replace function arguments to the resolved args
              resolvedArgs.find(_._1 == i.toTermName) match
                case Some((_, expr)) =>
                  expr
                case None =>
                  i
          }
          // Then inline function calls nested in the body, with this function on the active path
          // so that self-referential or mutually recursive definitions are detected. This must be
          // a top-down traversal: a bottom-up one would visit the base DotRef of a member call
          // like obj.method(args) before its enclosing FunctionApply and inline the method body
          // without binding the arguments
          val nextActive =
            if fnSym.isNoSymbol then
              activeFunctions
            else
              fnSym :: activeFunctions
          substituted.transformExpression {
            case f: FunctionApply =>
              resolveFunctionApply(f, nextActive)
            case d: DotRef =>
              findFunctionDef(d) match
                case Some(mi: MethodSymbolInfo) =>
                  inlineFunctionBody(d, mi, Nil, nextActive)
                case _ =>
                  d
          }
        case None =>
          // If the function definition has no body expression, return the original expression
          base
    context.logTrace(s"Resolving ${base} => ${newExpr}: ${m.ft.returnType}")
    if newExpr.resolved || !m.ft.returnType.isResolved then
      newExpr
    else
      newExpr.withDataType(m.ft.returnType)

  end inlineFunctionBody

  /**
    * Inline a function application if a corresponding function definition is found; otherwise
    * return the FunctionApply unchanged
    */
  def resolveFunctionApply(f: FunctionApply, activeFunctions: List[Symbol] = Nil)(using
      context: Context
  ): Expression =
    findFunctionDef(f) match
      case Some(m: MethodSymbolInfo) =>
        val functionArgTypes = m.ft.args
        // Mapping function arguments aligned to the function definition
        var index        = 0
        val resolvedArgs = List.newBuilder[(TermName, Expression)]

        def mapArg(args: List[FunctionArg]): Unit =
          if !args.isEmpty then
            args.head match
              case FunctionArg(None, expr, _, _, span) =>
                if index >= functionArgTypes.length then
                  throw StatusCode
                    .SYNTAX_ERROR
                    .newException("Too many arguments", context.sourceLocationAt(span))
                val argType = functionArgTypes(index)
                argType.dataType match
                  case VarArgType(elemType) =>
                    index += 1
                    resolvedArgs += argType.name -> ListExpr(args, span)
                  // all args are consumed
                  case _ =>
                    index += 1
                    resolvedArgs += argType.name -> expr
                    mapArg(args.tail)
              case FunctionArg(Some(argName), expr, _, _, span) =>
                functionArgTypes.find(_.name == argName) match
                  case Some(argType) =>
                    argType.dataType match
                      case VarArgType(elemType) =>
                        resolvedArgs += argName -> ListExpr(args, expr.span)
                      // all args are consumed
                      case _ =>
                        resolvedArgs += argName -> expr
                        mapArg(args.tail)
                  case None =>
                    throw StatusCode
                      .SYNTAX_ERROR
                      .newException(
                        s"Unknown argument name: ${argName}",
                        context.sourceLocationAt(span)
                      )

        // Resolve function arguments
        mapArg(f.args)

        // Resolve identifiers in the function body with the given function arguments
        val expr = inlineFunctionBody(f.base, m, resolvedArgs.result(), activeFunctions)

        f.window match
          case Some(w) =>
            WindowApply(expr.withDataType(m.ft.returnType), w, None, expr.span)
          case None =>
            expr.withDataType(m.ft.returnType)
      case _ =>
        f
    end match
  end resolveFunctionApply

  /**
    * Inline the body of the partial query referenced by the given PartialQueryApply.
    * `activeQueries` tracks the partial-query symbols currently being inlined (innermost first) so
    * that recursive references are reported as user errors instead of looping forever.
    */
  def resolvePartialQuery(p: PartialQueryApply, activeQueries: List[Symbol] = Nil)(using
      context: Context
  ): Relation =
    val partialQueryName = Name.termName(p.partialQueryRef.leafName)
    lookupType(partialQueryName, context) match
      case Some(sym) =>
        sym.symbolInfo match
          case pqInfo: PartialQuerySymbolInfo =>
            if activeQueries.exists(_ eq sym) then
              val cyclePath = (activeQueries.reverse.map(_.name.name) :+ partialQueryName.name)
                .mkString(" -> ")
              throw StatusCode
                .RECURSIVE_PARTIAL_QUERY_REFERENCE
                .newException(
                  s"Recursive partial query reference detected: ${cyclePath}",
                  context.sourceLocationAt(p.span)
                )
            if activeQueries.size >= maxInlineExpansionDepth then
              // A safety net distinct from RECURSIVE_PARTIAL_QUERY_REFERENCE: the eq-based check
              // above already catches every cycle, so this only fires for a legitimately deep
              // chain of distinct partial queries
              throw StatusCode
                .INLINE_EXPANSION_LIMIT_EXCEEDED
                .newException(
                  s"Partial query inlining for ${partialQueryName} exceeded the maximum depth ${maxInlineExpansionDepth}",
                  context.sourceLocationAt(p.span)
                )

            // Check for argument count mismatch
            if pqInfo.params.length != p.args.length then
              throw StatusCode
                .INVALID_ARGUMENT
                .newException(
                  s"Partial query '${partialQueryName}' expects ${pqInfo
                      .params
                      .length} arguments, but ${p.args.length} were provided",
                  context.sourceLocationAt(p.span)
                )

            // Inline the partial query body
            val body = pqInfo.body

            // Substitute parameters if any. Arguments were already resolved in the caller's
            // frame by the bottom-up traversal, so substitute-first keeps caller-side scoping
            val substitutedBody =
              if pqInfo.params.nonEmpty then
                substituteParams(body, pqInfo.params, p.args)
              else
                body

            // Replace EmptyRelation in the body with the input relation
            val inlinedBody = inlinePartialQueryBody(p.child, substitutedBody)

            // Inline partial queries nested in the body, with this partial query on the active
            // path so that self-referential or mutually recursive definitions are detected
            inlinedBody
              .transformUp { case np: PartialQueryApply =>
                resolvePartialQuery(np, sym :: activeQueries)
              }
              .asInstanceOf[Relation]
          case _ =>
            // Not a partial query, keep as is (might be an error)
            context.logWarn(s"'${partialQueryName}' is not a partial query definition")
            p
      case None =>
        context.logWarn(s"Partial query '${partialQueryName}' not found")
        p
    end match
  end resolvePartialQuery

  /**
    * Replace EmptyRelation nodes in the partial query body with the input relation.
    */
  private def inlinePartialQueryBody(input: Relation, body: Relation): Relation = body
    .transformUp { case e: EmptyRelation =>
      input
    }
    .asInstanceOf[Relation]

  /**
    * Substitute parameter references in the partial query body with actual argument values.
    */
  private def substituteParams(
      body: Relation,
      params: List[DefArg],
      args: List[Expression]
  ): Relation =
    // Create a mapping from parameter names to argument values
    val paramMap =
      params
        .zip(args)
        .map { case (param, arg) =>
          param.name -> arg
        }
        .toMap

    // Replace identifier references to parameters with their argument values
    body
      .transformUpExpressions {
        case i: Identifier if paramMap.contains(Name.termName(i.leafName)) =>
          paramMap(Name.termName(i.leafName))
      }
      .asInstanceOf[Relation]

end FunctionInliner
