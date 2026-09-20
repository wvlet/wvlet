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
import wvlet.lang.model.expr.*
import wvlet.uni.json.JSON

/**
  * Converts the literal argument expressions of a tool call or an external function call into
  * JSON values
  */
object LiteralArgs:
  /**
    * @param callee
    *   name of the tool or function, for error messages
    */
  def toJson(callee: String, e: Expression): JSON.JSONValue =
    def notLiteral(found: Expression): Nothing =
      throw StatusCode
        .INVALID_ARGUMENT
        .newException(s"'${callee}' arguments must be literal values, found: ${found}")
    e match
      case s: StringLiteral =>
        JSON.JSONString(s.unquotedValue)
      case l: LongLiteral =>
        JSON.JSONLong(l.value)
      case d: DoubleLiteral =>
        JSON.JSONDouble(d.value)
      case d: DecimalLiteral =>
        JSON.JSONDouble(d.value.toDouble)
      case _: TrueLiteral =>
        JSON.JSONBoolean(true)
      case _: FalseLiteral =>
        JSON.JSONBoolean(false)
      case _: NullLiteral =>
        JSON.JSONNull()
      // Negative (or explicitly signed) numbers parse as a unary expression over the literal
      case a: ArithmeticUnaryExpr =>
        val negate = a.sign == Sign.Negative
        toJson(callee, a.child) match
          case JSON.JSONLong(v) =>
            JSON.JSONLong(
              if negate then
                -v
              else
                v
            )
          case JSON.JSONDouble(v) =>
            JSON.JSONDouble(
              if negate then
                -v
              else
                v
            )
          case _ =>
            notLiteral(e)
      case p: ParenthesizedExpression =>
        toJson(callee, p.child)
      case l: Literal =>
        JSON.JSONString(l.stringValue)
      case other =>
        notLiteral(other)

end LiteralArgs
