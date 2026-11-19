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
package wvlet.lang.compiler.typer

import wvlet.lang.model.DataType.*
import wvlet.lang.model.Type
import wvlet.lang.model.Type.ErrorType
import wvlet.lang.model.Type.FunctionType
import wvlet.lang.model.Type.NoType
import wvlet.lang.model.expr.BinaryExprType
import wvlet.lang.compiler.Name
import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.JSONArray
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.json.JSON.JSONString
import wvlet.uni.json.JSON.JSONValue
import wvlet.uni.test.UniTest

import scala.io.Source
import scala.util.Using

/**
  * Replays the traces of the Lean model of the operator typing rules (formal/, exported by `lake
  * exe operator-traces`) against the shipped [[TyperRules]]. The model proves that the result types
  * do not depend on operand order, that numeric arithmetic types as `unify`, that errors propagate,
  * and that untyped operands defer; this test checks that the Scala code computes what the model
  * computes.
  */
class OperatorTypingConformanceTest extends UniTest:

  private val fixture: Map[String, JSONValue] =
    val text =
      Using.resource(
        Source.fromInputStream(
          getClass.getResourceAsStream("/wvlet/lang/compiler/typer/operator-traces.json")
        )
      )(_.mkString)
    obj(JSON.parse(text))

  private def obj(v: JSONValue): Map[String, JSONValue] =
    v match
      case JSONObject(entries) =>
        entries.toMap
      case other =>
        fail(s"Expected a JSON object: ${other.toJSON}")

  private def arr(v: JSONValue): List[JSONValue] =
    v match
      case JSONArray(elems) =>
        elems.toList
      case other =>
        fail(s"Expected a JSON array: ${other.toJSON}")

  private def strOf(v: JSONValue): String =
    v match
      case JSONString(s) =>
        s
      case other =>
        fail(s"Expected a string: ${other.toJSON}")

  private def cases(name: String): List[Map[String, JSONValue]] = arr(fixture(name)).map(obj)

  extension (m: Map[String, JSONValue])
    private def str(key: String): String = strOf(m(key))

  private val modelError: Type = ErrorType("model error")

  private val types: Map[String, Type] = Map(
    "noType"         -> NoType,
    "null"           -> NullType,
    "int"            -> IntType,
    "long"           -> LongType,
    "float"          -> FloatType,
    "double"         -> DoubleType,
    "boolean"        -> BooleanType,
    "string"         -> StringType,
    "short"          -> ShortType,
    "date"           -> DateType,
    "decimal(10,2)"  -> DecimalType.of(10, 2),
    "decimal(20,4)"  -> DecimalType.of(20, 4),
    "decimal(30,0)"  -> DecimalType.of(30, 0),
    "decimal(38,10)" -> DecimalType.of(38, 10),
    "decimal(p,s)"   ->
      DecimalType(UnresolvedTypeParameter("p", None), UnresolvedTypeParameter("s", None)),
    "unresolved" -> UnresolvedType("x"),
    "function"   -> FunctionType(Name.termName("f"), Nil, IntType, Nil),
    "error"      -> modelError
  )

  private def tpe(name: String): Type = types.getOrElse(name, fail(s"Unknown model type ${name}"))

  /**
    * Results are compared by error-ness for errors, since the model abstracts messages, and
    * decimals by precision and scale, since results include decimals outside the operand sample
    */
  private def describe(t: Type): String =
    t match
      case _: ErrorType =>
        "error"
      case DecimalType(IntConstant(p), IntConstant(s)) =>
        s"decimal(${p},${s})"
      case other =>
        types
          .collectFirst {
            case (n, v) if v == other =>
              n
          }
          .getOrElse(s"unmapped ${other}")

  private val arithmeticOps: Map[String, BinaryExprType] =
    BinaryExprType.values.map(op => op.expr -> op).toMap

  private def replayPairs(section: String, label: String)(f: (Type, Type) => Type): Unit =
    val all = cases(section)
    all.size shouldBe types.size * types.size
    all.foreach { c =>
      val clue = s"${c.str("left")} ${label} ${c.str("right")}"
      (clue, describe(f(tpe(c.str("left")), tpe(c.str("right"))))) shouldBe (clue, c.str("result"))
    }

  test("the fixture covers every model type") {
    arr(fixture("types")).map(strOf).toSet shouldBe types.keySet
  }

  test("sign prefixes follow the model") {
    cases("unary").foreach { c =>
      val clue = s"-${c.str("operand")}"
      (clue, describe(TyperRules.unaryType(tpe(c.str("operand"))))) shouldBe (clue, c.str("result"))
    }
  }

  test("arithmetic follows the model") {
    val all = cases("arithmetic")
    all.size shouldBe arithmeticOps.size * types.size * types.size
    all.foreach { c =>
      val op   = arithmeticOps.getOrElse(c.str("op"), fail(s"Unknown operator ${c.str("op")}"))
      val clue = s"${c.str("left")} ${c.str("op")} ${c.str("right")}"
      (
        clue,
        describe(TyperRules.arithmeticType(op, tpe(c.str("left")), tpe(c.str("right"))))
      ) shouldBe (clue, c.str("result"))
    }
  }

  test("equality follows the model") {
    replayPairs("equality", "=")(TyperRules.equalityType)
  }

  test("ordering comparisons follow the model") {
    for op <- Seq("<", "<=", ">", ">=") do
      replayPairs("comparison", op)(TyperRules.comparisonType(op, _, _))
  }

  test("logical operators follow the model") {
    for op <- Seq("AND", "OR") do
      replayPairs("logical", op)(TyperRules.logicalType(op, _, _))
  }

end OperatorTypingConformanceTest
