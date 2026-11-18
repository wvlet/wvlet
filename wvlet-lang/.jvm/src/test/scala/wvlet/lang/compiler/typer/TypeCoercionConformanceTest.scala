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

import wvlet.lang.model.DataType
import wvlet.lang.model.DataType.*
import wvlet.lang.model.Type
import wvlet.lang.model.Type.ErrorType
import wvlet.lang.model.Type.NoType
import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.JSONArray
import wvlet.uni.json.JSON.JSONBoolean
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.json.JSON.JSONString
import wvlet.uni.json.JSON.JSONValue
import wvlet.uni.test.UniTest

import scala.io.Source
import scala.util.Using

/**
  * Replays the traces of the Lean model of the common-type and coercion rules (formal/, exported by
  * `lake exe typer-traces`) against the shipped [[TypeInference]]. The model proves the laws of
  * `unify` and `findCommonType` (commutative, associative, idempotent, least upper bound); this
  * test checks that the Scala code computes what the model computes.
  */
class TypeCoercionConformanceTest extends UniTest:

  private val fixture: Map[String, JSONValue] =
    val text =
      Using.resource(
        Source.fromInputStream(
          getClass.getResourceAsStream("/wvlet/lang/compiler/typer/typer-traces.json")
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

  private def cases(name: String): List[Map[String, JSONValue]] = arr(fixture(name)).map(obj)

  private def strOf(v: JSONValue): String =
    v match
      case JSONString(s) =>
        s
      case other =>
        fail(s"Expected a string: ${other.toJSON}")

  extension (m: Map[String, JSONValue])
    private def str(key: String): String = strOf(m(key))

    private def bool(key: String): Boolean =
      m(key) match
        case JSONBoolean(b) =>
          b
        case other =>
          fail(s"Expected a boolean at ${key}: ${other.toJSON}")

  /**
    * Scala types for the model's types. `error` is one fixed instance: the model abstracts the
    * message, and the Scala code compares types by equality.
    */
  private val modelError: Type = ErrorType("model error")

  private val types: Map[String, Type] = Map(
    "noType"        -> NoType,
    "null"          -> NullType,
    "int"           -> IntType,
    "long"          -> LongType,
    "float"         -> FloatType,
    "double"        -> DoubleType,
    "boolean"       -> BooleanType,
    "string"        -> StringType,
    "short"         -> ShortType,
    "date"          -> DateType,
    "decimal(10,2)" -> DecimalType.of(10, 2),
    "decimal(5,1)"  -> DecimalType.of(5, 1),
    "varchar(10)"   -> VarcharType(Some(IntConstant(10))),
    "error"         -> modelError
  )

  private def tpe(name: String): Type = types.getOrElse(name, fail(s"Unknown model type ${name}"))

  /** Results are compared by error-ness for errors, since the model abstracts messages. */
  private def describe(t: Type): String =
    t match
      case _: ErrorType =>
        "error"
      case other =>
        types
          .collectFirst {
            case (n, v) if v == other =>
              n
          }
          .getOrElse(s"unmapped ${other}")

  test("the fixture covers every model type") {
    arr(fixture("types")).map(strOf).toSet shouldBe types.keySet
  }

  test("unify follows the model") {
    val all = cases("unify")
    all.size shouldBe types.size * types.size
    all.foreach { c =>
      val clue = s"unify(${c.str("a")}, ${c.str("b")})"
      (clue, describe(TypeInference.unify(tpe(c.str("a")), tpe(c.str("b"))))) shouldBe
        (clue, c.str("result"))
    }
  }

  test("canCoerce follows the model") {
    val all = cases("canCoerce")
    all.size shouldBe types.size * types.size
    all.foreach { c =>
      val clue = s"canCoerce(${c.str("from")}, ${c.str("to")})"
      (clue, TypeInference.canCoerce(tpe(c.str("from")), tpe(c.str("to")))) shouldBe
        (clue, c.bool("result"))
    }
  }

  test("widen follows the model") {
    val all = cases("widen")
    all.size shouldBe types.size * types.size
    all.foreach { c =>
      val clue = s"widen(${c.str("from")}, ${c.str("to")})"
      (clue, describe(TypeInference.widen(tpe(c.str("from")), tpe(c.str("to"))))) shouldBe
        (clue, c.str("result"))
    }
  }

  test("findCommonType follows the model") {
    val all = cases("findCommonType")
    val n   = types.size
    all.size shouldBe 1 + n + n * n + n * n * n
    all.foreach { c =>
      val names = arr(c("types")).map(strOf)
      val clue  = s"findCommonType(${names.mkString(", ")})"
      (clue, describe(TypeInference.findCommonType(names.map(tpe)))) shouldBe
        (clue, c.str("result"))
    }
  }

end TypeCoercionConformanceTest
