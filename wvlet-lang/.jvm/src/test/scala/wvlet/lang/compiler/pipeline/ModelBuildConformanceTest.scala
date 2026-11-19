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
package wvlet.lang.compiler.pipeline

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.JSONArray
import wvlet.uni.json.JSON.JSONNull
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.json.JSON.JSONString
import wvlet.uni.json.JSON.JSONValue
import wvlet.uni.test.UniTest

import scala.io.Source
import scala.util.Using

/**
  * Replays the traces of the Lean model of `wvlet build` ordering (formal/, exported by
  * `lake exe model-build-traces`) against the shipped [[ModelOrder]]. The model proves that the
  * build order is a topological permutation of the selection, that selectors resolve to their
  * dependency closures, and that cycle detection reports a real cycle exactly when one exists; this
  * test checks that the Scala code computes what the model computes.
  */
class ModelBuildConformanceTest extends UniTest:

  private val fixture: Map[String, JSONValue] =
    val text =
      Using.resource(
        Source.fromInputStream(
          getClass.getResourceAsStream("/wvlet/lang/compiler/pipeline/model-build-traces.json")
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

  private def str(v: JSONValue): String =
    v match
      case JSONString(s) =>
        s
      case other =>
        fail(s"Expected a string: ${other.toJSON}")

  private def names(v: JSONValue): List[String] = arr(v).map(str)

  private def cases(name: String): List[Map[String, JSONValue]] = arr(fixture(name)).map(obj)

  private val models: List[String] = names(fixture("models"))

  private def upstreamsOf(c: Map[String, JSONValue]): Map[String, List[String]] =
    models.zip(arr(c("ups")).map(names)).toMap

  /** The outcome of [[ModelOrder.select]], in the fixture's terms */
  private def selectOutcome(
      upstreams: Map[String, List[String]],
      selectors: List[String]
  ): Map[String, JSONValue] =
    try
      Map(
        "result" ->
          JSONArray(ModelOrder.select(models, upstreams, selectors).map(JSONString(_)).toIndexedSeq)
      )
    catch
      case e: WvletLangException if e.statusCode == StatusCode.INVALID_ARGUMENT =>
        Map("error" -> JSONString("empty"))
      case e: WvletLangException if e.statusCode == StatusCode.MODEL_NOT_FOUND =>
        val name = "Model '(.*)' is not a materialized model".r.findFirstMatchIn(e.getMessage).get
        Map("error" -> JSONString("notFound"), "name" -> JSONString(name.group(1)))

  private def checkSelect(upstreams: Map[String, List[String]], c: Map[String, JSONValue]): Unit =
    val selectors = names(c("selectors"))
    val clue      = s"select(${selectors.mkString("[", ", ", "]")}) with ${upstreams}"
    (clue, selectOutcome(upstreams, selectors)) shouldBe (clue, c - "selectors")

  test("the fixture covers every graph on three models") {
    cases("graphs").size shouldBe 512
  }

  test("findCycle follows the model") {
    cases("graphs").foreach { c =>
      val upstreams = upstreamsOf(c)
      val expected  =
        c("cycle") match
          case _: JSONNull =>
            None
          case v =>
            Some(names(v))
      (upstreams, ModelOrder.findCycle(models, upstreams)) shouldBe (upstreams, expected)
    }
  }

  test("buildOrder follows the model") {
    cases("graphs").foreach { c =>
      val upstreams = upstreamsOf(c)
      arr(c("orders"))
        .map(obj)
        .foreach { o =>
          val selected = names(o("selected"))
          val clue     = s"buildOrder(${selected}) with ${upstreams}"
          o.get("order") match
            case Some(order) =>
              (clue, ModelOrder.buildOrder(selected, upstreams)) shouldBe (clue, names(order))
            case None =>
              val e = intercept[WvletLangException](ModelOrder.buildOrder(selected, upstreams))
              e.statusCode shouldBe StatusCode.RECURSIVE_MODEL_REFERENCE
              (clue, e.getMessage) shouldBe (clue, ModelOrder.cycleMessage(names(o("cycle"))))
        }
    }
  }

  test("select follows the model") {
    val all = cases("graphs").flatMap(c => arr(c("select")).map(s => upstreamsOf(c) -> obj(s)))
    all.size shouldBe 64 * 12
    all.foreach((upstreams, s) => checkSelect(upstreams, s))
  }

  test("selector parsing follows the model") {
    val upstreams = Map("a" -> List("b"), "b" -> Nil, "c" -> Nil)
    cases("parsing").foreach(checkSelect(upstreams, _))
  }

  test("upstreamsOf follows the model") {
    val all = cases("edges")
    all.size shouldBe 512 + 9
    all.foreach { c =>
      val refs = obj(c("refs")).map((k, v) => k -> names(v))
      // Defined models: a and b are materialized, e and f ephemeral; x is undefined
      val defined   = Set("a", "b", "e", "f")
      val refsOf    = (n: String) => Option.when(defined.contains(n))(refs.getOrElse(n, Nil))
      val isMat     = Set("a", "b")
      val upstreams = obj(c("upstreams"))
      List("a", "b").foreach { m =>
        val clue = s"upstreamsOf(${m}) with ${refs}"
        (clue, ModelOrder.upstreamsOf(refs.getOrElse(m, Nil), isMat, refsOf)) shouldBe
          (clue, names(upstreams(m)))
      }
    }
  }

end ModelBuildConformanceTest
