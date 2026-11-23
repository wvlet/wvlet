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
package wvlet.lang.engine

import wvlet.lang.compiler.*
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.engine.expr.AndExpr
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.runtime.Normalize
import wvlet.lang.engine.runtime.Optimizer
import wvlet.lang.engine.runtime.RuntimePlan
import wvlet.lang.model.plan.Relation
import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.JSONArray
import wvlet.uni.json.JSON.JSONBoolean
import wvlet.uni.json.JSON.JSONLong
import wvlet.uni.json.JSON.JSONNull
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.json.JSON.JSONString
import wvlet.uni.json.JSON.JSONValue
import wvlet.uni.test.UniTest

import scala.io.Source
import scala.util.Using

/**
  * Replays the traces of the Lean model of `FactorOr` (formal/, exported by
  * `lake exe factor-or-traces`) against the engine. The model proves that factoring the conjuncts
  * common to every branch out of an `or` keeps the condition's value under three-valued logic,
  * provided conjuncts with the same signature have the same value, and that the old `InList.sig`
  * (which left out a NULL value and joined the values with commas) broke this. For each traced
  * condition, this test checks that `Normalize` factors it as the model does, into as many
  * conjuncts, and that the engine returns the model's rows for the condition (with FactorOr on and
  * off) and for its factored form, as DuckDB does.
  */
class FactorOrConformanceTest extends UniTest:

  private val fixture: Map[String, JSONValue] =
    val text =
      Using.resource(
        Source.fromInputStream(
          getClass.getResourceAsStream("/wvlet/lang/engine/runtime/factor-or-traces.json")
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

  /** A cell as text: an integer, a boolean, a string, or NULL */
  private def cell(v: JSONValue): String =
    v match
      case JSONLong(n) =>
        n.toString
      case JSONBoolean(b) =>
        b.toString
      case JSONString(s) =>
        s
      case _: JSONNull =>
        "null"
      case other =>
        fail(s"Unexpected cell: ${other.toJSON}")

  /** A cell as a Wvlet literal */
  private def literal(v: JSONValue): String =
    v match
      case JSONString(s) =>
        s"'${s}'"
      case other =>
        cell(other)

  private def sorted(rs: Seq[List[String]]): List[String] = rs.map(_.mkString("|")).sorted.toList

  private val table = arr(fixture("table"))
    .map(r => arr(r).map(literal).mkString("[", ", ", "]"))
    .mkString("[", ", ", "]")

  private val cases = arr(fixture("cases")).map(obj)

  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance(".", Nil, _ => (), options)
  private val engine      = conformance.engine
  // Bind without the rule, so that Normalize alone factors the bound condition
  private val unfactored = engine.withOptimizer(options.withDisabled(Optimization.FactorOr))

  override def afterAll: Unit = conformance.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def withQuery[A](query: String)(f: Context ?=> Relation => A): A =
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    f(GenSQL.expand(q, summon[Context]))

  private def query(where: String): String = s"from ${table} as t(a, b, c, s) where ${where}"

  private def rowsOf(e: WvletEngine, q: String): List[List[String]] =
    withQuery(q) { r =>
      e.execute(r)
        .batches
        .flatMap(b =>
          (0 until b.size).map(i =>
            b.columns
              .map(_.get(i))
              .map {
                case null =>
                  "null"
                case n: Number =>
                  n.longValue().toString
                case other =>
                  other.toString
              }
              .toList
          )
        )
        .toList
    }

  private def filterOf(p: RuntimePlan): RuntimePlan.Filter =
    p match
      case f: RuntimePlan.Filter =>
        f
      case other =>
        other.children.map(filterOf).headOption.getOrElse(fail(s"No filter in\n${p.explain}"))

  private def conjuncts(e: VExpr): List[VExpr] =
    e match
      case AndExpr(l, r) =>
        conjuncts(l) ++ conjuncts(r)
      case other =>
        List(other)

  test("factor each traced condition as the model does") {
    cases.size shouldBe 13
    for c <- cases do
      val where = str(c("where"))
      debug(where)
      val p         = withQuery(query(where))(unfactored.runtimePlan(_))
      val o         = Optimizer.only(Normalize, options)
      val optimized = o.run(p)
      val factored  = c("factored") == JSONBoolean(true)
      o.firedRules.contains("factor_or") shouldBe factored
      conjuncts(filterOf(optimized).cond.expr).size shouldBe cell(c("conjuncts")).toInt
  }

  test("return the model's rows for each traced condition and its factored form") {
    for c <- cases do
      val want = sorted(arr(c("rows")).map(r => arr(r).map(cell)))
      for where <- Seq(str(c("where")), str(c("factoredWhere"))) do
        val q = query(where)
        debug(q)
        for e <- Seq(engine, unfactored) do
          sorted(rowsOf(e, q)) shouldBe want
        withQuery(q)(conformance.assertMatches(q, _))
  }

end FactorOrConformanceTest
