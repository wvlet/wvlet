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
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.model.plan.Relation
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
  * Replays the traces of the Lean model of semi-join reduction (formal/, exported by `lake exe
  * semi-reduce-traces`) against the engine. The model proves that IN, NOT IN, EXISTS, and NOT
  * EXISTS over an inner join whose needed columns come from one input keep the same rows over the
  * semi join of that input, NULLs and duplicates included. This test runs each traced query with
  * `reduce_semi_joins` on and off: both must return the model's rows (and DuckDB's), and with the
  * rule on, the subquery's inner join must become a semi join.
  */
class SemiReduceConformanceTest extends UniTest:

  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance.tpch(options)
  private val engine      = conformance.engine
  private val off         = engine.withOptimizer(options.withDisabled(Optimization.ReduceSemiJoins))

  override def afterAll: Unit = conformance.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private val fixture: Map[String, JSONValue] =
    val text =
      Using.resource(
        Source.fromInputStream(
          getClass.getResourceAsStream("/wvlet/lang/engine/runtime/semi-reduce-traces.json")
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
        fail(s"Expected a JSON string: ${other.toJSON}")

  private def cell(v: JSONValue): String =
    v match
      case _: JSONNull =>
        "null"
      case other =>
        other.toJSON

  /** The VALUES list of an input's rows, with typed NULLs */
  private def values(name: String): String = arr(fixture(name))
    .map(r =>
      arr(r)
        .map {
          case _: JSONNull =>
            "cast(null as bigint)"
          case v =>
            v.toJSON
        }
        .mkString("(", ", ", ")")
    )
    .mkString("(values ", ", ", ")")

  private val o = s"${values("o")} as o(ok, oid)"
  private val l = s"${values("l")} as l(lk, lv)"
  private val r = s"${values("r")} as r(rk, rv)"

  /** The traced query as SQL */
  private def sql(consumer: String, extra: String): String =
    val cond =
      if extra.isEmpty then
        "lk = rk"
      else
        s"lk = rk and ${extra}"
    consumer match
      case "in" =>
        s"select oid from ${o} where ok in (select lk from ${l}, ${r} where ${cond})"
      case "not_in" =>
        s"select oid from ${o} where ok not in (select lk from ${l}, ${r} where ${cond})"
      case "exists" =>
        s"select oid from ${o} where exists (select 1 from ${l}, ${r} where ${cond} and lk = ok)"
      case _ =>
        s"select oid from ${o} where not exists (select 1 from ${l}, ${r} where ${cond} and lk = ok)"

  private def compiled[A](query: String)(f: Context ?=> Relation => A): A =
    val unit      = CompilationUnit.fromSqlString(query)
    val result    = compiler.compileSingleUnit(unit)
    given Context = result.context
    f(EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}")))

  private def rows(e: WvletEngine, query: String): Seq[String] =
    compiled(query) { q =>
      e.execute(GenSQL.expand(q, summon[Context]))
        .batches
        .flatMap(b =>
          (0 until b.size).map(i => Option(b.columns(0).get(i)).fold("null")(_.toString))
        )
        .sorted
    }

  private def explain(e: WvletEngine, query: String): String = compiled(query)(e.explain(_))

  private def semiJoins(plan: String): Int = plan
    .linesIterator
    .count(l => l.contains("HashJoin[Semi]") || l.contains("HashJoin[RightSemi]"))

  private val cases = arr(fixture("cases")).map(obj)

  test("the fixture has traces") {
    cases.size shouldBe 16
  }

  cases.foreach { c =>
    val (consumer, extra) = (str(c("consumer")), str(c("extra")))
    test(s"${consumer} over an inner join on lk = rk ${extra}") {
      val query    = sql(consumer, extra)
      val expected = arr(c("rows")).map(cell).sorted
      arr(c("inner_rows")).map(cell).sorted shouldBe expected
      rows(engine, query) shouldBe expected
      rows(off, query) shouldBe expected
      compiled(query)(conformance.assertMatches(query, _, engine))
      if consumer != "not_in" then
        // NOT IN evaluates its subquery at plan time, outside the rewritten plan
        semiJoins(explain(engine, query)) shouldBe semiJoins(explain(off, query)) + 1
    }
  }

end SemiReduceConformanceTest
