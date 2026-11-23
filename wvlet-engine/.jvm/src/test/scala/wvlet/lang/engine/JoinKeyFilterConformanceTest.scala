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
  * Replays the traces of the Lean model of carrying filters across join keys (formal/, exported by
  * `lake exe join-key-filter-traces`) against the engine. The model proves that a conjunct over the
  * preserved input's key, above an inner, LEFT, RIGHT, semi, or anti join, keeps the same rows when
  * both inputs are filtered by it instead. This test runs each traced join with `join_key_filters`
  * on and off: both must return the model's rows (and DuckDB's), and with the rule on the conjunct
  * must also filter the other input.
  */
class JoinKeyFilterConformanceTest extends UniTest:

  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance.tpch(options)
  private val engine      = conformance.engine
  private val off         = engine.withOptimizer(options.withDisabled(Optimization.JoinKeyFilters))

  override def afterAll: Unit = conformance.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private val fixture: Map[String, JSONValue] =
    val text =
      Using.resource(
        Source.fromInputStream(
          getClass.getResourceAsStream("/wvlet/lang/engine/join-key-filter-traces.json")
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

  /** The VALUES list of an input's (key, id) rows, with typed NULL keys */
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

  private val l = s"${values("left")} as l(lk, lid)"
  private val r = s"${values("right")} as r(rk, rid)"

  /**
    * The traced join as SQL. The join sits under an aggregation, so that the binder orders it
    * before the conjunct above reaches it, as it does for TPC-DS q78
    */
  private def sql(kind: String, side: String, pred: String): String =
    val cond = pred.replace(
      "{k}",
      if side == "left" then
        "lk"
      else
        "rk"
    )
    kind match
      case "semi" | "anti" =>
        val not =
          if kind == "anti" then
            "not "
          else
            ""
        s"""select lid, null as rid from (
           |  select lk, lid from ${l}
           |  where ${not}exists (select 1 from ${r} where rk = lk)
           |  group by lk, lid
           |) t where ${cond}""".stripMargin
      case _ =>
        s"""select lid, rid from (
           |  select lk, lid, rk, rid from ${l} ${kind} join ${r} on lk = rk
           |  group by lk, lid, rk, rid
           |) t where ${cond}""".stripMargin

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
          (0 until b.size).map(i =>
            b.columns.map(c => Option(c.get(i)).map(_.toString).getOrElse("null")).mkString("|")
          )
        )
        .sorted
    }

  private def explain(e: WvletEngine, query: String): String = compiled(query)(e.explain(_))

  private val cases = arr(fixture("cases")).map(obj)

  test("the fixture has traces") {
    cases.size shouldBe 36
  }

  cases.foreach { c =>
    val (kind, side, pred) = (str(c("kind")), str(c("side")), str(c("pred")))
    test(s"${kind} join, conjunct over the ${side} key: ${pred}") {
      val query    = sql(kind, side, pred)
      val expected = arr(c("rows")).map(row => arr(row).map(cell).mkString("|")).sorted
      rows(engine, query) shouldBe expected
      rows(off, query) shouldBe expected
      compiled(query)(conformance.assertMatches(query, _, engine))
      // With the rule, the other input is filtered too: the plans differ
      explain(engine, query) shouldNotBe explain(off, query)
    }
  }

  test("don't carry conjuncts across floating-point keys") {
    // -0.0 and 0.0 are the same key, and NaN equals itself in a hash join, unlike under `=`
    val query =
      """select lid, rid from (values (cast(0.0 as double), 1), (cast(2.0 as double), 2)) as l(lk, lid)
        |left join (values (cast(-0.0 as double), 11), (cast(2.0 as double), 12)) as r(rk, rid)
        |on lk = rk
        |where lk > 1""".stripMargin
    explain(engine, query) shouldBe explain(off, query)
    rows(engine, query) shouldBe Seq("2|12")
  }

end JoinKeyFilterConformanceTest
