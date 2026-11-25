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
  * Replays the traces of the Lean model of self-join aggregation (formal/, exported by `lake exe
  * self-join-aggregate-traces`) against the engine. The model proves that a semi join of rows of a
  * table with the table itself on an equal key and a different value keeps the same rows as a semi
  * join on the key with `group by k having min(x) <> max(x)`, and, where only the distinct keys
  * matter and the left side reads the whole table, that the keys are the aggregation's, NULL keys
  * and values included. This test runs each traced query with `self_join_aggregate` on and off:
  * both must return the model's rows (and DuckDB's), and with the rule on, the self join of the
  * whole table must become an aggregation.
  */
class SelfJoinAggregateConformanceTest extends UniTest:

  private val fixture: Map[String, JSONValue] =
    val text =
      Using.resource(
        Source.fromInputStream(
          getClass.getResourceAsStream("/wvlet/lang/engine/runtime/self-join-aggregate-traces.json")
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

  // The fixture's table t(k, x, id), written by DuckDB to Parquet for both engines to read
  private val path   = s"target/self-join-aggregate/t-${ProcessHandle.current().pid()}.parquet"
  private val values = arr(fixture("t"))
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
    .mkString("values ", ", ", "")

  java.io.File(path).getParentFile.mkdirs()

  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance(
    "spec/tpch",
    Seq(
      s"create table t as select * from (${values}) as v(k, x, id)",
      s"copy t to '${path}' (format parquet)"
    ),
    e => e.catalog.registerParquet("t", path),
    options
  )

  private val engine = conformance.engine
  private val off    = engine.withOptimizer(options.withDisabled(Optimization.SelfJoinAggregate))

  override def afterAll: Unit = conformance.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  /** The traced query as SQL */
  private def sql(consumer: String, extra: String): String =
    val cond =
      if extra.isEmpty then
        "t1.k = t2.k and t1.x <> t2.x"
      else
        s"t1.k = t2.k and t1.x <> t2.x and ${extra}"
    consumer match
      case "exists" =>
        val where =
          if extra.isEmpty then
            ""
          else
            s" and ${extra}"
        s"select id from t as t1 where exists (select 1 from t as t2 where t1.k = t2.k and t1.x <> t2.x)${where}"
      case "in" =>
        s"select id from t as o where o.k in (select t1.k from t as t1, t as t2 where ${cond})"
      case _ =>
        s"select distinct t1.k from t as t1, t as t2 where ${cond}"

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

  private val cases = arr(fixture("cases")).map(obj)

  test("the fixture has traces") {
    cases.size shouldBe 9
  }

  cases.foreach { c =>
    val (consumer, extra) = (str(c("consumer")), str(c("extra")))
    test(s"${consumer} over a self join on k with a different x ${extra}") {
      val query    = sql(consumer, extra)
      val expected = arr(c("rows")).map(cell).sorted
      arr(c("join_rows")).map(cell).sorted shouldBe expected
      rows(engine, query) shouldBe expected
      rows(off, query) shouldBe expected
      compiled(query)(conformance.assertMatches(query, _, engine))
      // The rule leaves a filtered left side, whose aggregation would read all of t
      explain(off, query) shouldNotContain "min(#"
      if extra.isEmpty then
        explain(engine, query) shouldContain "min(#"
      else
        explain(engine, query) shouldNotContain "min(#"
    }
  }

end SelfJoinAggregateConformanceTest
