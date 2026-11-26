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
  * Replays the traces of the Lean model of aggregate windows (formal/, exported by `lake exe
  * aggregate-windows-traces`) against the engine. The model proves that over a join of I with E on
  * I's group key alone, a min, max, or avg over the join's rows of a key equals the aggregate over
  * I's rows of the key, and that a sum doesn't. This test runs each traced scalar subquery with
  * `aggregate_windows` on and off: both must return the decorrelated rows (and DuckDB's), and with
  * the rule on, the min, max, and avg must become windows while the sum stays a join.
  */
class AggregateWindowsConformanceTest extends UniTest:

  private val fixture: Map[String, JSONValue] =
    val text =
      Using.resource(
        Source.fromInputStream(
          getClass.getResourceAsStream("/wvlet/lang/engine/runtime/aggregate-windows-traces.json")
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

  /** A traced value as text: an avg's fraction as a double */
  private def cell(v: JSONValue): String =
    v match
      case _: JSONNull =>
        "null"
      case JSONArray(Seq(n, d)) =>
        (n.toJSON.toDouble / d.toJSON.toDouble).toString
      case other =>
        other.toJSON

  private def valuesOf(table: String): String = arr(fixture(table))
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

  // The fixture's tables i(g, x, id) and e(e, w), written by DuckDB to Parquet for both engines
  private val dir   = s"target/aggregate-windows/${ProcessHandle.current().pid()}"
  private val iPath = s"${dir}/i.parquet"
  private val ePath = s"${dir}/e.parquet"

  java.io.File(iPath).getParentFile.mkdirs()

  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance(
    "spec/tpch",
    Seq(
      s"create table i as select * from (${valuesOf("i")}) as v(g, x, id)",
      s"copy i to '${iPath}' (format parquet)",
      s"create table e as select * from (${valuesOf("e")}) as v(e, w)",
      s"copy e to '${ePath}' (format parquet)"
    ),
    e =>
      e.catalog.registerParquet("i", iPath)
      e.catalog.registerParquet("e", ePath)
    ,
    options
  )

  private val engine = conformance.engine
  private val off    = engine.withOptimizer(options.withDisabled(Optimization.AggregateWindows))

  override def afterAll: Unit = conformance.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  /** The traced query as SQL */
  private def sql(agg: String): String =
    s"select i.id, e.w, (select ${agg}(i2.x) from i as i2 where i2.g = i.g) as v from i, e where i.g = e.e"

  private def compiled[A](query: String)(f: Context ?=> Relation => A): A =
    val unit      = CompilationUnit.fromSqlString(query)
    val result    = compiler.compileSingleUnit(unit)
    given Context = result.context
    f(EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}")))

  /** The result rows, each as its cells' text: values as the fixture writes them */
  private def rows(e: WvletEngine, query: String): Seq[Seq[String]] =
    compiled(query) { q =>
      val r = e.execute(GenSQL.expand(q, summon[Context]))
      r.batches
        .flatMap(b =>
          (0 until b.size).map(i =>
            b.columns
              .map { c =>
                Option(c.get(i)).fold("null") {
                  case d: Double =>
                    d.toString
                  case n: java.math.BigDecimal =>
                    n.stripTrailingZeros().toPlainString
                  case other =>
                    other.toString
                }
              }
              .toSeq
          )
        )
        .sortBy(_.mkString(","))
    }

  private def traced(v: JSONValue): Seq[Seq[String]] = arr(v)
    .map(r => arr(r).map(cell))
    .sortBy(_.mkString(","))

  private def explain(e: WvletEngine, query: String): String = compiled(query)(e.explain(_))

  private val cases = arr(fixture("cases")).map(obj)

  test("the fixture has traces") {
    cases.size shouldBe 4
  }

  cases.foreach { c =>
    val agg = str(c("agg"))
    test(s"${agg} of a scalar subquery over the rows of a join on its key") {
      val query    = sql(agg)
      val expected = traced(c("join_rows"))
      rows(engine, query) shouldBe expected
      rows(off, query) shouldBe expected
      compiled(query)(conformance.assertMatches(query, _, engine))
      explain(off, query) shouldNotContain "Window["
      if agg == "sum" then
        // A sum over the join's rows counts each row of i once per matching row of e
        traced(c("rows")) shouldNotBe expected
        explain(engine, query) shouldNotContain "Window["
      else
        traced(c("rows")) shouldBe expected
        explain(engine, query) shouldContain "Window["
    }
  }

end AggregateWindowsConformanceTest
