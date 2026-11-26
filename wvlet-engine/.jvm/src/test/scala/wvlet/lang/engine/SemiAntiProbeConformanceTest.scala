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
  * Replays the traces of the Lean model of one probe for an EXISTS and a NOT EXISTS over the same
  * table (formal/, exported by `lake exe semi-anti-probe-traces`) against the engine. The model
  * proves that an anti join over a semi join of the same table, or a semi join over an anti join,
  * keeps the rows that one pass marking matched and excluded rows keeps, with three-valued
  * conditions and NULL keys. This test runs each traced query with `semi_anti_one_probe` on and
  * off: both must return the model's rows (and DuckDB's), and with the rule on, the two joins must
  * become one RIGHT SEMI join with an exclusion.
  */
class SemiAntiProbeConformanceTest extends UniTest:

  private val fixture: Map[String, JSONValue] =
    val text =
      Using.resource(
        Source.fromInputStream(
          getClass.getResourceAsStream("/wvlet/lang/engine/runtime/semi-anti-probe-traces.json")
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
        other.toJSON

  private def cell(v: JSONValue): String =
    v match
      case _: JSONNull =>
        "null"
      case other =>
        other.toJSON

  private def values(table: String): String = arr(fixture(table))
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

  // The fixture's tables, written by DuckDB to Parquet for both engines to read
  private val dir   = s"target/semi-anti-probe/${ProcessHandle.current().pid()}"
  private val bPath = s"${dir}/b.parquet"
  private val tPath = s"${dir}/t.parquet"
  // t once more as another table, which the rule must not fuse with t
  private val uPath = s"${dir}/u.parquet"

  java.io.File(bPath).getParentFile.mkdirs()

  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance(
    "spec/tpch",
    Seq(
      s"create table b as select * from (${values("b")}) as v(k, s, id)",
      // Rows with keys no row of b has change no result, but make t the larger side, so that
      // both joins build on b as TPC-H q21's do
      s"create table t as select * from (${values("t")}) as v(k, s, a, c, id) " +
        "union all select 100 + i, 1, 2, 1, 100 + i from range(300) as r(i)",
      s"copy b to '${bPath}' (format parquet)",
      s"copy t to '${tPath}' (format parquet)",
      s"copy t to '${uPath}' (format parquet)",
      "create table u as select * from t"
    ),
    e =>
      e.catalog.registerParquet("b", bPath)
      e.catalog.registerParquet("t", tPath)
      e.catalog.registerParquet("u", uPath)
    ,
    options
  )

  private val engine = conformance.engine
  private val off    = engine.withOptimizer(options.withDisabled(Optimization.SemiAntiOneProbe))

  override def afterAll: Unit = conformance.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def sub(table: String, residual: Boolean, filter: String): String =
    val differ =
      if residual then
        s" and ${table}.s <> b.s"
      else
        ""
    s"(select 1 from ${table} where ${table}.k = b.k${differ} and ${filter})"

  /** The traced query as SQL */
  private def sql(
      existsFirst: Boolean,
      semiResidual: Boolean,
      antiResidual: Boolean,
      q: String,
      p: String,
      antiTable: String = "t"
  ): String =
    val e    = s"exists ${sub("t", semiResidual, q)}"
    val ne   = s"not exists ${sub(antiTable, antiResidual, p).replace(s"${antiTable}.", "t.")}"
    val cond =
      if existsFirst then
        s"${e} and ${ne}"
      else
        s"${ne} and ${e}"
    s"select id from b where ${cond}"

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
    cases.size shouldBe 32
  }

  cases.foreach { c =>
    val existsFirst  = str(c("exists_first")) == "true"
    val semiResidual = str(c("semi_residual")) == "true"
    val antiResidual = str(c("anti_residual")) == "true"
    val (q, p)       = (str(c("q")), str(c("p")))
    val query        = sql(existsFirst, semiResidual, antiResidual, q, p)
    test(s"probe t once for: ${query}") {
      val expected = arr(c("rows")).map(cell).sorted
      arr(c("join_rows")).map(cell).sorted shouldBe expected
      rows(engine, query) shouldBe expected
      rows(off, query) shouldBe expected
      compiled(query)(conformance.assertMatches(query, _, engine))
      explain(off, query) shouldNotContain "exclude="
      explain(engine, query) shouldContain "exclude="
    }
  }

  test("leave an EXISTS and a NOT EXISTS over different tables") {
    val query =
      "select id from b where exists (select 1 from t where t.k = b.k and t.s <> b.s) " +
        "and not exists (select 1 from u where u.k = b.k and u.c < u.a)"
    compiled(query)(conformance.assertMatches(query, _, engine))
    explain(engine, query) shouldNotContain "exclude="
  }

  test("leave an EXISTS and a NOT EXISTS on different keys") {
    val query =
      "select id from b where exists (select 1 from t where t.k = b.k and t.s <> b.s) " +
        "and not exists (select 1 from t where t.s = b.k and t.c < t.a)"
    compiled(query)(conformance.assertMatches(query, _, engine))
    explain(engine, query) shouldNotContain "exclude="
  }

end SemiAntiProbeConformanceTest
