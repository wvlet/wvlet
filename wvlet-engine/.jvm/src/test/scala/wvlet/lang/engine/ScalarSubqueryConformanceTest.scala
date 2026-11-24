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
import wvlet.lang.engine.exec.JoinKind
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.runtime.*
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
  * Replays the traces of the Lean model of correlated scalar subqueries (formal/, exported by `lake
  * exe scalar-subquery-traces`) against the engine. The model proves that `Planner
  * .decorrelateScalar` (the body grouped by the correlation keys, left-joined to the outer input,
  * with the value over no rows for the unmatched outer rows) gives every outer row SQL's value,
  * NULL keys, duplicate outer rows, and groups without rows included. This test reads each outer
  * row's value as a select item and back through WHERE conditions (`is null` and `= v` for each
  * value), checks that the engine returns the model's rows with decorrelation on and off and that
  * DuckDB agrees, and that the plan is one left join over an aggregation.
  */
class ScalarSubqueryConformanceTest extends UniTest:
  import RuntimePlan.*

  private val fixture: Map[String, JSONValue] =
    val text =
      Using.resource(
        Source.fromInputStream(
          getClass.getResourceAsStream("/wvlet/lang/engine/runtime/scalar-subquery-traces.json")
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

  /** A cell: an integer or NULL, as text */
  private def cell(v: JSONValue): String =
    v match
      case JSONLong(n) =>
        n.toString
      case _: JSONNull =>
        "null"
      case other =>
        fail(s"Expected an integer or null: ${other.toJSON}")

  private def rows(v: JSONValue): List[List[String]] = arr(v).map(r => arr(r).map(cell))

  private def sorted(rs: Seq[List[String]]): List[String] = rs.map(_.mkString(",")).sorted.toList

  private def values(rs: List[List[String]]): String = rs
    .map(_.mkString("(", ", ", ")"))
    .mkString("(values ", ", ", ")")

  private val left  = values(rows(fixture("left")))
  private val right = values(rows(fixture("right")))

  private def inlineTable(rs: List[List[String]]): String = rs
    .map(_.mkString("[", ", ", "]"))
    .mkString("[", ", ", "]")

  private val options       = OptimizerOptions().withValidatePlans(true)
  private val conformance   = EngineConformance(".", Nil, _ => (), options)
  private val engine        = conformance.engine
  private val noDecorrelate = engine.withOptimizer(options.withDisabled(Optimization.Decorrelate))

  override def afterAll: Unit = conformance.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def compiled[A](query: String)(f: Context ?=> Relation => A): A =
    // Wvlet queries start with `from`: SQL has no select-item unnest
    val unit =
      if query.startsWith("from ") then
        CompilationUnit.fromWvletString(query)
      else
        CompilationUnit.fromSqlString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    f(EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}")))

  private def withQuery[A](query: String)(f: Context ?=> Relation => A): A =
    compiled(query)(q => f(GenSQL.expand(q, summon[Context])))

  private def rowsOf(e: WvletEngine, query: String): List[List[String]] =
    withQuery(query) { q =>
      e.execute(q)
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
                  fail(s"Unexpected value ${other}")
              }
              .toList
          )
        )
        .toList
    }

  private def nodes(p: RuntimePlan): Seq[RuntimePlan] = p +: p.children.flatMap(nodes)

  /** The engine's rows (decorrelated or not) equal the model's, and DuckDB's */
  private def check(query: String, expected: List[List[String]]): Unit =
    debug(query)
    try
      for e <- Seq(engine, noDecorrelate) do
        sorted(rowsOf(e, query)) shouldBe sorted(expected)
      compiled(query)(conformance.assertMatches(query, _))
    catch
      case e: Throwable =>
        warn(s"Mismatch for ${query}")
        throw e

  private def sub(c: Map[String, JSONValue]): String =
    s"(select ${str(c("agg"))} from ${right} as r(c, d) where ${str(c("where"))})"

  private val cases = arr(fixture("cases")).map(obj)

  /**
    * The query's runtime plan has one left join, over an aggregation, and above it a `CASE WHEN key
    * IS NULL` for the unmatched rows exactly when the value over no rows is not NULL
    */
  private def checkPlan(query: String, emptyIsNull: Boolean): Unit =
    val p     = withQuery(query)(engine.runtimePlan(_))
    val kinds = nodes(p).collect { case j: Join =>
      j.kind
    }
    try
      kinds shouldBe Seq(JoinKind.Left)
      nodes(p).exists(_.isInstanceOf[Aggregate]) shouldBe true
      val aboveJoin = p.explain.split("Join\\[Left\\]", 2)(1)
      aboveJoin.contains("case(is_null(") shouldBe !emptyIsNull
    catch
      case e: Throwable =>
        warn(s"Unexpected plan for ${query}:\n${p.explain}")
        throw e

  test("give every outer row the model's value of a correlated scalar subquery") {
    cases.size shouldBe 28
    for c <- cases do
      val rs = rows(c("rows"))
      // Correlated scalar subqueries bind in conditions only, so the value of each outer row is
      // read back through one condition per value: `is null`, and `= v` for each non-NULL v
      val isNull = s"select a, b from ${left} as l(a, b) where ${sub(c)} is null"
      check(isNull, rs.filter(_(2) == "null").map(_.take(2)))
      val emptyIsNull = c("emptyIsNull") == JSONBoolean(true)
      checkPlan(isNull, emptyIsNull)
      for v <- rs.map(_(2)).distinct.filter(_ != "null") do
        check(
          s"select a, b from ${left} as l(a, b) where ${sub(c)} = ${v}",
          rs.filter(_(2) == v).map(_.take(2))
        )
      // The NULL outer key never matches, so it gets the value over no rows
      emptyIsNull shouldBe rs.exists(r => r(0) == "null" && r(2) == "null")
  }

  test("give every outer row the model's value as a select item") {
    for c <- cases do
      val rs          = rows(c("rows"))
      val emptyIsNull = c("emptyIsNull") == JSONBoolean(true)
      val query       = s"select a, b, ${sub(c)} as v from ${left} as l(a, b)"
      check(query, rs)
      checkPlan(query, emptyIsNull)
      // Under `*`, inside an expression, and without an alias
      check(
        s"select *, coalesce(${sub(c)}, -1) from ${left} as l(a, b)",
        rs.map(r =>
          r.take(2) :+ (
            if r(2) == "null" then
              "-1"
            else
              r(2)
          )
        )
      )
  }

  /** The model's rows grouped by `key` (outer columns), with the value and the group's row count */
  private def grouped(
      rs: List[List[String]],
      key: List[String] => List[String]
  ): List[List[String]] = rs.groupBy(r => key(r) :+ r(2)).toList.map((k, g) => k :+ g.size.toString)

  /** Whether the subquery reads outer column `b` (then `group by a` alone leaves `b` ungrouped) */
  private def readsB(c: Map[String, JSONValue]): Boolean = str(c("where")).contains("= b")

  test("give every group the model's value as a select item of an aggregation") {
    for c <- cases do
      val rs = rows(c("rows"))
      // The subquery reads grouping keys only, so every row of a group has the same value
      check(
        s"select a, b, ${sub(c)} as v, count(*) as n from ${left} as l(a, b) group by a, b",
        grouped(rs, _.take(2))
      )
      // Next to an aggregate in the same item
      check(
        s"select a, b, coalesce(${sub(
            c
          )}, -1) * count(*) as w from ${left} as l(a, b) group by a, b",
        grouped(rs, _.take(2)).map { r =>
          val v =
            if r(2) == "null" then
              -1L
            else
              r(2).toLong
          r.take(2) :+ (v * r(3).toLong).toString
        }
      )
      if !readsB(c) then
        val byA = grouped(rs, _.take(1))
        check(s"select a, ${sub(c)} as v, count(*) as n from ${left} as l(a, b) group by a", byA)
        // The grand total's `a` is NULL, so it gets the NULL key's value: the value over no rows
        val nullValue = rs.find(_(0) == "null").map(_(2)).get
        check(
          s"select a, ${sub(c)} as v, count(*) as n from ${left} as l(a, b) group by rollup(a)",
          byA :+ List("null", nullValue, rs.size.toString)
        )
  }

  test("give every outer row the model's value next to unnest") {
    for c <- cases do
      val rs = rows(c("rows"))
      // `end` ends a Wvlet flow, so the model's CASE is an if-then-else here
      val agg = str(c("agg")).replace(
        "case when count(*) > 0 then null else 1 end",
        "if count(*) > 0 then null else 1"
      )
      val wvSub =
        s"{ from ${inlineTable(rows(fixture("right")))} as r(c, d) where ${str(
            c("where")
          )} select ${agg} }"
      check(
        s"from ${inlineTable(
            rows(fixture("left"))
          )} as l(a, b) select a, b, ${wvSub} as v, unnest([1, 2]) as u",
        rs.flatMap(r => Seq(r :+ "1", r :+ "2"))
      )
  }

  test("keep every outer row once with the value under other conditions") {
    // The model's `scalarFilter_sound`: a condition over an outer column and the value
    for c <- cases do
      val rs       = rows(c("rows"))
      val query    = s"select a, b from ${left} as l(a, b) where b is null or ${sub(c)} > 1"
      val expected = rs.filter(r => r(1) == "null" || (r(2) != "null" && r(2).toLong > 1))
      check(query, expected.map(_.take(2)))
  }

end ScalarSubqueryConformanceTest
