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
import wvlet.lang.engine.runtime.RuntimePlan
import wvlet.lang.model.plan.Relation
import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.JSONArray
import wvlet.uni.json.JSON.JSONBoolean
import wvlet.uni.json.JSON.JSONLong
import wvlet.uni.json.JSON.JSONNull
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.json.JSON.JSONValue
import wvlet.uni.test.UniTest

import scala.io.Source
import scala.util.Using

/**
  * Replays the traces of the Lean model of NOT IN (formal/, exported by `lake exe not-in-traces`)
  * against the engine. The model proves that each of the engine's bindings of `x NOT IN (subquery)`
  * (the anti join of a filter conjunct, the null-aware anti join of a correlated one, and the value
  * set elsewhere) returns SQL's rows under three-valued logic, and that a plain anti join on
  * `x = y` would not. This test checks, for each traced subquery, that the engine returns the
  * model's rows and values, with decorrelation on and off, and that DuckDB agrees.
  */
class NotInConformanceTest extends UniTest:
  import RuntimePlan.*

  private val fixture: Map[String, JSONValue] =
    val text =
      Using.resource(
        Source.fromInputStream(
          getClass.getResourceAsStream("/wvlet/lang/engine/runtime/not-in-traces.json")
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

  /** A cell: an integer, a boolean, or NULL, as text */
  private def cell(v: JSONValue): String =
    v match
      case JSONLong(n) =>
        n.toString
      case JSONBoolean(b) =>
        b.toString
      case _: JSONNull =>
        "null"
      case other =>
        fail(s"Expected an integer, a boolean, or null: ${other.toJSON}")

  private def rows(v: JSONValue): List[List[String]] = arr(v).map(r => arr(r).map(cell))

  private def sorted(rs: Seq[List[String]]): List[String] = rs.map(_.mkString(",")).sorted.toList

  private val leftRows  = rows(fixture("left"))
  private val rightRows = rows(fixture("right"))
  private val cases     = arr(fixture("cases")).map(obj)

  private def inlineTable(rs: List[List[String]]): String = rs
    .map(_.mkString("[", ", ", "]"))
    .mkString("[", ", ", "]")

  private def sqlValues(rs: List[List[String]]): String = rs
    .map(_.mkString("(", ", ", ")"))
    .mkString("values ", ", ", "")

  private val left  = inlineTable(leftRows)
  private val right = inlineTable(rightRows)

  private val options       = OptimizerOptions().withValidatePlans(true)
  private val conformance   = EngineConformance(".", Nil, _ => (), options)
  private val engine        = conformance.engine
  private val noDecorrelate = engine.withOptimizer(options.withDisabled(Optimization.Decorrelate))

  override def afterAll: Unit = conformance.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def withQuery[A](query: String, sql: Boolean)(f: Context ?=> Relation => A): A =
    val unit =
      if sql then
        CompilationUnit.fromSqlString(query)
      else
        CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    f(GenSQL.expand(q, summon[Context]))

  private def rowsOf(e: WvletEngine, query: String, sql: Boolean = false): List[List[String]] =
    withQuery(query, sql) { q =>
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
                case b: java.lang.Boolean =>
                  b.toString
                case other =>
                  fail(s"Unexpected value ${other}")
              }
              .toList
          )
        )
        .toList
    }

  private def joinKinds(query: String): Seq[JoinKind] =
    def nodes(p: RuntimePlan): Seq[RuntimePlan] = p +: p.children.flatMap(nodes)
    withQuery(query, sql = false) { q =>
      nodes(engine.runtimePlan(q)).collect { case j: Join =>
        j.kind
      }
    }

  /** The engine's rows equal the model's, with and without decorrelation, and DuckDB's */
  private def check(query: String, expected: JSONValue, sql: Boolean = false): Unit =
    val want = sorted(rows(expected))
    debug(query)
    for e <- Seq(engine, noDecorrelate) do
      sorted(rowsOf(e, query, sql)) shouldBe want
    withQuery(query, sql)(conformance.assertMatches(query, _))

  private def sub(g: String): String = s"(from ${right} as r(k, c) where k = ${g} select c)"

  private def sqlSub(g: String): String =
    s"(select c from (${sqlValues(rightRows)}) as r(k, c) where k = ${g})"

  test("return the model's rows for NOT IN and IN conjuncts over each subquery") {
    cases.size shouldBe 6
    for c <- cases do
      val g     = cell(c("group"))
      val notIn = s"from ${left} as l(a, b) where a not in ${sub(g)}"
      check(notIn, c("notInRows"))
      check(s"from ${left} as l(a, b) where a in ${sub(g)}", c("inRows"))
      // The plan: no join over an empty subquery or one with a NULL (a constant condition),
      // and an anti join otherwise
      val values = arr(c("values")).map(cell)
      joinKinds(notIn) shouldBe (
        if values.isEmpty || values.contains("null") then
          Nil
        else
          Seq(JoinKind.Anti)
      )
  }

  test("return the model's values of IN and NOT IN in select items") {
    for c <- cases do
      val g = cell(c("group"))
      check(
        s"from ${left} as l(a, b) select a, b, a in ${sub(g)} as i, a not in ${sub(g)} as n",
        c("items")
      )
      check(
        s"""select a, b, a in ${sqlSub(g)} as i, a not in ${sqlSub(g)} as n
           |from (${sqlValues(leftRows)}) as l(a, b)""".stripMargin,
        c("items"),
        sql = true
      )
  }

  test("return the model's rows for a correlated NOT IN") {
    val query =
      s"from ${left} as l(a, b) where a not in (from ${right} as r(k, c) where k = l.b select c)"
    check(query, fixture("correlatedNotInRows"))
    joinKinds(query) shouldBe Seq(JoinKind.Anti)
  }

end NotInConformanceTest
