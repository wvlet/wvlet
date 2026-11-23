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
import wvlet.lang.engine.catalog.TableSource
import wvlet.lang.engine.exec.JoinKind
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.plan.Planner
import wvlet.lang.engine.plan.SourceResolver
import wvlet.lang.engine.runtime.*
import wvlet.lang.model.plan.RawSQL
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
  * Replays the traces of the Lean model of EXISTS / IN decorrelation (formal/, exported by
  * `lake exe decorrelate-traces`) against the engine. The model proves that the semi and anti joins
  * of correlated [NOT] EXISTS and IN conjuncts (`Planner.correlatedJoin`) and the EXISTS mark under
  * OR / NOT (`Planner.decorrelateExists`) return SQL's rows under three-valued logic, NULLs and
  * duplicates included. This test checks, for each traced subquery, that the engine returns the
  * model's rows with the plan shape the model describes, and that DuckDB agrees.
  *
  * It also replays the semi and anti join cases of the filter pushdown model (proved by
  * `Pushdown.rewrite_sound`): a WHERE conjunct next to an EXISTS reaches `PushDownFilters` as a
  * filter over the semi or anti join, which moves it into the outer input.
  */
class DecorrelateConformanceTest extends UniTest:
  import RuntimePlan.*

  private val fixture: Map[String, JSONValue] =
    val text =
      Using.resource(
        Source.fromInputStream(
          getClass.getResourceAsStream("/wvlet/lang/engine/runtime/decorrelate-traces.json")
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

  private def inlineTable(rs: List[List[String]]): String = rs
    .map(_.mkString("[", ", ", "]"))
    .mkString("[", ", ", "]")

  private val left          = inlineTable(rows(fixture("left")))
  private val right         = inlineTable(rows(fixture("right")))
  private val pushdownLeft  = inlineTable(rows(fixture("pushdownLeft")))
  private val pushdownRight = inlineTable(rows(fixture("pushdownRight")))

  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance(".", Nil, _ => (), options)
  private val engine      = conformance.engine

  override def afterAll: Unit = conformance.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def withQuery[A](query: String)(f: Context ?=> Relation => A): A =
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    f(GenSQL.expand(q, summon[Context]))

  private def rowsOf(query: String): List[List[String]] =
    withQuery(query) { q =>
      engine
        .execute(q)
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

  private def nodes(p: RuntimePlan): Seq[RuntimePlan] = p +: p.children.flatMap(nodes)

  private def joinKinds(query: String): Seq[JoinKind] =
    withQuery(query) { q =>
      nodes(engine.runtimePlan(q)).collect { case j: Join =>
        j.kind
      }
    }

  /** The engine's rows equal the model's, and DuckDB's */
  private def check(query: String, expected: JSONValue): Unit =
    debug(query)
    try
      sorted(rowsOf(query)) shouldBe sorted(rows(expected))
      withQuery(query)(conformance.assertMatches(query, _))
    catch
      case e: Throwable =>
        warn(s"Mismatch for ${query}")
        throw e

  private def body(where: String, select: String = ""): String =
    val w =
      if where.isEmpty then
        ""
      else
        s" where ${where}"
    s"from ${right} as r(c, d)${w}${select}"

  /** An EXISTS argument */
  private def sub(where: String): String = s"{ ${body(where)} }"

  private val outer = s"from ${left} as l(a, b)"

  test("return the model's rows for EXISTS and NOT EXISTS conjuncts as semi and anti joins") {
    val cases = arr(fixture("exists")).map(obj)
    cases.size shouldBe 8
    for c <- cases do
      val w         = str(c("where"))
      val exists    = s"${outer} where exists ${sub(w)}"
      val notExists = s"${outer} where not exists ${sub(w)}"
      check(exists, c("existsRows"))
      check(notExists, c("notExistsRows"))
      joinKinds(exists) shouldBe Seq(JoinKind.Semi)
      joinKinds(notExists) shouldBe Seq(JoinKind.Anti)
  }

  test("return the model's rows for IN conjuncts as semi joins") {
    val cases = arr(fixture("in")).map(obj)
    cases.size shouldBe 5
    for c <- cases do
      val query =
        s"${outer} where ${str(c("target"))} in (${body(
            str(c("where")),
            s" select ${str(c("value"))}"
          )})"
      check(query, c("rows"))
      joinKinds(query) shouldBe Seq(JoinKind.Semi)
  }

  test("return the model's rows for EXISTS under OR and NOT as a mark over a left join") {
    val cases = arr(fixture("marks")).map(obj)
    cases.size shouldBe 4
    for c <- cases do
      val e = s"exists ${sub(str(c("where")))}"
      for cond <- arr(c("conds")).map(obj) do
        val query = s"${outer} where ${str(cond("cond")).replace("E", e)}"
        check(query, cond("rows"))
        // A left join with the distinct keys, which keeps every outer row once
        joinKinds(query) shouldBe Seq(JoinKind.Left)
  }

  /** The bound plan, and the plan after the passes that run up to filter pushdown */
  private def pushed(query: String): (RuntimePlan, RuntimePlan) =
    withQuery(query) { r =>
      val planner = Planner(
        engine.catalog,
        new SourceResolver:
          override def tableSource(name: String, fullName: String): Option[TableSource] = None
          override def fileSource(path: String): Option[TableSource]                    = None
          override def parseSql(r: RawSQL): Relation = fail("No SQL")
        ,
        options
      )
      val bound   = RuntimeBinder(planner).bind(r)
      val placing = options.withDisabled(Optimization.JoinKeyFilters)
      (bound, Optimizer(Seq(Normalize, PushDownFilters), placing).run(bound))
    }

  private def hasFilter(p: RuntimePlan): Boolean = nodes(p).exists(_.isInstanceOf[Filter])

  test("place each conjunct over a semi or anti join as the pushdown model does") {
    val cases = arr(fixture("pushdown")).map(obj)
    cases.size shouldBe 24
    for c <- cases do
      val kind               = str(c("kind"))
      val (prefix, joinKind) =
        kind match
          case "semi" =>
            ("exists", JoinKind.Semi)
          case "anti" =>
            ("not exists", JoinKind.Anti)
          case other =>
            fail(s"Unexpected join kind ${other}")
      val query =
        s"""from ${pushdownLeft} as l(a, b)
           |where ${prefix} { from ${pushdownRight} as r(c, d) where ${str(c("on"))} }
           |  and (${str(c("where"))})""".stripMargin
      val (b, p) = pushed(query)
      debug(s"${query}\n${p.explain}")
      try
        if c("trueConstant") == JSONBoolean(true) then
          hasFilter(p) shouldBe false
        else
          str(c("place")) shouldBe "left"
          hasFilter(b) shouldBe true
          val j = nodes(p)
            .collectFirst {
              case j: Join if j.kind == joinKind =>
                j
            }
            .getOrElse(fail(s"No ${kind} join in\n${p.explain}"))
          val above = nodes(p).takeWhile(_ ne j).exists(_.isInstanceOf[Filter])
          (above, hasFilter(j.left)) shouldBe (false, true)
        check(query, c("rows"))
      catch
        case e: Throwable =>
          warn(
            s"Mismatch for ${kind} join on ${str(c("on"))} where ${str(c("where"))}:\n${p.explain}"
          )
          throw e
    end for
  }

end DecorrelateConformanceTest
