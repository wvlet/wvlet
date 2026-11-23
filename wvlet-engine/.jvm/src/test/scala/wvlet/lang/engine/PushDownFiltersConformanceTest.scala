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
  * Replays the traces of the Lean model of filter pushdown through joins (formal/, exported by
  * `lake exe pushdown-filter-traces`) against the shipped [[PushDownFilters]] pass. The model
  * proves that each placement keeps the rows of the filter over the join, for every input and ON
  * condition, NULLs included, and that the refused placements would not. This test checks, for each
  * traced query, that the pass puts the WHERE conjunct where the model does (with
  * `join_key_filters` off, which copies a conjunct over join keys to the other input), and that the
  * engine returns the model's rows.
  */
class PushDownFiltersConformanceTest extends UniTest:
  import RuntimePlan.*

  private val fixture: Map[String, JSONValue] =
    val text =
      Using.resource(
        Source.fromInputStream(
          getClass.getResourceAsStream("/wvlet/lang/engine/runtime/pushdown-filter-traces.json")
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

  /** A cell of a row: an integer or NULL */
  private def cell(v: JSONValue): Option[Long] =
    v match
      case JSONLong(n) =>
        Some(n)
      case _: JSONNull =>
        None
      case other =>
        fail(s"Expected an integer or null: ${other.toJSON}")

  private def rows(v: JSONValue): List[List[Option[Long]]] = arr(v).map(r => arr(r).map(cell))

  private def inlineTable(rs: List[List[Option[Long]]]): String = rs
    .map(r => r.map(_.fold("null")(_.toString)).mkString("[", ", ", "]"))
    .mkString("[", ", ", "]")

  private val left  = inlineTable(rows(fixture("left")))
  private val right = inlineTable(rows(fixture("right")))
  private val cases = arr(fixture("cases")).map(obj)

  private val options = OptimizerOptions().withValidatePlans(true)
  private val engine  = WvletEngine(optimizer = options)

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def withQuery[A](query: String)(f: Context ?=> Relation => A): A =
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    f(GenSQL.expand(q, summon[Context]))

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
      val bound = RuntimeBinder(planner).bind(r)
      // The model places the conjunct itself; copies over the other input's join keys
      // (`join_key_filters`) have their own model and conformance test (JoinKeyFilterConformanceTest)
      val placing = options.withDisabled(Optimization.JoinKeyFilters)
      (bound, Optimizer(Seq(Normalize, PushDownFilters), placing).run(bound))
    }

  private def rowsOf(query: String): List[List[Option[Long]]] =
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
                  None
                case n: Number =>
                  Some(n.longValue())
                case other =>
                  fail(s"Unexpected value ${other}")
              }
              .toList
          )
        )
        .toList
    }

  private def nodes(p: RuntimePlan): Seq[RuntimePlan] = p +: p.children.flatMap(nodes)

  private def hasFilter(p: RuntimePlan): Boolean = nodes(p).exists(_.isInstanceOf[Filter])

  private def sorted(rs: List[List[Option[Long]]]): List[String] =
    rs.map(_.map(_.fold("null")(_.toString)).mkString(",")).sorted

  private def joinKind(kind: String): JoinKind =
    kind match
      case "left" =>
        JoinKind.Left
      case "right" =>
        JoinKind.Right
      case "full" =>
        JoinKind.Full
      case other =>
        fail(s"Unexpected join kind ${other}")

  /** Checks where the WHERE conjunct of the query ended up */
  private def checkPlace(p: RuntimePlan, kind: String, place: String): Unit =
    if kind == "inner" then
      // The conjunct is one more condition of the inner join tree, which ReorderJoins places
      // (along with the equalities to constants the pass infers)
      place shouldBe "cond"
      nodes(p).exists(_.isInstanceOf[InnerJoins]) shouldBe true
      hasFilter(p) shouldBe false
    else
      val j = nodes(p)
        .collectFirst {
          case j: Join if j.kind == joinKind(kind) =>
            j
        }
        .getOrElse(fail(s"No ${kind} join in\n${p.explain}"))
      val above = nodes(p).takeWhile(_ ne j).exists(_.isInstanceOf[Filter])
      (above, hasFilter(j.left), hasFilter(j.right)) shouldBe (
        place match
          case "left" =>
            (false, true, false)
          case "right" =>
            (false, false, true)
          case "above" =>
            (true, false, false)
          case other =>
            fail(s"Unexpected placement ${other} for a ${kind} join")
      )

  test("place each conjunct over a join as the model does, and return the model's rows") {
    cases.size shouldBe 88
    for c <- cases do
      val kind  = str(c("kind"))
      val place = str(c("place"))
      val join  =
        if kind == "inner" then
          "join"
        else
          s"${kind} join"
      // The projection comes first, so that the conjunct reaches the pass as a filter over it
      // (the binder would otherwise bind a WHERE over an inner join into its conditions)
      val query =
        s"""from ${left} as l(a, b)
           |${join} { from ${right} as r(c, d) } on ${str(c("on"))}
           |select a, b, c, d
           |where ${str(c("where"))}""".stripMargin
      val (b, p) = pushed(query)
      debug(s"${query}\n${p.explain}")
      try
        // A TRUE conjunct that reads no column is dropped, by the binder or by the pass
        if c("trueConstant") == JSONBoolean(true) then
          hasFilter(p) shouldBe false
        else
          hasFilter(b) shouldBe true
          checkPlace(p, kind, place)
        sorted(rowsOf(query)) shouldBe sorted(rows(c("rows")))
      catch
        case e: Throwable =>
          warn(
            s"Mismatch for ${kind} join on ${str(c("on"))} where ${str(c("where"))} (${place}):\n${p
                .explain}"
          )
          throw e
    end for
  }

end PushDownFiltersConformanceTest
