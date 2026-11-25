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
  * Replays the traces of the Lean model of moving a LEFT join above inner joins (formal/, exported
  * by `lake exe left-join-commute-traces`) against the shipped [[ReorderJoins]] pass
  * (`commute_left_joins`). The model proves that `(A ⟕ B) ⋈ C` has the rows of `(A ⋈ C) ⟕ B` when
  * the inner join's condition reads no column of `B`. This test checks, for each traced query, that
  * the optimized plan moves the LEFT join above the join with the third table exactly when the
  * model does, and that the engine returns the model's rows.
  */
class LeftJoinCommuteConformanceTest extends UniTest:
  import RuntimePlan.*

  private val fixture: Map[String, JSONValue] =
    val text =
      Using.resource(
        Source.fromInputStream(
          getClass.getResourceAsStream("/wvlet/lang/engine/runtime/left-join-commute-traces.json")
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
  private val third = inlineTable(rows(fixture("third")))
  private val cases = arr(fixture("cases")).map(obj)

  private val engine = WvletEngine(optimizer = OptimizerOptions().withValidatePlans(true))

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def withQuery[A](query: String)(f: Context ?=> Relation => A): A =
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    f(GenSQL.expand(q, summon[Context]))

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

  private def sorted(rs: List[List[Option[Long]]]): List[String] =
    rs.map(_.map(_.fold("null")(_.toString)).mkString(",")).sorted

  /** Whether a LEFT join of the plan joins over the inner (or cross) join with the third table */
  private def leftJoinAbove(p: RuntimePlan): Boolean = nodes(p).exists {
    case j: Join if j.kind == JoinKind.Left =>
      nodes(j.left).exists {
        case i: Join =>
          i.kind == JoinKind.Inner || i.kind == JoinKind.Cross
        case _ =>
          false
      }
    case _ =>
      false
  }

  test("move a LEFT join above an inner join as the model does, and return the model's rows") {
    cases.size shouldBe 14
    for c <- cases do
      val moves = c("commutes") == JSONBoolean(true)
      // The WHERE binds into the conditions of the inner join tree, as in TPC-DS q80
      val query =
        s"""from ${left} as l(a, b)
           |left join { from ${right} as r(c, d) } on ${str(c("on"))}, ${third} as t(e)
           |where ${str(c("where"))}
           |select a, b, c, d, e""".stripMargin
      val p = withQuery(query)(engine.runtimePlan(_))
      debug(s"${query}\n${p.explain}")
      try
        leftJoinAbove(p) shouldBe moves
        sorted(rowsOf(query)) shouldBe sorted(rows(c("rows")))
      catch
        case e: Throwable =>
          warn(
            s"Mismatch on ${str(c("on"))} where ${str(c("where"))} (moves: ${moves}):\n${p.explain}"
          )
          throw e
    end for
  }

end LeftJoinCommuteConformanceTest
