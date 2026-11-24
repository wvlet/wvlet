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
import wvlet.lang.engine.expr.AndExpr
import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.runtime.Normalize
import wvlet.lang.engine.runtime.Optimizer
import wvlet.lang.engine.runtime.RuntimePlan
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
  * Replays the traces of the Lean model of `FactorOr` (formal/, exported by
  * `lake exe factor-or-traces`) against the engine. The model proves that factoring the conjuncts
  * common to every branch out of an `or` keeps the condition's value under three-valued logic,
  * provided conjuncts with the same signature have the same value, and that the old `InList.sig`
  * (which left out a NULL value and joined the values with commas) broke this. For each traced
  * condition, this test checks that `Normalize` factors it as the model does, into as many
  * conjuncts, and that the engine returns the model's rows for the condition (with FactorOr on and
  * off) and for its factored form, as DuckDB does.
  */
class FactorOrConformanceTest extends UniTest:

  private val fixture: Map[String, JSONValue] =
    val text =
      Using.resource(
        Source.fromInputStream(
          getClass.getResourceAsStream("/wvlet/lang/engine/runtime/factor-or-traces.json")
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
        fail(s"Expected a string: ${other.toJSON}")

  /**
    * A cell as a Wvlet literal: an integer, a boolean, a quoted string (so that the text `'null'`
    * is not NULL), or NULL
    */
  private def cell(v: JSONValue): String =
    v match
      case JSONLong(n) =>
        n.toString
      case JSONBoolean(b) =>
        b.toString
      case JSONString(s) =>
        s"'${s}'"
      case _: JSONNull =>
        "null"
      case other =>
        fail(s"Unexpected cell: ${other.toJSON}")

  private def sorted(rs: Seq[List[String]]): List[String] = rs.map(_.mkString("|")).sorted.toList

  private val tableRows = arr(fixture("table")).map(r => arr(r).map(cell))

  private def inlineTable(rows: Seq[Seq[String]]): String = rows
    .map(_.mkString("[", ", ", "]"))
    .mkString("[", ", ", "]")

  private val table = inlineTable(tableRows)

  // The table split into two inputs joined on the row id: `l(i, a, v)` with v = b and
  // `r(i, v, s)` with v = c, so that the traced `b` and `c` are columns with the same name
  private val leftTable = inlineTable(tableRows.zipWithIndex.map((r, i) => i.toString :: r.take(2)))
  private val rightTable = inlineTable(
    tableRows.zipWithIndex.map((r, i) => i.toString :: r.drop(2))
  )

  private val cases = arr(fixture("cases")).map(obj)

  private val options     = OptimizerOptions().withValidatePlans(true)
  private val conformance = EngineConformance(".", Nil, _ => (), options)
  private val engine      = conformance.engine
  // Bind without the rule, so that Normalize alone factors the bound condition
  private val unfactored = engine.withOptimizer(options.withDisabled(Optimization.FactorOr))

  override def afterAll: Unit = conformance.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def withQuery[A](query: String)(f: Context ?=> Relation => A): A =
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    f(GenSQL.expand(q, summon[Context]))

  private def query(where: String): String = s"from ${table} as t(a, b, c, s) where ${where}"

  private def rowsOf(e: WvletEngine, q: String): List[List[String]] =
    withQuery(q) { r =>
      e.execute(r)
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
                case s: String =>
                  s"'${s}'"
                case other =>
                  other.toString
              }
              .toList
          )
        )
        .toList
    }

  private def filterOf(p: RuntimePlan): RuntimePlan.Filter =
    p match
      case f: RuntimePlan.Filter =>
        f
      case other =>
        other.children.map(filterOf).headOption.getOrElse(fail(s"No filter in\n${p.explain}"))

  private def conjuncts(e: VExpr): List[VExpr] =
    e match
      case AndExpr(l, r) =>
        conjuncts(l) ++ conjuncts(r)
      case other =>
        List(other)

  test("factor each traced condition as the model does") {
    cases.size shouldBe 17
    for c <- cases do
      val where = str(c("where"))
      debug(where)
      val p         = withQuery(query(where))(unfactored.runtimePlan(_))
      val o         = Optimizer.only(Normalize, options)
      val optimized = o.run(p)
      val factored  = c("factored") == JSONBoolean(true)
      o.firedRules.contains("factor_or") shouldBe factored
      conjuncts(filterOf(optimized).cond.expr).size shouldBe cell(c("conjuncts")).toInt
  }

  test("return the model's rows for each traced condition and its factored form") {
    for c <- cases do
      val want = sorted(arr(c("rows")).map(r => arr(r).map(cell)))
      for where <- Seq(str(c("where")), str(c("factoredWhere"))) do
        val q = query(where)
        debug(q)
        for e <- Seq(engine, unfactored) do
          sorted(rowsOf(e, q)) shouldBe want
        withQuery(q)(conformance.assertMatches(q, _))
  }

  // Conditions outside the model's (functions, LIKE), with whether FactorOr should factor them.
  // Before the signature audit, the first and third cases' conjuncts printed alike, so FactorOr
  // took the first branch's conjunct out of both; the second is a real common conjunct
  private val unmodeled = Seq(
    // A literal whose text reads as two arguments
    "(coalesce(s, 'x:string),lit(y') = 'x' and b) or (coalesce(s, 'x', 'y') = 'x' and c)" -> false,
    // A literal with a comma, common to both branches
    "(coalesce(s, 'x,y') = 'x,y' and b) or (coalesce(s, 'x,y') = 'x,y' and c)" -> true,
    // An escape character: `'x%' escape 'x'` matches `%` only
    "(s like 'x%' escape 'x' and b) or (s like 'x%' and c)" -> false
  )

  test("tell conditions apart whose literals print like other expressions") {
    for (where, factors) <- unmodeled do
      val q = query(where)
      debug(q)
      val p = withQuery(q)(unfactored.runtimePlan(_))
      val o = Optimizer.only(Normalize, options)
      o.run(p)
      o.firedRules.contains("factor_or") shouldBe factors
      val rows = sorted(rowsOf(engine, q))
      sorted(rowsOf(unfactored, q)) shouldBe rows
      withQuery(q)(conformance.assertMatches(q, _))
  }

  private val joined   = s"from ${leftTable} as l(i, a, v)"
  private val right    = s"${rightTable} as r(i, v, s)"
  private val asTable  = " select l.a, l.v as b, r.v as c, r.s"
  private val trueSubq = "exists { from [[1]] as u(x) }"

  /** The traced condition over the join in each context, and the rows it gives */
  private def joinQueries(c: Map[String, JSONValue]): Seq[(String, List[String])] =
    val w    = str(c("joinWhere"))
    val rows = arr(c("rows")).map(r => arr(r).map(cell))
    // A left join keeps each left row, with the right columns when the condition is TRUE
    val outer = tableRows.map(r =>
      if rows.contains(r) then
        r
      else
        r.take(2) ++ List("null", "null")
    )
    Seq(
      // Bound unfactored, factored by Normalize
      s"${joined} join ${right} on l.i = r.i and (${w})${asTable}"      -> sorted(rows),
      s"${joined} left join ${right} on l.i = r.i and (${w})${asTable}" -> sorted(outer),
      // Factored by the planner (Planner.factorOr), as conditions with subqueries are
      s"${joined} left join ${right} on l.i = r.i where ${trueSubq} and (${w})${asTable}" ->
        sorted(rows),
      s"${joined} left join ${right} on l.i = r.i and ${trueSubq} and (${w})${asTable}" ->
        sorted(outer),
      s"${joined} join ${right} on l.i = r.i where ${str(c("existsBranchesWhere"))}${asTable}" ->
        sorted(rows),
      s"${joined} where exists { from ${right} where r.i = l.i and (${w}) } select l.a, l.v as b" ->
        sorted(rows.map(_.take(2)))
    )

  test("return the model's rows for each traced condition over a join") {
    for
      c         <- cases
      (q, want) <- joinQueries(c)
    do
      debug(q)
      for e <- Seq(engine, unfactored) do
        sorted(rowsOf(e, q)) shouldBe want
      withQuery(q)(conformance.assertMatches(q, _))
  }

end FactorOrConformanceTest
