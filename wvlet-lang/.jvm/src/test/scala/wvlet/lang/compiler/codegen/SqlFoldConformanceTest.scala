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
package wvlet.lang.compiler.codegen

import wvlet.lang.api.Span.NoSpan
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.codegen.SqlGenerator.FoldOp
import wvlet.lang.compiler.codegen.SqlGenerator.SQLBlock
import wvlet.lang.compiler.parser.WvletParser
import wvlet.lang.model.expr.*
import wvlet.lang.model.plan.EmptyRelation
import wvlet.lang.model.plan.Filter
import wvlet.uni.json.JSON
import wvlet.uni.json.JSON.JSONArray
import wvlet.uni.json.JSON.JSONBoolean
import wvlet.uni.json.JSON.JSONObject
import wvlet.uni.json.JSON.JSONString
import wvlet.uni.json.JSON.JSONValue
import wvlet.uni.test.UniTest

import scala.io.Source
import scala.util.Using

/**
  * Replays the traces of the Lean model of SELECT-block folding (formal/, exported by
  * `lake exe sql-fold-traces`) against [[SqlGenerator]]. The model proves that folding a pipeline
  * into nested SELECT blocks keeps its rows; this test checks that the generator decides as the
  * model does:
  *   - every reachable block shape accepts exactly the operators the model accepts, and
  *   - every Wvlet pipeline of up to three operators compiles into as many SELECT blocks as the
  *     model builds.
  */
class SqlFoldConformanceTest extends UniTest:

  private val fixture: Map[String, JSONValue] =
    val text =
      Using.resource(
        Source.fromInputStream(
          getClass.getResourceAsStream("/wvlet/lang/compiler/codegen/sql-fold-traces.json")
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

  private def bool(v: JSONValue): Boolean =
    v match
      case JSONBoolean(b) =>
        b
      case other =>
        fail(s"Expected a boolean: ${other.toJSON}")

  private def cases(name: String): List[Map[String, JSONValue]] = arr(fixture(name)).map(obj)

  private val kinds = List(
    "filter",
    "filterUnsafe",
    "having",
    "project",
    "distinct",
    "dedup",
    "group",
    "agg",
    "sort",
    "sortUnsafe",
    "limit",
    "offset"
  )

  private def id(name: String): Identifier = UnquotedIdentifier(name, NoSpan)
  private val dummyFilter                  = Filter(EmptyRelation(NoSpan), id("p"), NoSpan)

  /**
    * A block holding one placeholder clause per flag of the model's shape. `aggregated` stands in
    * for the grouping keys, to cover a global aggregation or count
    */
  private def blockOf(shape: Map[String, JSONValue], aggregated: Boolean): SQLBlock =
    def has(key: String): Boolean = bool(shape(key))
    SQLBlock(
      whereFilter =
        if has("where") then
          List(dummyFilter)
        else
          Nil
      ,
      groupingKeys =
        if has("group") && !aggregated then
          List(UnresolvedGroupingKey(NameExpr.EmptyName, id("k"), NoSpan))
        else
          Nil
      ,
      aggregated = has("group") && aggregated,
      having =
        if has("having") then
          List(dummyFilter)
        else
          Nil
      ,
      selectItems =
        if has("select") then
          List(SingleColumn(NameExpr.EmptyName, id("a"), NoSpan))
        else
          Nil
      ,
      isDistinct = has("distinct"),
      orderBy =
        if has("orderBy") then
          List(SortItem(id("a"), None, None, NoSpan))
        else
          Nil
      ,
      offset =
        if has("offset") then
          Some(LongLiteral(1, "1", NoSpan))
        else
          None
      ,
      limit =
        if has("limit") then
          Some(LongLiteral(1, "1", NoSpan))
        else
          None
    )

  end blockOf

  /**
    * The operator the generator presents for a model kind. The generator derives the scope flag of
    * a projection from the block's WHERE conjuncts; the model keeps it on the shape
    */
  private def foldOp(kind: String, shape: Map[String, JSONValue]): FoldOp =
    val whereSafe = !bool(shape("where")) || bool(shape("whereSafe"))
    kind match
      case "filter" | "filterUnsafe" =>
        FoldOp.Filter
      case "having" =>
        FoldOp.Having
      case "project" =>
        FoldOp.Project(whereSafe)
      case "distinct" =>
        FoldOp.Distinct(whereSafe)
      case "dedup" =>
        FoldOp.Dedup
      case "group" =>
        FoldOp.Group
      case "agg" =>
        FoldOp.Agg
      case "sort" =>
        FoldOp.Sort(true)
      case "sortUnsafe" =>
        FoldOp.Sort(false)
      case "limit" =>
        FoldOp.Limit
      case "offset" =>
        FoldOp.Offset

  test("accept exactly the operators the model accepts on every reachable block shape") {
    val decisions = cases("decisions")
    decisions.size shouldBe 384
    for
      c          <- decisions
      aggregated <- List(false, true)
    do
      val shape    = obj(c("shape"))
      val accepted = arr(c("accepts")).map(str).toSet
      for kind <- kinds do
        val block = blockOf(shape, aggregated)
        if block.accepts(foldOp(kind, shape)) != accepted.contains(kind) then
          fail(
            s"${kind} on ${c("shape").toJSON} (aggregated=${aggregated}): expected ${accepted
                .contains(kind)}"
          )
  }

  /**
    * Wvlet text for each kind. The projection computes `c` and passes `a` through, so `where a` and
    * `order by a` are scope-safe and `where c` and `order by c` are not
    */
  private def wvletOf(kind: String, prev: Option[String]): String =
    kind match
      case "filter" | "having" =>
        "where a > 0"
      case "filterUnsafe" =>
        "where c > 0"
      case "project" =>
        "select a, b + 1 as c"
      case "distinct" =>
        "select distinct a, b + 1 as c"
      case "dedup" =>
        "dedup"
      case "group" =>
        "group by a"
      case "agg" if prev.contains("group") || prev.contains("having") =>
        // `group by a agg ...` aggregates per group as one node; a count counts the groups
        "count"
      case "agg" =>
        "agg b.sum as s"
      case "sort" =>
        "order by a"
      case "sortUnsafe" =>
        "order by c"
      case "limit" =>
        "limit 3"
      case other =>
        fail(s"No Wvlet syntax for ${other}")

  /**
    * Pipelines that Wvlet can write one node per operator. Wvlet has no `offset`, a filter directly
    * after a bare `group by` is always HAVING, and a projection directly after it is an aggregation
    */
  private def expressible(ops: List[String]): Boolean =
    !ops.contains("offset") &&
      ops
        .zip(ops.drop(1))
        .forall { (a, b) =>
          !(a == "group" && Set("filter", "filterUnsafe", "project").contains(b))
        }

  private val selectKeyword = "(?i)\\bselect\\b".r

  private def generateSQL(wv: String): String =
    val unit = CompilationUnit.fromWvletString(wv)
    val plan = WvletParser(unit).parse()
    SqlGenerator(CodeFormatterConfig(sqlDBType = DBType.DuckDB)).print(plan)

  test("compile every pipeline into as many SELECT blocks as the model builds") {
    val pipelines = cases("pipelines")
    pipelines.size shouldBe 1486
    var replayed = 0
    for c <- pipelines do
      val ops = arr(c("ops")).map(str)
      if expressible(ops) then
        val prevs    = None :: ops.map(Some(_))
        val wv       = ("from t" :: ops.zip(prevs).map((k, p) => wvletOf(k, p))).mkString(" ")
        val sql      = generateSQL(wv)
        val expected = arr(c("blocks")).size
        val actual   = selectKeyword.findAllIn(sql).size
        if actual != expected then
          fail(s"${wv}: expected ${expected} SELECT blocks, but got ${actual}:\n${sql}")
        replayed += 1
    debug(s"Replayed ${replayed} pipelines")
    (replayed > 900) shouldBe true
  }

end SqlFoldConformanceTest
