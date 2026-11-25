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
import wvlet.lang.engine.exec.FilterOp
import wvlet.lang.engine.exec.Operator
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.vector.*
import wvlet.lang.model.plan.*
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Paths
import java.sql.DriverManager

/**
  * `dictionary_filters`: filter conditions over one dictionary-coded string column run once per
  * dictionary entry and keep the same rows as running per row
  */
class EngineDictionaryFiltersTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)
  private val on      = WvletEngine(optimizer = options)
  private val off     = on.withOptimizer(options.withDisabled(Optimization.DictionaryFilters))
  // Without scan row filters, string = and IN stay in the engine's filters too
  private val onNoRow  = on.withOptimizer(options.withDisabled(Optimization.ScanRowFilters))
  private val offNoRow = on.withOptimizer(
    options.withDisabled(Optimization.ScanRowFilters, Optimization.DictionaryFilters)
  )

  /**
    * 10000 rows in row groups of 2048, with dictionary-encoded strings (some NULLs) whose
    * dictionaries differ by row group, and a high-cardinality string
    */
  private lazy val file: String =
    val dir = Paths.get("target/engine-test").toAbsolutePath
    Files.createDirectories(dir)
    val path = dir.resolve("dictionary_filters.parquet")
    Files.deleteIfExists(path)
    Class.forName("org.duckdb.DuckDBDriver")
    val conn = DriverManager.getConnection("jdbc:duckdb:")
    try
      val st = conn.createStatement()
      try st.execute(s"""copy (
             |  select
             |    range::bigint as id,
             |    case when range % 17 = 0 then null else 'Brand#' || ((range * 7) % 5)::varchar end as brand,
             |    chr(70 + ((range // 2048 + range) % 4)::integer) || ' POLISHED' as ptype,
             |    'n' || range::varchar as name
             |  from range(10000)
             |) to '${path}' (format parquet, row_group_size 2048)""".stripMargin)
      finally st.close()
    finally
      conn.close()
    path.toString

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def run[A](query: String)(body: Relation => Context ?=> A): A =
    val unit           = CompilationUnit.fromWvletString(query.replace("$t", s"'${file}'"))
    val r              = compiler.compileSingleUnit(unit)
    given ctx: Context = r.context
    val q              = queryOf(unit.executionPlan).getOrElse(fail(s"No query in ${query}"))
    body(q)

  private def rows(e: WvletEngine, query: String): Seq[String] =
    run(query)(q => e.execute(q).rows.map(_.mkString(",")).toSeq.sorted)

  private def filterSettings(e: WvletEngine, query: String): Seq[Boolean] =
    def collect(op: Operator): Seq[Boolean] =
      (
        op match
          case f: FilterOp =>
            Seq(f.dictionaryFilters)
          case _ =>
            Nil
      ) ++ op.children.flatMap(collect)
    run(query)(q => collect(e.physicalPlan(GenSQL.expand(q, summon[Context]))))

  private def queryOf(e: ExecutionPlan): Option[Relation] =
    e match
      case ExecuteQuery(q: Relation) =>
        Some(q)
      case ExecuteTasks(ts) =>
        ts.iterator.flatMap(queryOf).nextOption()
      case _ =>
        None

  private def sameRows(query: String): Seq[String] =
    val r = rows(on, query)
    r shouldBe rows(off, query)
    rows(onNoRow, query) shouldBe r
    rows(offNoRow, query) shouldBe r
    r

  test("set dictionary_filters on filters only when enabled") {
    val q = "from $t where ptype not like 'F%'"
    filterSettings(on, q) shouldContain true
    filterSettings(off, q).contains(true) shouldBe false
  }

  test("filter by LIKE, NOT LIKE, and != per dictionary entry") {
    sameRows("from $t where ptype like 'G%' select id").size shouldBe 2500
    sameRows("from $t where ptype not like 'G%' select id").size shouldBe 7500
    val ne = sameRows("from $t where brand != 'Brand#3' select id")
    ne.size shouldBe
      rows(off, "from $t where brand is not null and brand != 'Brand#3' select id").size
  }

  test("filter by = and IN per dictionary entry, with NULLs") {
    sameRows("from $t where brand = 'Brand#1' select id").nonEmpty shouldBe true
    sameRows("from $t where brand in ('Brand#1', 'Brand#4') select id").nonEmpty shouldBe true
    sameRows("from $t where brand is null or brand not in ('Brand#2') select id").nonEmpty shouldBe
      true
  }

  test("filter by per-entry parts mixed with other columns") {
    sameRows("""from $t
        |where (brand = 'Brand#1' and ptype like 'H%' and id < 5000)
        |   or (brand = 'Brand#2' and ptype not like 'F%' and id >= 5000)
        |select id""".stripMargin).nonEmpty shouldBe true
    // A high-cardinality column has more entries than half the rows: evaluated per row
    sameRows("from $t where name like 'n1%' and brand != 'Brand#0' select id").nonEmpty shouldBe
      true
  }

  test("filter dictionary-coded strings carried through a join") {
    sameRows("""from $t as a
        |join $t as b on a.id = b.id
        |where b.ptype like 'F%' and a.brand != 'Brand#1'
        |select a.id""".stripMargin).nonEmpty shouldBe true
  }

  test("evaluate a condition once per entry and map results back to rows") {
    val wrapped = DictionaryPredicates.wrap(
      AndExpr(
        LikeExpr(ColRef(1, PType.Str, "s"), "a%", negate = false),
        Compare(CmpOp.Gt, ColRef(0, PType.Int64, "x"), Const(1L, PType.Int64))
      )
    )
    wrapped match
      case AndExpr(p: PerDictEntry, _: Compare) =>
        p.column shouldBe 1
      case other =>
        fail(s"Unexpected ${other.sig}")
    // ANDs and ORs over one column stay outside, so Selection can short-circuit them
    val col = ColRef(0, PType.Str, "s")
    DictionaryPredicates.wrap(
      OrExpr(LikeExpr(col, "a%", negate = false), LikeExpr(col, "b%", negate = false))
    ) match
      case OrExpr(_: PerDictEntry, _: PerDictEntry) =>
      case other =>
        fail(s"Unexpected ${other.sig}")
    // Codes: "ab" = 0, "b" = 1; row 3 is NULL and row 5 a PLAIN value
    val s = StrVec(
      Array("ab", "b", "ab", null, "b", "ax", "ab", "b"),
      Array(false, false, false, true, false, false, false, false),
      Array(0, 1, 0, -1, 1, -1, 0, 1),
      2
    )
    val xs = LongVec(PType.Int64, Array.fill(8)(5L), null)
    val b  = Batch(Array[Vec](xs, s), 8)
    val r  = wrapped.eval(b).asInstanceOf[BoolVec]
    (0 until 8).map(i =>
      if r.isNull(i) then
        "null"
      else
        r.get(i).toString
    ) shouldBe Seq("true", "false", "true", "null", "false", "true", "true", "false")
    // More entries than half the rows: the same result per row
    val distinct = StrVec(Array("ab", "b", "ac"), null, Array(0, 1, 2), 3)
    val lk = DictionaryPredicates.wrap(LikeExpr(ColRef(0, PType.Str, "s"), "a%", negate = true))
    val r2 = lk.eval(Batch(Array[Vec](distinct), 3)).asInstanceOf[BoolVec]
    (0 until 3).map(r2.get(_)) shouldBe Seq(false, true, false)
  }

end EngineDictionaryFiltersTest
