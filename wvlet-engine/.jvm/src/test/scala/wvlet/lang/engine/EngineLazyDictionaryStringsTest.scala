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
import wvlet.lang.engine.catalog.ParquetSource
import wvlet.lang.engine.catalog.ScanSpec
import wvlet.lang.engine.exec.Operator
import wvlet.lang.engine.exec.ScanOp
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.vector.StrVec
import wvlet.lang.model.plan.*
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Paths
import java.sql.DriverManager

/**
  * `lazy_dictionary_strings`: Parquet scans hold dictionary-encoded strings as their codes and
  * entries, and queries over them give the same results as building each row's String
  */
class EngineLazyDictionaryStringsTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)
  private val on      = WvletEngine(optimizer = options)
  private val off     = on.withOptimizer(options.withDisabled(Optimization.LazyDictionaryStrings))

  /**
    * 10000 rows in row groups of 2048, with dictionary-encoded strings (some NULLs), and a
    * high-cardinality string whose dictionary may fall back to PLAIN pages
    */
  private lazy val file: String =
    val dir = Paths.get("target/engine-test").toAbsolutePath
    Files.createDirectories(dir)
    val path = dir.resolve("lazy_dictionary_strings.parquet")
    Files.deleteIfExists(path)
    Class.forName("org.duckdb.DuckDBDriver")
    val conn = DriverManager.getConnection("jdbc:duckdb:")
    try
      val st = conn.createStatement()
      try st.execute(s"""copy (
             |  select
             |    range::bigint as id,
             |    case when range % 17 = 0 then null else chr(65 + ((range * 7) % 3)::integer) end as flag,
             |    chr(70 + ((range // 2048 + range) % 4)::integer) as status,
             |    case when range < 3000 then 'n' || (range % 10)::varchar
             |      else 'name-' || range::varchar || repeat('x', (range % 7)::integer) end as name,
             |    (range % 50)::decimal(12, 2) as qty
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

  /** Whether each scan of the physical plan holds dictionary strings as their entries */
  private def scanEntries(e: WvletEngine, query: String): Seq[Boolean] =
    def collect(op: Operator): Seq[Boolean] =
      (
        op match
          case s: ScanOp =>
            Seq(s.lazyStrings)
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
    r

  test("hold dictionary strings as entries with lazy_dictionary_strings") {
    val q = "from $t where id > 10 group by flag agg _.count"
    scanEntries(on, q) shouldContain true
    scanEntries(on, q).contains(false) shouldBe false
    scanEntries(off, q).contains(true) shouldBe false
  }

  test("read the same strings and codes from entries-backed vectors") {
    val src                                    = ParquetSource("t", file)
    val cols                                   = Some(Seq("id", "flag", "status", "name"))
    def strings(spec: ScanSpec): Seq[Seq[Any]] =
      src
        .scan(spec)
        .flatMap { b =>
          (0 until b.size).map(i => (0 until b.numColumns).map(b.column(_).get(i)))
        }
        .toSeq
    def values(spec: ScanSpec): Seq[Seq[String]] =
      src
        .scan(spec)
        .flatMap(b => (1 until b.numColumns).map(b.column(_).asInstanceOf[StrVec].values.toSeq))
        .toSeq
    for entries <- Seq(true, false) do
      val lazySpec = ScanSpec(
        cols,
        dictionaryCodes = true,
        dictionaryEntries = entries,
        lazyStrings = true
      )
      strings(lazySpec) shouldBe strings(ScanSpec(cols))
      values(lazySpec) shouldBe values(ScanSpec(cols))
    val flag = src
      .scan(ScanSpec(cols, dictionaryCodes = true, lazyStrings = true))
      .next()
      .column(1)
      .asInstanceOf[StrVec]
    flag.holdsEntries shouldBe true
    flag.dictSize shouldBe 3
  }

  test("gather entries-backed vectors without building their strings") {
    val v = StrVec.ofEntries(
      Array("a", "b"),
      Array(1, -1, 0, 1),
      Array(false, true, false, false),
      4
    )
    val g = v.gather(Array(3, 2, -1, 1), 4).asInstanceOf[StrVec]
    g.holdsEntries shouldBe true
    (0 until 4).map(g.get) shouldBe Seq("b", "a", null, null)
    g.codes.toSeq shouldBe Seq(1, 0, -1, -1)
    g.values.toSeq shouldBe Seq("b", "a", null, null)
    v.str(0) shouldBe "b"
    v.values.toSeq shouldBe Seq("b", null, "a", "b")
  }

  test("group and project dictionary strings as their values") {
    sameRows("""from $t
        |group by flag, status
        |agg _.count, qty.sum, id.max""".stripMargin).size shouldBe 16
    sameRows("""from $t
        |where id % 3 != 1
        |select id, flag, status, name""".stripMargin).size shouldBe 6667
    sameRows("""from $t
        |group by name
        |agg _.count""".stripMargin).size shouldBe 7010
  }

  test("filter on dictionary strings read as entries") {
    sameRows("""from $t
        |where flag = 'B' and name like 'n%'
        |select id, flag, name""".stripMargin)
    sameRows("""from $t
        |where status in ('F', 'G') and id < 5000
        |group by status, flag
        |agg _.count""".stripMargin)
  }

  test("join on dictionary strings read as entries") {
    sameRows("""from $t as a
        |join $t as b on a.name = b.name and a.status = b.status
        |where b.id < 4000
        |group by a.flag
        |agg _.count""".stripMargin)
  }

end EngineLazyDictionaryStringsTest
