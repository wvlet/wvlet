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
  * `dictionary_group_keys`: Parquet scans keep the dictionary codes of strings, and aggregations by
  * them give the same results as grouping by the strings
  */
class EngineDictionaryGroupKeysTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)
  private val on      = WvletEngine(optimizer = options)
  private val off     = on.withOptimizer(options.withDisabled(Optimization.DictionaryGroupKeys))

  /**
    * 10000 rows in row groups of 2048, with dictionary-encoded strings (some NULLs) whose
    * dictionaries differ by row group, and a high-cardinality string
    */
  private lazy val file: String =
    val dir = Paths.get("target/engine-test").toAbsolutePath
    Files.createDirectories(dir)
    val path = dir.resolve("dictionary_group_keys.parquet")
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
             |    'n' || range::varchar as name,
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

  /** Whether each scan of the physical plan asks for dictionary codes */
  private def scanCodes(e: WvletEngine, query: String): Seq[Boolean] =
    def collect(op: Operator): Seq[Boolean] =
      (
        op match
          case s: ScanOp =>
            Seq(s.dictionaryCodes)
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

  test("ask scans for dictionary codes only with dictionary_group_keys") {
    val q = "from $t where id > 10 group by flag agg _.count"
    scanCodes(on, q) shouldContain true
    scanCodes(on, q).contains(false) shouldBe false
    scanCodes(off, q).contains(true) shouldBe false
  }

  test("decode dictionary codes only when asked") {
    val src   = ParquetSource("t", file)
    val coded = src.scan(ScanSpec(Some(Seq("flag", "name")), dictionaryCodes = true)).next()
    val flag  = coded.column(0).asInstanceOf[StrVec]
    flag.codes shouldNotBe null
    flag.dictSize shouldBe 3
    (0 until flag.size).foreach { i =>
      if flag.isNull(i) then
        flag.codes(i) shouldBe -1
    }
    src.scan(ScanSpec(Some(Seq("flag")))).next().column(0).asInstanceOf[StrVec].codes shouldBe null
  }

  test("group by dictionary-coded strings as by their values") {
    val r = sameRows("""from $t
        |group by flag, status
        |agg _.count, qty.sum, id.max""".stripMargin)
    r.size shouldBe 16
  }

  test("group by dictionary codes after a filter and with a high-cardinality key") {
    sameRows("""from $t
        |where id % 3 != 1
        |group by status
        |agg _.count""".stripMargin).size shouldBe 4
    sameRows("""from $t
        |group by flag, name
        |agg _.count""".stripMargin).size shouldBe 10000
  }

  test("group by dictionary codes carried through a join") {
    sameRows("""from $t as a
        |join $t as b on a.id = b.id
        |where b.id < 40
        |group by a.flag
        |agg _.count""".stripMargin).size shouldBe 4
  }

end EngineDictionaryGroupKeysTest
