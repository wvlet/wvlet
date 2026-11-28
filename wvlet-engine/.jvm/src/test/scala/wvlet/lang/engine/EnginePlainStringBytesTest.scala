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
  * `plain_string_bytes`: Parquet scans hold strings without a dictionary as their UTF-8 bytes, and
  * queries over them give the same results as building each row's String
  */
class EnginePlainStringBytesTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)
  private val on      = WvletEngine(optimizer = options)
  private val off     = on.withOptimizer(options.withDisabled(Optimization.PlainStringBytes))

  /**
    * 10000 rows in row groups of 2048. `comment` is unique per row, so its pages are PLAIN; it has
    * NULLs, empty strings, and multi-byte characters. `flag` is dictionary-encoded
    */
  private lazy val file: String =
    val dir = Paths.get("target/engine-test").toAbsolutePath
    Files.createDirectories(dir)
    val path = dir.resolve("plain_string_bytes.parquet")
    Files.deleteIfExists(path)
    Class.forName("org.duckdb.DuckDBDriver")
    val conn = DriverManager.getConnection("jdbc:duckdb:")
    try
      val st = conn.createStatement()
      try st.execute(s"""copy (
             |  select
             |    range::bigint as id,
             |    chr(65 + (range % 3)::integer) as flag,
             |    case
             |      when range % 13 = 0 then null
             |      when range % 29 = 0 then ''
             |      when range % 5 = 0 then 'c' || range::varchar || ' special ' || md5(range::varchar) || ' requests'
             |      when range % 7 = 0 then '日本語 ' || range::varchar || ' café'
             |      else 'c' || range::varchar || ' ' || md5(range::varchar)
             |    end as comment
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

  private def scanFlags(e: WvletEngine, query: String): Seq[Boolean] =
    def collect(op: Operator): Seq[Boolean] =
      (
        op match
          case s: ScanOp =>
            Seq(s.plainStringBytes)
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

  test("hold PLAIN strings as bytes with plain_string_bytes") {
    val q = "from $t where comment like '%special%' select id"
    scanFlags(on, q) shouldContain true
    scanFlags(off, q).contains(true) shouldBe false
    val src     = ParquetSource("t", file)
    val cols    = Some(Seq("comment", "flag"))
    val batches = src.scan(ScanSpec(cols, plainStringBytes = true)).toSeq
    batches.forall(_.column(0).asInstanceOf[StrVec].holdsBytes) shouldBe true
    batches.exists(_.column(1).asInstanceOf[StrVec].holdsBytes) shouldBe false
    def values(spec: ScanSpec): Seq[Any] =
      src.scan(spec).flatMap(b => (0 until b.size).map(i => b.column(0).get(i))).toSeq
    values(ScanSpec(cols, plainStringBytes = true)) shouldBe values(ScanSpec(cols))
  }

  test("give the same rows as String vectors") {
    sameRows("from $t where comment not like '%special%requests%' agg _.count").head shouldBe "7447"
    sameRows("from $t where comment like 'c1%' select id, comment")
    sameRows("from $t where comment like '%café' select id")
    sameRows("from $t where comment like '日本語%' and flag = 'A' select id, comment")
    sameRows("from $t where comment like '%' agg _.count")
    sameRows("from $t where comment like 'c_0 %' select id")
    sameRows("from $t where comment = '' select id")
    sameRows("from $t where id % 100 = 3 select id, comment")
    sameRows("from $t group by flag agg comment.max, comment.min, _.count")
    sameRows("from $t order by comment desc limit 20 select id, comment")
    sameRows(
      "from $t as a join $t as b on a.comment = b.comment where a.id < 500 select a.id, b.comment"
    )
  }

end EnginePlainStringBytesTest
