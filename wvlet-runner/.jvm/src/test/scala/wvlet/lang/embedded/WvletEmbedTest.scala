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
package wvlet.lang.embedded

import wvlet.lang.api.StatusCode
import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.SourceIO
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.compiler.codegen.GenSQL
import wvlet.uni.log.LogLevel
import wvlet.uni.test.UniTest

import java.math.MathContext
import java.sql.DriverManager
import scala.util.Using

/** [[WvletEmbed]]: the public API for running Wvlet queries in-process */
class WvletEmbedTest extends UniTest:
  private val tpchData   = "spec/tpch/data"
  private val tpchTables = Seq(
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier"
  )

  private def withEngine[A](body: WvletEmbed => A): A = Using.resource(WvletEmbed.open())(body)

  test("should run a query over inline rows") {
    withEngine { engine =>
      val r = engine.query("""from [[1, 'a'], [2, 'b'], [3, 'c']] as t(id, name)
                             |where id >= 2
                             |select id, name""".stripMargin)
      r.columns shouldBe
        IndexedSeq(WvletEmbedColumn("id", "int"), WvletEmbedColumn("name", "string"))
      r.rowCount shouldBe 2
      r.rows shouldBe IndexedSeq(IndexedSeq(2L, "b"), IndexedSeq(3L, "c"))
      r.get(1, 1) shouldBe "c"
    }
  }

  test("should keep columns of the same name") {
    withEngine { engine =>
      val r = engine.query("from [[1, 2]] as t(a, b) select a, b as a")
      r.columns.map(_.name) shouldBe IndexedSeq("a", "a")
      r.rows shouldBe IndexedSeq(IndexedSeq(1L, 2L))
    }
  }

  test("should return the result as JSON") {
    withEngine { engine =>
      val r = engine.query("from [[1, 'a', null]] as t(id, name, v) select id, name, v")
      r.toJson shouldContain """"rows":[[1,"a",null]]"""
      r.toJson shouldContain """{"name":"id","type":"int"}"""
    }
  }

  test("should type registered tables at compile time") {
    withEngine { engine =>
      engine.registerTable("nation", s"${tpchData}/nation.parquet")
      engine.tableNames shouldContain "nation"
      val r = engine.query("from nation where n_regionkey = 0 select n_name order by n_name")
      r.rows.map(_.head) shouldBe
        IndexedSeq("ALGERIA", "ETHIOPIA", "KENYA", "MOROCCO", "MOZAMBIQUE")
      val e = intercept[WvletLangException] {
        engine.query("from nation select no_such_column")
      }
      e.statusCode shouldBe StatusCode.COLUMN_NOT_FOUND
    }
  }

  test("should resolve relative paths against the base folder") {
    Using.resource(WvletEmbed.open(tpchData)) { engine =>
      engine.registerTable("region", "region.parquet")
      engine.query("from region agg _.count as n").rows shouldBe IndexedSeq(IndexedSeq(5L))
      engine.query("from 'nation.parquet' agg _.count as n").rows shouldBe
        IndexedSeq(IndexedSeq(25L))
    }
  }

  test("should keep tables saved by earlier queries") {
    withEngine { engine =>
      engine.query("from [[1, 10], [2, 20]] as t(id, v) save to saved_t").rowCount shouldBe 0
      engine.query("from saved_t agg _.sum(v) as total").rows shouldBe IndexedSeq(IndexedSeq(30L))
    }
  }

  test("should return the last query's rows of several statements") {
    withEngine { engine =>
      val r = engine.query("""from [[1]] as t(a)
                             |test _.rows should be [[1]]
                             |
                             |from [[2]] as t(b)""".stripMargin)
      r.columns.map(_.name) shouldBe IndexedSeq("b")
      r.rows shouldBe IndexedSeq(IndexedSeq(2L))
    }
  }

  test("should report failed tests and syntax errors") {
    withEngine { engine =>
      intercept[WvletLangException] {
        engine.query("from [[1]] as t(a)\ntest _.rows should be [[2]]")
      }.statusCode shouldBe StatusCode.TEST_FAILED
      intercept[WvletLangException] {
        engine.query("from [[1]] as t(a) select a +")
      }.statusCode.isUserError shouldBe true
    }
  }

  test("should reject an unsupported file, a registered name, and calls after close") {
    val engine = WvletEmbed.open()
    intercept[WvletLangException] {
      engine.registerTable("t", "data.xlsx")
    }.statusCode shouldBe StatusCode.INVALID_ARGUMENT
    engine.registerTable("nation", s"${tpchData}/nation.parquet")
    intercept[WvletLangException] {
      engine.registerTable("nation", s"${tpchData}/region.parquet")
    }.statusCode shouldBe StatusCode.INVALID_ARGUMENT
    engine.close()
    intercept[WvletLangException] {
      engine.query("from [[1]] as t(a)")
    }.statusCode shouldBe StatusCode.INVALID_ARGUMENT
  }

  test("should be callable from Java") {
    WvletEmbedJavaUsage.countRegions(tpchData) shouldBe 5L
  }

  // Numbers compare at 10 significant digits: avg(double) differs from DuckDB's in the last digits
  private def normalize(v: Any): String =
    Option(v).map(_.toString) match
      case None =>
        "null"
      case Some(s) =>
        try
          BigDecimal(s).round(MathContext(10)).bigDecimal.stripTrailingZeros.toPlainString
        catch
          case _: NumberFormatException =>
            s

  private def duckdbRows(sql: String): Seq[Seq[String]] =
    Using.resource(DriverManager.getConnection("jdbc:duckdb:")) { conn =>
      Using.resource(conn.createStatement()) { stmt =>
        tpchTables.foreach { t =>
          stmt.execute(s"create view ${t} as select * from '${tpchData}/${t}.parquet'")
        }
        Using.resource(stmt.executeQuery(sql)) { rs =>
          val n = rs.getMetaData.getColumnCount
          Iterator
            .continually(rs.next())
            .takeWhile(identity)
            .map(_ => (1 to n).map(i => normalize(rs.getString(i))))
            .toList
        }
      }
    }

  private def duckdbSQL(query: String): String =
    val compiler = Compiler(
      CompilerOptions(
        sourceFolders = Nil,
        workEnv = WorkEnv(".", logLevel = LogLevel.WARN),
        dbType = DBType.DuckDB
      )
    )
    val unit   = CompilationUnit.fromWvletString(query)
    val result = compiler.compileSingleUnit(unit)
    if result.hasFailures then
      throw result.failureException
    GenSQL.generateSQL(unit)(using result.context)

  test("should return the same rows as DuckDB for TPC-H queries") {
    withEngine { engine =>
      tpchTables.foreach(t => engine.registerTable(t, s"${tpchData}/${t}.parquet"))
      Seq("q1", "q3", "q5", "q6", "q10", "q12", "q14", "q18").foreach { q =>
        val query    = SourceIO.readAsString(s"spec/tpch/${q}.wv")
        val actual   = engine.query(query).rows.map(_.map(normalize))
        val expected = duckdbRows(duckdbSQL(query))
        actual.nonEmpty shouldBe true
        actual.sortBy(_.mkString("\t")) shouldBe expected.sortBy(_.mkString("\t"))
      }
    }
  }

end WvletEmbedTest
