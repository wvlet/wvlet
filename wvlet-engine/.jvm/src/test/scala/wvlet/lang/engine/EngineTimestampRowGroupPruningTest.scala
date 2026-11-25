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
import wvlet.lang.model.plan.*
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Paths
import java.sql.DriverManager

/**
  * Row-group pruning on timestamp and date columns keeps every row group holding a matching row.
  * The engine floors TIMESTAMP(NANOS) values to micros, while Parquet statistics hold the nanos, so
  * a row at T + 500ns matches `ts <= T` although its row group's min is above T. Results are
  * compared with DuckDB reading the same file (NANOS floored to micros, as the engine reads them).
  */
class EngineTimestampRowGroupPruningTest extends UniTest:
  private val engine = WvletEngine()

  /**
    * 10240 rows in row groups of 2048 (DuckDB's smallest). Row i holds 2020-01-01 plus i
    * microseconds and 500ns (tns), i microseconds (tus), i milliseconds (tms), and i days (d)
    */
  private lazy val file: String =
    val dir = Paths.get("target/engine-test").toAbsolutePath
    Files.createDirectories(dir)
    val path = dir.resolve("timestamp_row_group_pruning.parquet")
    Files.deleteIfExists(path)
    duckdb { st =>
      st.execute(s"""copy (
             |  select
             |    range::bigint as id,
             |    make_timestamp_ns(1577836800000000000 + range * 1000 + 500) as tns,
             |    timestamp '2020-01-01' + to_microseconds(range) as tus,
             |    (timestamp '2020-01-01' + to_milliseconds(range))::timestamp_ms as tms,
             |    date '2020-01-01' + range::integer as d
             |  from range(10240)
             |) to '${path}' (format parquet, row_group_size 2048)""".stripMargin)
    }
    path.toString

  private def duckdb[A](body: java.sql.Statement => A): A =
    Class.forName("org.duckdb.DuckDBDriver")
    val conn = DriverManager.getConnection("jdbc:duckdb:")
    try
      val st = conn.createStatement()
      try body(st)
      finally st.close()
    finally
      conn.close()

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def engineIds(cond: String): Seq[Long] =
    val unit           = CompilationUnit.fromWvletString(s"from '${file}' where ${cond} select id")
    val r              = compiler.compileSingleUnit(unit)
    given ctx: Context = r.context
    val q              = queryOf(unit.executionPlan).getOrElse(fail(s"No query in ${cond}"))
    engine.execute(q).rows.map(_.head.toString.toLong).toSeq.sorted

  private def queryOf(e: ExecutionPlan): Option[Relation] =
    e match
      case ExecuteQuery(q: Relation) =>
        Some(q)
      case ExecuteTasks(ts) =>
        ts.iterator.flatMap(queryOf).nextOption()
      case _ =>
        None

  private def duckdbIds(cond: String): Seq[Long] = duckdb { st =>
    // DuckDB's ::timestamp rounds nanos to the nearest micro, so floor them explicitly
    val rs = st.executeQuery(s"""select id from (
         |  select id, make_timestamp(epoch_ns(tns) // 1000) as tns, tus, tms, d from '${file}'
         |) where ${cond}""".stripMargin)
    val b = Seq.newBuilder[Long]
    while rs.next() do
      b += rs.getLong(1)
    b.result().sorted
  }

  /**
    * Predicates on `col` around the row group boundary at row 2048 (`at`), up to row 6144 (`later`)
    */
  private def predicates(col: String, at: String, later: String): Seq[String] = Seq(
    s"${col} <= ${at}",
    s"${col} < ${at}",
    s"${col} = ${at}",
    s"${col} >= ${at}",
    s"${col} > ${at}",
    s"${at} >= ${col}",
    s"${col} between ${at} and ${later}",
    s"${col} in (${at}, ${later})"
  )

  private def check(col: String, wvAt: String, wvLater: String, sqlAt: String, sqlLater: String) =
    predicates(col, wvAt, wvLater)
      .zip(predicates(col, sqlAt, sqlLater))
      .foreach { (wv, sql) =>
        val expected = duckdbIds(sql)
        debug(s"${wv}: ${expected.size} rows")
        expected.nonEmpty shouldBe true
        engineIds(wv) shouldBe expected
      }

  test("keep NANOS row groups whose rows floor to a matching micros value") {
    check(
      "tns",
      "'2020-01-01 00:00:00.002048'::timestamp",
      "'2020-01-01 00:00:00.006144'::timestamp",
      "timestamp '2020-01-01 00:00:00.002048'",
      "timestamp '2020-01-01 00:00:00.006144'"
    )
  }

  test("prune MICROS row groups without losing matches") {
    check(
      "tus",
      "'2020-01-01 00:00:00.002048'::timestamp",
      "'2020-01-01 00:00:00.006144'::timestamp",
      "timestamp '2020-01-01 00:00:00.002048'",
      "timestamp '2020-01-01 00:00:00.006144'"
    )
  }

  test("prune MILLIS row groups without losing matches") {
    check(
      "tms",
      "'2020-01-01 00:00:02.048'::timestamp",
      "'2020-01-01 00:00:06.144'::timestamp",
      "timestamp '2020-01-01 00:00:02.048'",
      "timestamp '2020-01-01 00:00:06.144'"
    )
  }

  test("prune DATE row groups without losing matches") {
    check("d", "'2025-08-10'::date", "'2036-10-27'::date", "date '2025-08-10'", "date '2036-10-27'")
  }

end EngineTimestampRowGroupPruningTest
