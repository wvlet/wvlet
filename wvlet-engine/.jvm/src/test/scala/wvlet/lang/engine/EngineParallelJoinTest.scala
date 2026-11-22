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
  * Right and full joins whose probe side is split into parallel parts must emit each unmatched
  * build row exactly once, also through filters and aggregations above the join
  */
class EngineParallelJoinTest extends UniTest:
  private val dir = Paths.get("target/engine-test/parallel-join").toAbsolutePath
  Files.createDirectories(dir)
  private val conn =
    Class.forName("org.duckdb.DuckDBDriver")
    DriverManager.getConnection("jdbc:duckdb:")

  // facts: 20 row groups; dims: keys 0, 3, 6, ... 1497, of which those >= 1000 match no fact
  EngineTPCHBench.execute(
    conn,
    s"copy (select range::bigint as k, range % 7 as v from range(1000)) to '${dir}/facts.parquet' (format parquet, row_group_size 50)"
  )

  EngineTPCHBench.execute(
    conn,
    s"copy (select range::bigint as dk, 'd' || range as name from range(0, 1500, 3)) to '${dir}/dims.parquet' (format parquet)"
  )

  EngineTPCHBench.execute(
    conn,
    s"copy (select range::bigint as sk, 's' || range as label from range(0, 150, 3)) to '${dir}/small.parquet' (format parquet)"
  )

  private val engine   = WvletEngine()
  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  override def afterAll: Unit = conn.close()

  private def queryOf(e: ExecutionPlan): Option[Relation] =
    e match
      case ExecuteQuery(q: Relation) =>
        Some(q)
      case ExecuteTasks(ts) =>
        ts.iterator.flatMap(queryOf).nextOption()
      case _ =>
        None

  /** Sorted rows of a query on the engine and on DuckDB (through the generated SQL) */
  private def explainOf(query: String): String =
    val unit           = CompilationUnit.fromWvletString(query)
    val r              = compiler.compileSingleUnit(unit)
    given ctx: Context = r.context
    engine.explain(queryOf(unit.executionPlan).get)

  private def both(query: String): (Seq[String], Seq[String]) =
    val unit           = CompilationUnit.fromWvletString(query)
    val r              = compiler.compileSingleUnit(unit)
    given ctx: Context = r.context
    val q              = queryOf(unit.executionPlan).getOrElse(fail(s"No query in ${query}"))
    val plan           = engine.explain(q)
    debug(plan)
    plan shouldContain "Morsel"
    val actual =
      engine.execute(q).rows.map(_.map(v => String.valueOf(v)).mkString(",")).toSeq.sorted
    val sql = wvlet.lang.compiler.codegen.GenSQL.generateSQLFromRelation(q, addHeader = false).sql
    val st  = conn.createStatement()
    val rs  = st.executeQuery(sql)
    val n   = rs.getMetaData.getColumnCount
    val expected = Seq.newBuilder[String]
    while rs.next() do
      expected += (1 to n).map(i => String.valueOf(rs.getObject(i))).mkString(",")
    st.close()
    (actual, expected.result().sorted)

  private def check(query: String): Unit =
    val (actual, expected) = both(query)
    actual.size shouldBe expected.size
    actual shouldBe expected

  private val facts = s"'${dir}/facts.parquet'"
  private val dims  = s"'${dir}/dims.parquet'"

  test("should emit unmatched build rows once in a right join") {
    check(s"from ${facts} right join ${dims} on k = dk select k, v, dk, name")
  }

  test("should emit unmatched rows of both sides in a full join") {
    check(s"from ${facts} full join ${dims} on k = dk and v < 3 select k, v, dk, name")
  }

  test("should pass unmatched build rows through filters and aggregations above the join") {
    check(
      s"from ${facts} right join ${dims} on k = dk where dk % 2 = 0 group by v select v, n = _.count, s = dk.sum"
    )
    check(s"from ${facts} right join ${dims} on k = dk select n = _.count, m = k.count")
  }

  test("should keep left joins right") {
    check(s"from ${facts} left join ${dims} on k = dk select k, v, name")
  }

  test("should build a left join on the smaller input") {
    val small = s"'${dir}/small.parquet'"
    // 50 rows on the left, 1000 on the right: build on the left as a right join
    val q = s"from ${small} left join ${facts} on sk = k and v > 2 select sk, label, k, v"
    explainOf(q) shouldContain "HashJoin[Right]"
    check(q)
    check(s"from ${small} left join ${facts} on sk = k group by sk select sk, n = k.count")
  }

end EngineParallelJoinTest
