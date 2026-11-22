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
import wvlet.lang.engine.catalog.*
import wvlet.lang.engine.vector.*
import wvlet.lang.model.plan.*
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Paths
import java.sql.DriverManager

/** Scans read only the referenced columns and skip Parquet row groups by statistics */
class EngineScanPushdownTest extends UniTest:
  private val engine = WvletEngine()

  /** An in-memory table that records the columns each scan asks for */
  private class RecordingTable(name: String, schema: Schema, batches: Seq[Batch])
      extends MemTable(name, schema, batches):
    val requests                                       = Seq.newBuilder[Seq[String]]
    override def scan(spec: ScanSpec): Iterator[Batch] =
      requests += spec.columns.getOrElse(schema.names)
      super.scan(spec)

  private val wide = RecordingTable(
    "wide",
    Schema(IndexedSeq(Field("a", PType.Int64), Field("b", PType.Int64), Field("c", PType.Str))),
    Seq(
      Batch(
        Array(
          LongVec(PType.Int64, Array(1L, 2L, 3L), null),
          LongVec(PType.Int64, Array(10L, 20L, 30L), null),
          StrVec(Array("x", "y", "z"), null)
        ),
        3
      )
    )
  )

  engine.catalog.register(wide)
  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def run[A](query: String)(body: (WvletEngine, Relation) => Context ?=> A): A =
    val unit           = CompilationUnit.fromWvletString(query)
    val r              = compiler.compileSingleUnit(unit)
    given ctx: Context = r.context
    val q              = queryOf(unit.executionPlan).getOrElse(fail(s"No query in ${query}"))
    body(engine, q)

  private def queryOf(e: ExecutionPlan): Option[Relation] =
    e match
      case ExecuteQuery(q: Relation) =>
        Some(q)
      case ExecuteTasks(ts) =>
        ts.iterator.flatMap(queryOf).nextOption()
      case _ =>
        None

  private def explain(query: String): String = run(query)((e, q) => e.explain(q))

  private def rows(query: String): Seq[IndexedSeq[Any]] =
    run(query)((e, q) => e.execute(q).rows.toSeq)

  /** A Parquet file of ids 0 until 1000 in row groups of 100 rows */
  private lazy val rowGroupFile: String =
    val dir = Paths.get("target/engine-test").toAbsolutePath
    Files.createDirectories(dir)
    val path = dir.resolve("row_groups.parquet")
    Files.deleteIfExists(path)
    Class.forName("org.duckdb.DuckDBDriver")
    val conn = DriverManager.getConnection("jdbc:duckdb:")
    try
      val st = conn.createStatement()
      try st.execute(
          s"copy (select range::bigint as id, 'v' || range as name from range(1000)) to '${path}' (format parquet, row_group_size 100)"
        )
      finally st.close()
    finally
      conn.close()
    path.toString

  test("should read only the columns a query references") {
    val plan = explain("""from 'spec/tpch/data/lineitem.parquet'
        |where l_shipdate <= '1998-09-02'::date
        |group by l_returnflag
        |select l_returnflag, cnt = _.count""".stripMargin)
    debug(plan)
    plan shouldContain "columns=l_returnflag,l_shipdate"
    plan.contains("l_comment") shouldBe false
  }

  test("should read only the columns window functions and the query above them reference") {
    val plan = explain(
      """from 'spec/tpch/data/lineitem.parquet'
        |select l_orderkey, r = rank() over (partition by l_orderkey order by l_extendedprice)"""
        .stripMargin
    )
    debug(plan)
    plan shouldContain "columns=l_orderkey,l_extendedprice"
    plan.contains("l_comment") shouldBe false
  }

  test("should read only the columns used above intersect and except") {
    val li   = "'spec/tpch/data/lineitem.parquet'"
    val plan = explain(s"""from {
        |  from ${li} select l_orderkey
        |  intersect
        |  { from ${li} where l_quantity > 10 select l_orderkey }
        |}
        |select _.count""".stripMargin)
    debug(plan)
    plan shouldContain "columns=l_orderkey)"
    plan shouldContain "columns=l_orderkey,l_quantity"
    plan.contains("l_comment") shouldBe false
  }

  test("should read only the columns used above union all") {
    val li   = "'spec/tpch/data/lineitem.parquet'"
    val plan = explain(s"""from {
        |  from ${li} select l_orderkey, l_quantity, l_comment
        |  concat
        |  { from ${li} select l_orderkey, l_extendedprice, l_shipmode }
        |}
        |select l_orderkey""".stripMargin)
    debug(plan)
    plan.contains("l_comment") shouldBe false
    plan.contains("l_shipmode") shouldBe false
    rows(s"""from {
        |  from ${li} where l_orderkey = 1 select l_orderkey, l_quantity
        |  concat
        |  { from ${li} where l_orderkey = 3 select l_orderkey, l_quantity }
        |}
        |select l_orderkey
        |order by l_orderkey""".stripMargin).map(_.head).distinct shouldBe Seq(1L, 3L)
  }

  test("should read CTEs referenced more than once in parallel parts, with pruned columns") {
    val plan = explain("""with t as { from 'spec/tpch/data/lineitem.parquet' where l_quantity > 10 }
        |from t as a join t as b on a.l_orderkey = b.l_orderkey
        |select a.l_orderkey, b.l_partkey""".stripMargin)
    debug(plan)
    // A reader of the shared result is a splittable scan: its pipeline runs as a Morsel
    """Morsel[^\n]*\n(\s+[^\n]*\n)*?\s+Scan\[CTE\[t\]\]""".r.findFirstIn(plan).isDefined shouldBe
      true
    plan shouldContain "(shared)"
    rows("""with t as { from 'spec/tpch/data/lineitem.parquet' where l_orderkey <= 3 }
        |from t as a join t as b on a.l_orderkey = b.l_orderkey and a.l_linenumber = b.l_linenumber
        |select a.l_orderkey""".stripMargin).size shouldBe
      rows("from 'spec/tpch/data/lineitem.parquet' where l_orderkey <= 3 select l_orderkey").size
  }

  test("should compute DISTINCT and the inputs of INTERSECT in parallel aggregations") {
    val li       = "'spec/tpch/data/lineitem.parquet'"
    val distinct = explain(s"from ${li} select distinct l_returnflag, l_linestatus")
    debug(distinct)
    distinct.contains("Distinct") shouldBe false
    distinct shouldContain "PartitionedAgg"
    val intersect = explain(s"""from ${li} select l_suppkey
        |intersect
        |{ from ${li} where l_quantity > 10 select l_suppkey }""".stripMargin)
    debug(intersect)
    // Each side is reduced to its distinct rows before the set operation
    """Intersect[^\n]*\n\s+Project[^\n]*\n\s+PartitionedAgg"""
      .r
      .findFirstIn(intersect)
      .isDefined shouldBe true
    rows(s"""from ${li} where l_orderkey <= 3 select l_returnflag
        |intersect
        |{ from ${li} where l_orderkey <= 3 select l_returnflag }""".stripMargin).size shouldBe
      rows(s"from ${li} where l_orderkey <= 3 select distinct l_returnflag").size
  }

  test("should cross join a scan with a one-row aggregate in parallel parts") {
    val li = "'spec/tpch/data/lineitem.parquet'"
    val q  =
      s"""from ${li}, { from ${li} select m = l_quantity.max } as mx
        |where l_quantity * 2 > m
        |select l_orderkey""".stripMargin
    val plan = explain(q)
    debug(plan)
    // The cross join runs inside the scan's parts, over the aggregate computed once
    """Morsel[^\n]*\n(\s+[^\n]*\n)*?\s+NestedLoopJoin\[Cross\]"""
      .r
      .findFirstIn(plan)
      .isDefined shouldBe true
    plan shouldContain "CrossInput"
    rows(q).size shouldBe rows(s"from ${li} where l_quantity * 2 > 50 select l_orderkey").size
  }

  test("should push comparisons into the Parquet scan") {
    val plan = explain(s"from '${rowGroupFile}' where id >= 950 select id")
    debug(plan)
    plan shouldContain "filter="
  }

  test("should return the same rows when row groups are skipped") {
    rows(s"from '${rowGroupFile}' where id >= 950 and id < 1000 select id").size shouldBe 50
    rows(s"from '${rowGroupFile}' where id = 123 select name") shouldBe Seq(IndexedSeq("v123"))
    rows(s"from '${rowGroupFile}' where id < 0 or id > 2000 select id") shouldBe Nil
    rows(s"from '${rowGroupFile}' where id = 5 or name = 'v777' select id").map(_.head) shouldBe
      Seq(5L, 777L)
  }

  test("should prune the columns of subqueries run while planning") {
    rows("from wide where b = { from wide select b.max } select a") shouldBe Seq(IndexedSeq(3L))
    val requests = wide.requests.result()
    debug(requests)
    requests.size shouldBe 2
    requests.foreach(cols => cols.contains("c") shouldBe false)
  }

  test("should read the input of scalar subqueries aggregating the same rows once") {
    val before = wide.requests.result().size
    rows("""from wide
        |where a = 1
        |select
        |  { from wide where a >= 2 select b.max } as mx,
        |  { from wide where a >= 2 select b.sum } as total,
        |  { from wide where a >= 2 select _.count } as cnt""".stripMargin) shouldBe
      Seq(IndexedSeq(30L, 50L, 2L))
    // One scan for the outer query and one for the three subqueries
    (wide.requests.result().size - before) shouldBe 2
  }

  test("should evaluate scalar subqueries over a CTE in the CTE's scope") {
    // The CTE is only known once its WITH is planned: the subqueries over it must still read it
    rows(
      """with high as { from wide where a >= 2 }
        |from wide
        |where a = 2
        |select { from high select b.sum } as total, { from high select _.count } as cnt"""
        .stripMargin
    ) shouldBe Seq(IndexedSeq(50L, 2L))
  }

end EngineScanPushdownTest
