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
package wvlet.lang.engine.catalog

import wvlet.lang.api.WvletLangException
import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.model.plan.ExecutionPlan
import wvlet.lang.model.plan.ExecuteQuery
import wvlet.lang.model.plan.ExecuteTasks
import wvlet.lang.engine.EngineResult
import wvlet.lang.engine.WvletEngine
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

/** TPC-H tables generated in memory, queried on the engine (JVM and Scala.js) */
class TpchGenTest extends UniTest:
  private val sf     = 0.01
  private val tables = TpchGen.tables(sf).map(t => t.name -> t).toMap
  private val engine = WvletEngine()
  tables.values.foreach(engine.catalog.register)

  private def rows(name: String): Seq[IndexedSeq[Any]] =
    val t = tables(name)
    t.scan(ScanSpec()).flatMap(b => Iterator.range(0, b.size).map(i => b.row(i).toIndexedSeq)).toSeq

  private val compiler = Compiler(
    CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv("."), dbType = DBType.Wvlet)
  )

  private def run(query: String): EngineResult =
    val unit = CompilationUnit.fromWvletString(query)
    val r    = compiler.compileSingleUnit(unit)
    r.reportAllErrors
    given ctx: wvlet.lang.compiler.Context       = r.context
    def find(e: ExecutionPlan): Option[Relation] =
      e match
        case ExecuteQuery(q: Relation) =>
          Some(q)
        case ExecuteTasks(ts) =>
          ts.iterator.flatMap(find).nextOption()
        case _ =>
          None
    engine.execute(find(unit.executionPlan).get)

  test("should scale row counts as the TPC-H specification does") {
    rows("region").size shouldBe 5
    rows("nation").size shouldBe 25
    rows("supplier").size shouldBe 100
    rows("customer").size shouldBe 1500
    rows("part").size shouldBe 2000
    rows("partsupp").size shouldBe 8000
    rows("orders").size shouldBe 15000
    val n = rows("lineitem").size
    (n >= 15000 && n <= 15000 * 7) shouldBe true
  }

  test("should generate the same tables for the same scale factor") {
    val again = TpchGen.tables(sf).find(_.name == "orders").get
    again.scan(ScanSpec()).next().row(10).toSeq shouldBe rows("orders")(10).toSeq
  }

  test("should use sparse order keys and 4 distinct suppliers per part") {
    rows("orders").take(10).map(_(0)) shouldBe Seq(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 33L, 34L)
    rows("partsupp").groupBy(_(0)).values.forall(ps => ps.map(_(1)).distinct.size == 4) shouldBe
      true
  }

  test("should join line items to orders, parts, and suppliers") {
    val r = run("""from lineitem
        |left join orders on l_orderkey = o_orderkey
        |left join partsupp on l_partkey = ps_partkey and l_suppkey = ps_suppkey
        |where o_orderkey is null or ps_partkey is null
        |select cnt = _.count""".stripMargin)
    r.rows.next()(0) shouldBe 0L
  }

  test("should run TPC-H q1 over the generated tables") {
    val r = run("""from lineitem
        |where l_shipdate <= '1998-09-02'::date
        |group by l_returnflag, l_linestatus
        |select l_returnflag, l_linestatus, sum_qty = l_quantity.sum, count_order = _.count
        |order by l_returnflag, l_linestatus""".stripMargin)
    r.rows.map(x => s"${x(0)}${x(1)}").toSeq shouldBe Seq("AF", "NF", "NO", "RF")
  }

  test("should reject a scale factor out of range") {
    intercept[WvletLangException] {
      TpchGen.tables(0)
    }
    intercept[WvletLangException] {
      TpchGen.tables(TpchGen.maxScaleFactor * 2)
    }
  }

end TpchGenTest
