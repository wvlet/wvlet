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
import wvlet.lang.engine.catalog.MemTable
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

/**
  * Readers of a shared CTE joined on a string key join on the long key that determines it
  * (`cte_determining_join_keys`)
  */
class EngineCTEJoinKeysTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)

  // customer 3 has a NULL id; the sale of customer NULL finds no customer
  private def tables: Seq[MemTable] =
    val customer = Batch(
      Array(
        Vec.fromValues(PType.Int64, Seq(1L, 2L, 3L, 4L)),
        Vec.fromValues(PType.Str, Seq("A", "B", null, "D")),
        Vec.fromValues(PType.Str, Seq("a", "b", "n", "d"))
      ),
      4
    )
    val sales = Batch(
      Array(
        Vec.fromValues(PType.Int64, Seq(1L, 1L, 2L, 2L, 3L, 3L, null, 4L)),
        Vec.fromValues(PType.Int64, Seq(2001L, 2002L, 2001L, 2002L, 2001L, 2002L, 2001L, 2001L)),
        Vec.fromValues(PType.Int64, Seq(10L, 20L, 5L, 1L, 7L, 9L, 3L, 8L))
      ),
      8
    )
    Seq(
      MemTable(
        "customer",
        Schema(
          IndexedSeq(
            Field("c_customer_sk", PType.Int64),
            Field("c_customer_id", PType.Str),
            Field("c_name", PType.Str)
          )
        ),
        Seq(customer),
        Seq(Seq("c_customer_sk"), Seq("c_customer_id"), Seq("c_name"))
      ),
      MemTable(
        "sales",
        Schema(
          IndexedSeq(
            Field("s_customer_sk", PType.Int64),
            Field("s_year", PType.Int64),
            Field("s_amount", PType.Int64)
          )
        ),
        Seq(sales),
        Nil
      )
    )

  end tables

  private def engine(o: OptimizerOptions): WvletEngine =
    val e = WvletEngine(optimizer = o)
    tables.foreach(e.catalog.register)
    e

  private val on  = engine(options)
  private val off = engine(options.withDisabled(Optimization.CTEDeterminingJoinKeys))

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  /** The plan and the rows of the query on the engine */
  private def run(e: WvletEngine, query: String): (String, Schema, Seq[Seq[Any]]) =
    val unit      = CompilationUnit.fromWvletString(query)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail(s"No query in ${query}"))
    val plan      = e.explain(q)
    debug(plan)
    val result = e.execute(q)
    (plan, result.schema, result.rows.map(_.toSeq).toSeq)

  private val yearTotals =
    """with t as {
      |  from customer, sales
      |  where c_customer_sk = s_customer_sk
      |  group by c_customer_id, c_name, s_year
      |  select c_customer_id as id, c_name as name, s_year as y, sum(s_amount) as total
      |}
      |""".stripMargin

  test("join the readers on the long key, without the row whose string key is NULL") {
    val query =
      yearTotals + """from t as a, t as b
        |where a.id = b.id and a.y = 2001 and b.y = 2002
        |select a.id, a.name, a.total as t1, b.total as t2
        |order by a.id
        |""".stripMargin
    val (planOn, _, rowsOn)   = run(on, query)
    val (planOff, _, rowsOff) = run(off, query)
    planOn shouldContain "__c_customer_sk"
    planOff.contains("__c_customer_sk") shouldBe false
    rowsOn shouldBe Seq(Seq("A", "a", 10L, 20L), Seq("B", "b", 5L, 1L))
    rowsOn shouldBe rowsOff
  }

  test("keep the joined readers' columns as they were") {
    val query =
      yearTotals + """from t as a, t as b
        |where a.id = b.id and a.y = 2001 and b.y = 2002
        |order by a.id
        |""".stripMargin
    val (planOn, schemaOn, rowsOn) = run(on, query)
    val (_, schemaOff, rowsOff)    = run(off, query)
    planOn shouldContain "__c_customer_sk"
    schemaOn.fields.map(_.name) shouldBe schemaOff.fields.map(_.name)
    schemaOn.size shouldBe 8
    rowsOn shouldBe rowsOff
  }

  test("keep a reader outside the joins reading the columns the query names") {
    val query =
      yearTotals + """from t as a, t as b
        |where a.id = b.id and a.y = 2001 and b.y = 2002
        |select a.id, b.total
        |concat { from t where y = 2001 select id, total }
        |order by id, total
        |""".stripMargin
    val (planOn, _, rowsOn) = run(on, query)
    val (_, _, rowsOff)     = run(off, query)
    planOn shouldContain "__c_customer_sk"
    rowsOn shouldBe rowsOff
  }

  test("keep the string key when the long key may be NULL") {
    // Without the join on c_customer_sk, nothing shows it is non-NULL
    val query =
      """with t as {
        |  from customer
        |  select c_customer_id as id, c_name as name
        |}
        |from t as a, t as b
        |where a.id = b.id
        |select a.id, b.name
        |order by a.id
        |""".stripMargin
    val (planOn, _, rowsOn) = run(on, query)
    planOn.contains("__c_customer_sk") shouldBe false
    rowsOn shouldBe Seq(Seq("A", "a"), Seq("B", "b"), Seq("D", "d"))
  }

  test("keep the string key when union branches copy different string keys") {
    // Customer 1 is "A" by id and "a" by name: equal long keys, different strings
    val query =
      """with t as {
        |  from customer, sales
        |  where c_customer_sk = s_customer_sk
        |  select c_customer_id as k, s_year as y
        |  concat {
        |    from customer, sales
        |    where c_customer_sk = s_customer_sk
        |    select c_name as k, s_year as y
        |  }
        |}
        |from t as a, t as b
        |where a.k = b.k and a.y = 2001 and b.y = 2002
        |select a.k
        |order by a.k
        |""".stripMargin
    val (planOn, _, rowsOn) = run(on, query)
    planOn.contains("__c_customer_sk") shouldBe false
    rowsOn shouldBe run(off, query)._3
  }

end EngineCTEJoinKeysTest
