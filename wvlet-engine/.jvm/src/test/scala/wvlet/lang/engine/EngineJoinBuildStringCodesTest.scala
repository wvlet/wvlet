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
import wvlet.lang.engine.exec.HashJoinOp
import wvlet.lang.engine.exec.Operator
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.lang.model.plan.*
import wvlet.uni.test.UniTest

import java.nio.file.Files
import java.nio.file.Paths
import java.sql.DriverManager

/**
  * `join_build_string_codes`: hash-join builds encode their string columns, and aggregations by
  * them give the same results as grouping by the strings
  */
class EngineJoinBuildStringCodesTest extends UniTest:
  private val options = OptimizerOptions().withValidatePlans(true)
  private val on      = WvletEngine(optimizer = options)
  private val off     = on.withOptimizer(options.withDisabled(Optimization.JoinBuildStringCodes))

  /** A fact table of 20000 rows and two dimensions with repeated strings and NULLs */
  private lazy val dir: String =
    val d = Paths.get("target/engine-test/join_build_string_codes").toAbsolutePath
    Files.createDirectories(d)
    Class.forName("org.duckdb.DuckDBDriver")
    val conn = DriverManager.getConnection("jdbc:duckdb:")
    try
      val st = conn.createStatement()
      try
        st.execute(s"""copy (
             |  select range::bigint as item, (range % 7)::bigint as store,
             |    (range % 12)::bigint as moy, (range % 100)::decimal(7, 2) as price
             |  from range(20000)
             |) to '${d}/fact.parquet' (format parquet, row_group_size 4096)""".stripMargin)
        st.execute(s"""copy (
             |  select range::bigint as item,
             |    case when range % 31 = 0 then null else 'cat' || (range % 5)::varchar end as cat,
             |    'brand' || (range % 40)::varchar as brand
             |  from range(20000) where range % 3 != 2
             |) to '${d}/item.parquet' (format parquet)""".stripMargin)
        st.execute(s"""copy (
             |  select range::bigint as store, 'store' || (range % 3)::varchar as name
             |  from range(7)
             |) to '${d}/store.parquet' (format parquet)""".stripMargin)
      finally
        st.close()
    finally
      conn.close()
    d.toString

  private val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))

  private def run[A](query: String)(body: Relation => Context ?=> A): A =
    val q0 = query
      .replace("$fact", s"'${dir}/fact.parquet'")
      .replace("$item", s"'${dir}/item.parquet'")
      .replace("$store", s"'${dir}/store.parquet'")
    val unit           = CompilationUnit.fromWvletString(q0)
    val r              = compiler.compileSingleUnit(unit)
    given ctx: Context = r.context
    val q              = queryOf(unit.executionPlan).getOrElse(fail(s"No query in ${query}"))
    body(q)

  private def queryOf(e: ExecutionPlan): Option[Relation] =
    e match
      case ExecuteQuery(q: Relation) =>
        Some(q)
      case ExecuteTasks(ts) =>
        ts.iterator.flatMap(queryOf).nextOption()
      case _ =>
        None

  private def rows(e: WvletEngine, query: String): Seq[String] =
    run(query)(q => e.execute(q).rows.map(_.mkString(",")).toSeq.sorted)

  private def sameRows(query: String): Seq[String] =
    val r = rows(on, query)
    r shouldBe rows(off, query)
    r

  /** Whether each hash join of the physical plan encodes its build strings */
  private def joinCodes(e: WvletEngine, query: String): Seq[Boolean] =
    def collect(op: Operator): Seq[Boolean] =
      (
        op match
          case j: HashJoinOp =>
            Seq(j.stringCodes)
          case _ =>
            Nil
      ) ++ op.children.flatMap(collect) ++ op.sharedInputs.flatMap(collect)
    run(query)(q => collect(e.physicalPlan(GenSQL.expand(q, summon[Context]))))

  private val starJoin =
    """from $fact as f
      |join $item as i on f.item = i.item
      |join $store as s on f.store = s.store""".stripMargin

  test("encode join build strings only with join_build_string_codes") {
    val q = s"${starJoin}\ngroup by i.cat agg _.count"
    joinCodes(on, q) shouldContain true
    joinCodes(on, q).contains(false) shouldBe false
    joinCodes(off, q).contains(true) shouldBe false
  }

  test("group by strings from join builds and an integer key as by their values") {
    val r = sameRows(s"""${starJoin}
        |group by i.cat, i.brand, s.name, f.moy
        |agg _.count, f.price.sum""".stripMargin)
    r.size shouldBe 466
    sameRows(s"""${starJoin}
        |group by s.name, i.cat
        |agg _.count""".stripMargin).size shouldBe 18
  }

  test("group join build strings together with strings from elsewhere") {
    // A union mixes batches with and without codes in one aggregation
    sameRows(s"""${starJoin}
        |select i.cat as cat, s.name as name
        |concat {
        |  from $$item select cat, brand as name
        |}
        |group by cat, name
        |agg _.count""".stripMargin).size shouldBe 98
    // Distinct over join build strings
    sameRows(s"""${starJoin}
        |select distinct s.name, i.cat""".stripMargin).size shouldBe 18
  }

end EngineJoinBuildStringCodesTest
