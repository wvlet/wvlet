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
package wvlet.lang.engine.plan

import wvlet.lang.compiler.CompilationUnit
import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.DBType
import wvlet.lang.compiler.WorkEnv
import wvlet.lang.engine.WvletEngine
import wvlet.lang.engine.catalog.MemTable
import wvlet.lang.engine.catalog.ValueCounts
import wvlet.lang.engine.vector.*
import wvlet.lang.model.plan.ExecuteQuery
import wvlet.lang.model.plan.ExecuteTasks
import wvlet.lang.model.plan.ExecutionPlan
import wvlet.lang.model.plan.Relation
import wvlet.uni.test.UniTest

/**
  * String join keys with no distinct count take their value sample's distinct values when the
  * sample is saturated (`sampled_string_distinct`), as TPC-DS q85's customer_demographics self-join
  * on (cd_marital_status, cd_education_status) needs at SF100
  */
class SampledStringDistinctTest extends UniTest:

  private def strs(xs: String*): Vec = StrVec(xs.toArray, null)

  test("count the values of a saturated sample") {
    // 5 values over 250K rows, none seen once
    val vc = ValueCounts(Array("M", "S", "W", "D", "U"), Array.fill(5)(50000L), 0, 250000)
    vc.sampledDistinct shouldBe Some(5.0)
  }

  test("not count a sample with many values seen once") {
    // A key: every sampled value is seen once
    val key = ValueCounts(Array.tabulate(4000)(_.toString), Array.fill(4000)(1L), 0, 4000)
    key.sampledDistinct shouldBe None
    // 3 values seen often and 1,000 seen once in 100K rows: 1% of rows hold a rare value
    val rare = ValueCounts(
      Array.tabulate(1003)(_.toString),
      Array(33000L, 33000L, 33000L) ++ Array.fill(1000)(1L),
      0,
      100000
    )
    rare.sampledDistinct shouldBe None
  }

  test("not count a small sample") {
    ValueCounts(Array("a", "b"), Array(500L, 500L), 0, 1000).sampledDistinct shouldBe None
  }

  /** A table estimated at `rows` rows, with saturated samples of the given string columns */
  private class Big(
      name: String,
      schema: Schema,
      rows: Long,
      samples: Map[String, Int],
      keys: Seq[Seq[String]]
  ) extends MemTable(name, schema, Nil, keys):
    override def estimatedRows: Option[Long]                      = Some(rows)
    override def valueCounts(column: String): Option[ValueCounts] = samples
      .get(column)
      .map(n =>
        ValueCounts(Array.tabulate(n)(i => s"v${i}"), Array.fill(n)(250000L / n), 0, 250000)
      )

  // 72M returns, each with two demographics rows of a 1.92M-row dimension; the dimension's
  // (status, education) pair has 35 values
  private val returns = Big(
    "returns",
    Schema(IndexedSeq(Field("r_d1", PType.Int64), Field("r_d2", PType.Int64))),
    72000000,
    Map.empty,
    Nil
  )

  private def demographics(name: String) = Big(
    name,
    Schema(
      IndexedSeq(
        Field(s"${name}_sk", PType.Int64),
        Field(s"${name}_status", PType.Str),
        Field(s"${name}_edu", PType.Str)
      )
    ),
    1920000,
    Map(s"${name}_status" -> 5, s"${name}_edu" -> 7),
    Seq(Seq(s"${name}_sk"))
  )

  private val compiler = Compiler(
    CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv("."), dbType = DBType.Wvlet)
  )

  private def explain(opts: OptimizerOptions): String =
    val e = WvletEngine(optimizer = opts)
    Seq(returns, demographics("da"), demographics("db")).foreach(e.catalog.register)
    val unit = CompilationUnit.fromWvletString("""from returns, da, db
        |where r_d1 = da_sk and r_d2 = db_sk
        |  and da_status = db_status and da_edu = db_edu and da_status = 'v1'
        |select r_d1""".stripMargin)
    val r = compiler.compileSingleUnit(unit)
    r.reportAllErrors
    given wvlet.lang.compiler.Context            = r.context
    def find(p: ExecutionPlan): Option[Relation] =
      p match
        case ExecuteQuery(q: Relation) =>
          Some(q)
        case ExecuteTasks(ts) =>
          ts.iterator.flatMap(find).nextOption()
        case _ =>
          None
    e.explain(find(unit.executionPlan).get)

  /** The output columns of the first join the plan runs */
  private def firstJoin(plan: String): String = plan
    .linesIterator
    .find(_.contains("HashJoin"))
    .getOrElse(fail(s"no join in\n${plan}"))

  test("join the fact table before joining two dimension rows on non-key columns") {
    val on = firstJoin(explain(OptimizerOptions()))
    on shouldContain "r_d1"
    val off = firstJoin(
      explain(OptimizerOptions().withDisabled(Optimization.SampledStringDistinct))
    )
    // Unknown distinct counts take the smaller side as unique: 1.92M rows instead of ~1e11
    off.contains("r_d1") shouldBe false
  }

end SampledStringDistinctTest
