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
import wvlet.uni.test.UniTest

/** ROLLUP / CUBE / grouping() queries on the engine must return the same rows as DuckDB */
class EngineGroupingSetsTest extends UniTest:
  private val conformance = EngineConformance(
    "wvlet-engine/.jvm/src/test/resources/engine-spec/grouping-sets",
    Nil,
    _ => ()
  )

  override def afterAll: Unit = conformance.close()

  for unit <- conformance.units do
    test(unit.sourceFile.fileName) {
      val results = conformance.run(unit)
      results.size shouldBe 4
      results.foreach { r =>
        debug(s"${r.name}: ${r.message}")
        if !r.matched || r.message.startsWith("skipped") then
          fail(s"${r.name}: ${r.message}")
      }
    }

  test("group each grouping set by its non-constant keys only") {
    val compiler = Compiler(CompilerOptions(sourceFolders = Nil, workEnv = WorkEnv(".")))
    val unit     = CompilationUnit.fromWvletString("""from 'spec/tpch/data/lineitem.parquet'
        |group by rollup(l_returnflag, l_linestatus)
        |select l_returnflag, l_linestatus, cnt = _.count""".stripMargin)
    val r         = compiler.compileSingleUnit(unit)
    given Context = r.context
    val q         = EngineTPCHBench.queryOf(unit).getOrElse(fail("No query"))
    val plan      = WvletEngine().explain(q)
    debug(plan)
    // The NULL keys outside a set and the grouping id are added after aggregating
    """Agg[^\n]*keys=\[[^\]]*lit\(""".r.findFirstIn(plan) shouldBe None
    plan shouldContain "lit(null:string)"
  }

end EngineGroupingSetsTest
