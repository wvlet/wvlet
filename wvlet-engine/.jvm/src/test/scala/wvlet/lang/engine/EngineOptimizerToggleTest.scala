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

import wvlet.lang.engine.exec.TaskRunner
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.uni.test.UniTest

/**
  * Turning off any one planner optimization may change plans but never results: spec/basic and
  * TPC-H, with each optimization disabled in turn, must still return the rows DuckDB returns (as
  * they do with every optimization on, see [[EngineBasicSpecTest]] and [[EngineTPCHTest]]). This
  * catches a query whose correct result secretly depends on an optimization
  */
class EngineOptimizerToggleTest extends UniTest:

  private def check(c: EngineConformance, skip: Set[String]): Unit =
    try
      val failures = c
        .units
        .filterNot(u => skip.contains(u.sourceFile.fileName))
        .flatMap(c.run)
        .filterNot(_.matched)
      if failures.nonEmpty then
        fail(failures.map(r => s"${r.name}: ${r.message}").mkString("\n"))
    finally
      c.close()

  // The plans with every optimization on, to compare with the plans without one
  private lazy val baselinePlans = EnginePlanGolden.suites.map(_.generate())

  /**
    * Queries too slow to run without an optimization: without OR factoring, q19's join has no
    * equality key and becomes a cross join of lineitem and part
    */
  private val tpchTooSlow: Map[Optimization, Set[String]] = Map(
    Optimization.FactorOr -> Set("q19.wv")
  )

  for o <- Optimization.values do
    val options = OptimizerOptions().withDisabled(o)

    test(s"spec/basic without ${o.name}") {
      check(EngineConformance("spec/basic", Nil, _ => (), options), EngineBasicSpecTest.knownGaps)
    }

    test(s"spec/tpch without ${o.name}") {
      val tables = EngineTPCHBench.tables
      check(
        EngineConformance(
          "spec/tpch",
          tables.map(t => s"create view ${t} as select * from 'spec/tpch/data/${t}.parquet'"),
          engine =>
            tables.foreach(t => engine.catalog.registerParquet(t, s"spec/tpch/data/${t}.parquet")),
          options
        ),
        tpchTooSlow.getOrElse(o, Set.empty)
      )
    }

    test(s"disabling ${o.name} changes some TPC-H or TPC-DS plan") {
      if TaskRunner.default.parallelism <= 1 && o == Optimization.Parallelize then
        ignore("Plans are not parallelized on one processor")
      if o == Optimization.PrefetchScalarSubqueries then
        // The subqueries become the same constants either way, one query per group or per subquery
        ignore("Changes how scalar subqueries are evaluated while planning, not the plan")
      val changed = EnginePlanGolden
        .suites
        .zip(baselinePlans)
        .exists((s, baseline) => s.generate(options) != baseline)
      if !changed then
        fail(s"Disabling ${o.name} changed no plan: is the flag wired to the optimization?")
    }

  end for

end EngineOptimizerToggleTest
