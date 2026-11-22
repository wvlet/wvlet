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

import wvlet.uni.test.UniTest

/** TPC-H queries (spec/tpch) on the engine must return the same rows as DuckDB */
class EngineTPCHTest extends UniTest:
  private val tables = Seq(
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier"
  )

  private val conformance = EngineConformance(
    "spec/tpch",
    tables.map(t => s"create view ${t} as select * from 'spec/tpch/data/${t}.parquet'"),
    engine => tables.foreach(t => engine.catalog.registerParquet(t, s"spec/tpch/data/${t}.parquet"))
  )

  override def afterAll: Unit = conformance.close()

  for unit <- conformance.units do
    test(unit.sourceFile.fileName) {
      conformance
        .run(unit)
        .foreach { r =>
          debug(s"${r.name}: ${r.message}")
          if !r.matched then
            fail(s"${r.name}: ${r.message}")
        }
    }
