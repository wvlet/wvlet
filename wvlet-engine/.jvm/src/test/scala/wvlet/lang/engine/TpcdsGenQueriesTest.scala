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

import wvlet.lang.compiler.codegen.GenSQL
import wvlet.lang.engine.catalog.TpcdsGen
import wvlet.uni.test.UniTest

/**
  * Every spec/tpcds query runs on the engine over the tables [[TpcdsGen]] generates, and returns
  * rows wherever it returns rows over dsdgen's tables of the same scale factor. Values differ from
  * dsdgen's, so answers aren't compared; returning rows shows the generated data has the values and
  * correlations the query looks for
  */
class TpcdsGenQueriesTest extends UniTest:
  private val sf = 0.1

  /**
    * Queries that return rows on dsdgen's tables but not on the generated ones (one row each on
    * dsdgen's), with the reason. This list must only shrink
    */
  private val knownMisses: Map[String, String] = Map(
    "q04.wv" -> "customers whose catalog, store, and web sales all grow from 2001 to 2002",
    "q23.wv" -> "best customers' catalog and web sales of frequently sold items",
    "q29.wv" -> "store returns bought again from the catalog within a few months"
  )

  private val (dataDir, tables) = EngineTPCDSTest.prepareData(sf)

  // Compiles the queries; its engine reads dsdgen's tables
  private val dsdgen = EngineConformance(
    "spec/tpcds",
    Nil,
    e => tables.foreach(t => e.catalog.registerParquet(t, s"${dataDir}/${t}.parquet"))
  )

  private val generated = WvletEngine(baseDir = "spec/tpcds")
  TpcdsGen.register(generated.catalog, sf)

  override def afterAll: Unit = dsdgen.close()

  dsdgen
    .units
    .foreach { unit =>
      val file = unit.sourceFile.fileName
      test(s"should return rows for ${file} where dsdgen's tables do") {
        dsdgen.queries(unit) { (name, q) =>
          val plan     = GenSQL.expand(q, summon[wvlet.lang.compiler.Context])
          val expected = dsdgen.engine.execute(plan).numRows
          val actual   = generated.execute(plan).numRows
          debug(s"${name}: ${actual} rows (dsdgen: ${expected})")
          knownMisses.get(file) match
            case Some(reason) =>
              if actual > 0 then
                fail(s"${name} now returns rows: remove it from knownMisses (${reason})")
            case None =>
              if expected > 0 && actual == 0 then
                fail(s"${name} returns no rows, but ${expected} on dsdgen's tables")
        }
      }
    }

end TpcdsGenQueriesTest
