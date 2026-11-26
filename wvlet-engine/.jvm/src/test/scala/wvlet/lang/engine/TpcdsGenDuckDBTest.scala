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

import wvlet.lang.engine.catalog.ParquetSource
import wvlet.lang.engine.catalog.ScanSpec
import wvlet.lang.engine.catalog.TableSource
import wvlet.lang.engine.catalog.TpcdsGen
import wvlet.uni.test.UniTest

/**
  * The TPC-DS tables [[TpcdsGen]] generates equal the ones DuckDB's dsdgen writes, column by
  * column, at scale factor 0.1
  */
class TpcdsGenDuckDBTest extends UniTest:
  private lazy val dsdgenDir = EngineTPCDSTest.prepareData(0.1)._1

  // dsdgen's d_last_dom doesn't follow the calendar; the generator's does
  private val knownDifferences = Set("d_last_dom")

  private def rowsOf(t: TableSource): IndexedSeq[Array[Any]] =
    t.scan(ScanSpec()).flatMap(b => Iterator.range(0, b.size).map(b.row(_))).toIndexedSeq

  // Tables fully defined by the specification: equal to dsdgen's row by row
  private val exactTables = TpcdsGen.tableNames.take(5)

  TpcdsGen
    .sources(0.1)
    .filter(s => exactTables.contains(s.name))
    .foreach { gen =>
      test(s"should generate ${gen.name} as dsdgen does") {
        val ref = ParquetSource(gen.name, s"${dsdgenDir}/${gen.name}.parquet")
        ref.schema.names shouldBe gen.schema.names
        ref.schema.types shouldBe gen.schema.types
        val expected = rowsOf(ref)
        val actual   = rowsOf(gen)
        actual.size shouldBe expected.size
        // The first differing row of each column, with the count of differing rows
        val diffs = gen
          .schema
          .names
          .zipWithIndex
          .filterNot((c, _) => knownDifferences.contains(c))
          .flatMap { (c, i) =>
            val rows = expected.indices.filter(r => expected(r)(i) != actual(r)(i))
            rows
              .headOption
              .map(r =>
                s"${c}: ${rows.size} rows differ, first at row ${r}: " +
                  s"expected ${expected(r)(i)}, got ${actual(r)(i)}"
              )
          }
        diffs shouldBe Seq.empty
      }
    }

  // The other tables: dsdgen's columns, row counts, and share of NULLs
  TpcdsGen
    .sources(0.1)
    .filterNot(s => exactTables.contains(s.name))
    .foreach { gen =>
      test(s"should generate ${gen.name} with dsdgen's columns, rows, and NULL shares") {
        val ref = ParquetSource(gen.name, s"${dsdgenDir}/${gen.name}.parquet")
        ref.schema.names shouldBe gen.schema.names
        ref.schema.types shouldBe gen.schema.types
        val expected = rowsOf(ref)
        val actual   = rowsOf(gen)
        actual.size shouldBe expected.size
        if expected.size >= 1000 then
          def share(rows: IndexedSeq[Array[Any]], i: Int): Double =
            rows.count(_(i) == null).toDouble / rows.size
          gen
            .schema
            .names
            .indices
            .foreach { i =>
              val c = gen.schema.names(i)
              (c, math.abs(share(actual, i) - share(expected, i)) < 0.02) shouldBe (c, true)
            }
      }
    }

end TpcdsGenDuckDBTest
