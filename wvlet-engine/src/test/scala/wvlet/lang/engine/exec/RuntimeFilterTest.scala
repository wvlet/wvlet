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
package wvlet.lang.engine.exec

import wvlet.lang.engine.plan.ScanFilters
import wvlet.lang.engine.vector.*
import wvlet.uni.cloud.parquet.ParquetFilter
import wvlet.uni.cloud.parquet.ParquetFilter.col
import wvlet.uni.test.UniTest

class RuntimeFilterTest extends UniTest:
  private def filterOf(keys: Vec, rows: Array[Int]): Option[ParquetFilter] =
    val f = RuntimeFilter(0, "k", (lo, hi) => ScanFilters.range("k", PType.Int64, lo, hi))
    f.set(keys, rows, rows.length)
    f.filter

  test("should set the range of the build keys at the given rows") {
    val keys = LongVec(PType.Int64, Array(7L, -3L, 100L, 42L), null)
    filterOf(keys, Array(0, 1, 3)) shouldBe Some(col("k") >= -3L && col("k") <= 42L)
  }

  test("should read int keys as longs") {
    val keys = IntVec(PType.Int32, Array(5, 2, 9), null)
    filterOf(keys, Array(0, 1, 2)) shouldBe Some(col("k") >= 2L && col("k") <= 9L)
  }

  test("should set no filter for an empty build or keys of other types") {
    filterOf(LongVec(PType.Int64, Array(1L), null), Array.empty) shouldBe None
    filterOf(StrVec(Array("a"), null), Array(0)) shouldBe None
  }

end RuntimeFilterTest
