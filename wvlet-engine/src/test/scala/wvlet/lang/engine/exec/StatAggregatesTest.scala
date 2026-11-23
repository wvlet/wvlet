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

import wvlet.lang.api.WvletLangException
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

/**
  * Statistical and bitwise aggregates on small inputs; results against DuckDB are checked in
  * EngineStdlibAggregateTest
  */
class StatAggregatesTest extends UniTest:

  /** Aggregate one group of rows; each argument is a column of values */
  private def agg(name: String, cols: (PType, Seq[Any])*): Any =
    val args = cols.map((t, vs) => Vec.fromValues(t, vs)).toArray
    val out  = Aggregates.resultType(name, cols.map(_._1))
    val acc  = Aggregates.accumulator(name, cols.map(_._1), out)
    val n    = args.head.size
    acc.ensureGroups(1)
    acc.update(new Array[Int](n), n, args)
    acc.result(1).get(0)

  test("should compute a perfect linear fit") {
    val y = (PType.Float64, Seq(3.0, 5.0, 7.0))
    val x = (PType.Float64, Seq(1.0, 2.0, 3.0))
    agg("regr_slope", y, x) shouldBe 2.0
    agg("regr_intercept", y, x) shouldBe 1.0
    agg("regr_r2", y, x) shouldBe 1.0
    agg("regr_count", y, x) shouldBe 3L
    agg("corr", y, x) shouldBe 1.0
  }

  test("should return NULL for empty groups, and 0 for regr_count") {
    val y = (PType.Float64, Seq(null, 1.0))
    val x = (PType.Float64, Seq(1.0, null))
    agg("corr", y, x) shouldBe null
    agg("covar_pop", y, x) shouldBe null
    agg("regr_count", y, x) shouldBe 0L
  }

  test("should keep the integer type of bitwise aggregates") {
    agg("bit_or", (PType.Int32, Seq(12, 10, null, 6))) shouldBe 14
    agg("bit_xor", (PType.Int64, Seq(12L, -3L))) shouldBe -15L
    agg("bit_and", (PType.Int32, Seq(null))) shouldBe null
  }

  test("should reject bitwise aggregates of non-integers") {
    intercept[WvletLangException] {
      Aggregates.resultType("bit_and", Seq(PType.Float64))
    }.getMessage shouldContain "pass an integer"
  }

end StatAggregatesTest
