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
package wvlet.lang.engine.expr

import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

/**
  * map(keys, values) on constants; map_agg results against DuckDB are in EngineStdlibAggregateTest
  */
class MapFunctionsTest extends UniTest:

  private def map(keys: Any, values: Any): Any = ScalarFunctions
    .lookup("map", Seq(Const(keys, PType.AnyT), Const(values, PType.AnyT)))
    .get
    .eval(Batch.ofRows(1))
    .get(0)

  test("should build a map in key order") {
    val m = map(List("b", "a"), List(1L, null))
    m shouldBe Map("b" -> 1L, "a" -> null)
    m.asInstanceOf[Map[Any, Any]].keys.toList shouldBe List("b", "a")
    ScalarFunctions.lookup("map", Nil).get.eval(Batch.ofRows(1)).get(0) shouldBe Map.empty
  }

  test("should give NULL for NULL lists") {
    map(null, null) shouldBe null
  }

  test("should reject duplicate keys, NULL keys, and misaligned lists as DuckDB does") {
    intercept[Exception] {
      map(List("a", "a"), List(1L, 2L))
    }
    intercept[Exception] {
      map(List("a", null), List(1L, 2L))
    }
    intercept[Exception] {
      map(List("a"), List(1L, 2L))
    }
  }

end MapFunctionsTest
