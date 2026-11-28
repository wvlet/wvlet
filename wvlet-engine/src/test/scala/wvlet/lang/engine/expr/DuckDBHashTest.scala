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

import wvlet.uni.test.UniTest

/** DuckDBHash against values DuckDB 2.0.1's hash() returns */
class DuckDBHashTest extends UniTest:
  private def unsigned(h: Long): String = java.lang.Long.toUnsignedString(h)

  test("should hash integers and strings as DuckDB does") {
    unsigned(DuckDBHash.hash(42)) shouldBe "7199933130570745587"
    unsigned(DuckDBHash.hash(1L)) shouldBe "4717996019076358352"
    unsigned(DuckDBHash.hash("ab")) shouldBe "12355297858342501949"
  }

  test("should combine the hashes of several values as DuckDB does") {
    unsigned(DuckDBHash.hashAll(Array[Any](42, 1))) shouldBe "15939545652191236702"
    unsigned(DuckDBHash.hashAll(Array[Any](42L, 1L))) shouldBe "15939545652191236702"
    unsigned(DuckDBHash.hashAll(Array[Any](7, "ab"))) shouldBe "8298745331865035290"
  }
