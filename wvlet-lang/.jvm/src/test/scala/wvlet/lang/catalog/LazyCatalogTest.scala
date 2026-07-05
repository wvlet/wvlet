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
package wvlet.lang.catalog

import wvlet.lang.compiler.DBType
import wvlet.uni.test.UniTest

import java.util.concurrent.atomic.AtomicInteger

class LazyCatalogTest extends UniTest:

  test("should not build the backing catalog until a metadata lookup") {
    val buildCount = AtomicInteger(0)
    val catalog    = LazyCatalog(
      "memory",
      DBType.DuckDB,
      () =>
        buildCount.incrementAndGet()
        InMemoryCatalog("memory", Nil)
    )

    // Name and dialect are available without materialization
    catalog.catalogName shouldBe "memory"
    catalog.dbType shouldBe DBType.DuckDB
    buildCount.get() shouldBe 0

    // First lookup materializes; further lookups reuse the same instance
    catalog.findTable("main", "t") shouldBe None
    catalog.listSchemaNames
    buildCount.get() shouldBe 1
  }

end LazyCatalogTest
