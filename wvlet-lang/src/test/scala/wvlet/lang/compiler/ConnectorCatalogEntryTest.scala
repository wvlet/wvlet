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
package wvlet.lang.compiler

import wvlet.lang.catalog.InMemoryCatalog
import wvlet.uni.test.UniTest

class ConnectorCatalogEntryTest extends UniTest:

  private def newEntry(providerCalls: scala.collection.mutable.ListBuffer[String]) =
    ConnectorCatalogEntry(
      catalog = InMemoryCatalog("primary", Nil),
      defaultSchema = "main",
      catalogProvider = Some { name =>
        providerCalls += name
        InMemoryCatalog(name, Nil)
      }
    )

  test("should resolve the primary catalog without invoking the provider") {
    val calls = scala.collection.mutable.ListBuffer.empty[String]
    val entry = newEntry(calls)
    entry.catalogFor("primary").map(_.catalogName) shouldBe Some("primary")
    calls.toList shouldBe Nil
  }

  test("should build another catalog through the provider only once per name") {
    val calls = scala.collection.mutable.ListBuffer.empty[String]
    val entry = newEntry(calls)
    entry.catalogFor("extra").map(_.catalogName) shouldBe Some("extra")
    entry.catalogFor("extra").map(_.catalogName) shouldBe Some("extra")
    calls.toList shouldBe List("extra")
  }

  test("should not resolve non-primary catalogs without a provider") {
    val entry = ConnectorCatalogEntry(InMemoryCatalog("primary", Nil), "main")
    entry.catalogFor("extra") shouldBe None
    entry.catalogFor("primary").map(_.catalogName) shouldBe Some("primary")
  }

end ConnectorCatalogEntryTest
