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
package wvlet.lang.runner

import wvlet.airspec.AirSpec

class QueryResultFormatTest extends AirSpec:
  test("unicode width") {
    for ch <- 'a' to 'Z' do
      QueryResultFormat.wcWidth(ch) shouldBe 1
    QueryResultFormat.wcWidth('東') shouldBe 2
  }

  test("truncate long strings") {
    QueryResultFormat.trimToWidth("hello", 3) shouldBe "he…"
    QueryResultFormat.trimToWidth("hello", 10) shouldBe "hello"
  }

  test("fit to width unicode strings") {
    QueryResultFormat.wcWidth("東京都") shouldBe 6
    QueryResultFormat.trimToWidth("東京都", 5) shouldBe "東京…"
    QueryResultFormat.trimToWidth("東京都", 10) shouldBe "東京都"
    QueryResultFormat.trimToWidth("東京都", 6) shouldBe "東京都"
  }
