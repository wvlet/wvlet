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
package wvlet.lang.api

import wvlet.airspec.AirSpec

class SpanTest extends AirSpec:
  test("create a span") {
    val span = Span.within(1, 2)
    span.toString shouldBe "[1..2)"
    span.exists shouldBe true
    span.start shouldBe 1
    span.end shouldBe 2
    span.pointOffset shouldBe 0
    span.point shouldBe 1
  }

  test("create a point span") {
    val span = Span.at(1)
    span.toString shouldBe "[1..1)"
    span.exists shouldBe true
    span.start shouldBe 1
    span.end shouldBe 1
    span.pointOffset shouldBe 0
    span.point shouldBe 1
  }

  test("create a span with point") {
    val span = Span(1, 5, 2)
    span.toString shouldBe "[1..3..5)"
    span.exists shouldBe true
    span.start shouldBe 1
    span.end shouldBe 5
    span.pointOffset shouldBe 2
    span.point shouldBe 3
  }

  test("NoSpan") {
    val span = Span.NoSpan
    span.exists shouldBe false
    span.toString shouldBe "[NoSpan]"
  }

end SpanTest
