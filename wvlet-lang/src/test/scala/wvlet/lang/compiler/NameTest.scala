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

import wvlet.airspec.AirSpec

class NameTest extends AirSpec:

  //
  test("Create Name") {
    val x = Name.termName("x")
    x.toString shouldBe "x"
    x.isTermName shouldBe true
    x.isTypeName shouldBe false

    val x2 = Name.termName("x")
    x shouldBeTheSameInstanceAs x2
  }

  test("Create TypeName") {
    val x = Name.typeName("x")
    x.toString shouldBe "x"
    x.isTermName shouldBe false
    x.isTypeName shouldBe true

    val x2 = Name.typeName("x")
    x shouldBeTheSameInstanceAs x2
  }

  test("NoName") {
    Name.NoName.isEmpty shouldBe true
    Name.NoName.isTermName
  }

  test("Compare Name objects") {
    val x = Name.termName("x")
    val y = Name.termName("y")
    x shouldNotBe y
  }

  test("Use HashTable for Name objects") {
    val x  = Name.termName("x")
    val y  = Name.termName("y")
    val z  = Name.termName("z")
    val x2 = Name.termName("x")

    x shouldNotBe y
    x shouldNotBe z
    y shouldNotBe z

    x shouldBe x2
    x shouldBeTheSameInstanceAs x2

    val table = collection.mutable.Map.empty[Name, Int]
    table(x) = 1
    table(y) = 2
    table(z) = 3
    table(x) shouldBe 1
    table(y) shouldBe 2
    table(z) shouldBe 3

    table(x) = 5
    table(x) shouldBe 5
  }

end NameTest
