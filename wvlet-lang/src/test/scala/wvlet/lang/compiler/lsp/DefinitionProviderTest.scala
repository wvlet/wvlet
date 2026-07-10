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
package wvlet.lang.compiler.lsp

import wvlet.lang.compiler.Compiler
import wvlet.lang.compiler.CompilerOptions
import wvlet.lang.compiler.WorkEnv
import wvlet.uni.test.UniTest

class DefinitionProviderTest extends UniTest:

  private def newCompiler: Compiler = Compiler(CompilerOptions(workEnv = WorkEnv(path = ".")))

  private def definition(content: String, offset: Int): Option[DefinitionResult] =
    DefinitionProvider.definition(content, offset, newCompiler)

  test("should jump from a model reference to its model definition"):
    val src =
      """model my_model = {
        |  from [[1, "alice", 10]] as person(id, name, age)
        |}
        |from my_model""".stripMargin
    // Cursor on the `my_model` reference in the last line
    val offset = src.lastIndexOf("my_model") + 1
    val result = definition(src, offset)
    result.isDefined shouldBe true
    // The `model my_model` definition starts on line 1
    result.get.startLine shouldBe 1
    result.get.startColumn shouldBe 1

  test("should jump from a type reference to its type definition"):
    val src =
      """type point = {
        |  x: long
        |  y: long
        |}
        |type line = {
        |  start: point
        |  stop: point
        |}""".stripMargin
    // Cursor on the first `point` reference in the `line` definition
    val offset = src.indexOf("start: point") + "start: ".length + 1
    val result = definition(src, offset)
    result.isDefined shouldBe true
    // The `type point` definition starts on line 1
    result.get.startLine shouldBe 1
    result.get.startColumn shouldBe 1

  test("should resolve a model reference by name when later typing fails"):
    // The second query references an unknown column, so full typing fails; the model reference must
    // still resolve to its definition via the name fallback.
    val src =
      """model my_model = {
        |  from [[1, "alice", 10]] as person(id, name, age)
        |}
        |from my_model
        |from [[1]] as t(x)
        |select does_not_exist""".stripMargin
    val offset = src.indexOf("from my_model") + "from ".length + 1
    val result = definition(src, offset)
    result.isDefined shouldBe true
    result.get.startLine shouldBe 1

  test("should return None when the cursor is on the definition itself"):
    val src =
      """model my_model = {
        |  from [[1, "alice", 10]] as person(id, name, age)
        |}
        |from my_model""".stripMargin
    // Cursor on the `my_model` name within its own definition on line 1
    val offset = src.indexOf("my_model") + 1
    definition(src, offset) shouldBe None

  test("should return None when the cursor is on a keyword"):
    val src =
      """model my_model = {
        |  from [[1, "alice", 10]] as person(id, name, age)
        |}
        |from my_model""".stripMargin
    // Cursor on the `from` keyword in the last line
    val offset = src.lastIndexOf("from")
    definition(src, offset) shouldBe None

  test("should return None when hovering trailing whitespace"):
    val src = "from [[1, \"alice\"]] as person(id, name)\n\n"
    definition(src, src.length) shouldBe None

  test("should return None for an unknown reference"):
    val src    = "from unknown_model"
    val offset = src.lastIndexOf("unknown_model") + 1
    definition(src, offset) shouldBe None

  test("should not throw on incomplete input with a trailing from"):
    // Must not throw; typing of an incomplete query may fail, yielding None
    definition("from ", 5) shouldBe None

  test("should return None for an empty source"):
    definition("", 0) shouldBe None

end DefinitionProviderTest
