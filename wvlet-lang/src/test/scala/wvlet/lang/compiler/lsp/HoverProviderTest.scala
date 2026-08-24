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

class HoverProviderTest extends UniTest:

  private def newCompiler: Compiler = Compiler(CompilerOptions(workEnv = WorkEnv(path = ".")))

  private def hover(content: String, offset: Int): Option[HoverResult] = HoverProvider.hover(
    content,
    offset,
    newCompiler
  )

  test("should show the model schema when hovering a model reference"):
    val src =
      """model my_model = {
        |  from [[1, "alice", 10]] as person(id, name, age)
        |}
        |from my_model""".stripMargin
    // Place the cursor on the `my_model` reference in the last line
    val offset = src.lastIndexOf("my_model") + 1
    val result = hover(src, offset)
    result.isDefined shouldBe true
    val content = result.get.content
    content shouldContain "model my_model"
    content shouldContain "id: long"
    content shouldContain "name: string"
    content shouldContain "age: long"

  test("should show name and type when hovering a column reference"):
    val src = "from [[1, \"alice\", 10]] as person(id, name, age)\nselect name"
    // Cursor on the `name` column in the select
    val offset = src.lastIndexOf("name") + 1
    val result = hover(src, offset)
    result.isDefined shouldBe true
    result.get.content shouldContain "name: string"

  test("should return the source range of the hovered node"):
    val src    = "from [[1, \"alice\"]] as person(id, name)\nselect name"
    val offset = src.lastIndexOf("name") + 1
    val result = hover(src, offset)
    result.isDefined shouldBe true
    // The `select name` is on the 2nd line (1-based)
    result.get.startLine shouldBe 2
    result.get.startColumn shouldBe (src.lastIndexOf("name") - src.lastIndexOf("\n"))

  test("should return None when hovering whitespace with no node"):
    val src = "from [[1, \"alice\"]] as person(id, name)\n\n"
    // Cursor on the trailing empty line
    val result = hover(src, src.length)
    result shouldBe None

  test("should return None for an empty source"):
    hover("", 0) shouldBe None

  test("should not throw on incomplete input with a trailing from"):
    // Must not throw; typing of an incomplete query may fail, yielding None
    hover("from ", 5) shouldBe None

  test("should not throw on an incomplete select projection"):
    val src = "from [[1, \"alice\", 10]] as person(id, name, age)\nselect "
    // Must not throw regardless of whether a hover can be produced
    hover(src, src.length)

  test("should show the stdlib signature and doc when hovering a member function"):
    val src    = "from [[1, \"alice\"]] as person(id, name)\nselect name.substring(2)"
    val offset = src.lastIndexOf("substring") + 1
    val result = hover(src, offset)
    result.isDefined shouldBe true
    val content = result.get.content
    content shouldContain "member of string"
    content shouldContain "def substring(start: int): string"
    content shouldContain "def substring(start: int, length: int): string"
    content shouldContain "1-origin start position"

  test("should show every owning type when hovering a shared member name"):
    val src    = "from [[1, \"alice\"]] as person(id, name)\nselect name.length"
    val offset = src.lastIndexOf("length") + 1
    val result = hover(src, offset)
    result.isDefined shouldBe true
    val content = result.get.content
    content shouldContain "member of string"
    content shouldContain "member of array"

  test("should show the stdlib doc when hovering a top-level window function"):
    val src    = "from [[1, 10]] as t(k, v)\nselect row_number() over (order by k) as rn"
    val offset = src.indexOf("row_number") + 1
    val result = hover(src, offset)
    result.isDefined shouldBe true
    val content = result.get.content
    content shouldContain "def row_number: long"
    content shouldContain "Sequential row number"

  test("should hover the column, not a stdlib function, for an alias-qualified column"):
    // `t.year` accesses a column named like a stdlib function through a relation alias
    val src    = "from [[2024, 1]] as t(year, v)\nselect t.year"
    val offset = src.lastIndexOf("year") + 1
    val result = hover(src, offset)
    result.foreach(r => r.content shouldNotContain "member of")

  test("should show the def signature when hovering a user-defined row method"):
    val src =
      """table hv_users = {
        |  id: int
        |  deleted_at: string
        |  def is_active: boolean = deleted_at.is_null
        |}
        |from hv_users
        |where _.is_active""".stripMargin
    val offset = src.lastIndexOf("is_active") + 1
    val result = hover(src, offset)
    result.isDefined shouldBe true
    val content = result.get.content
    content shouldContain "member of hv_users"
    content shouldContain "def is_active: boolean"

  test("should show the owning trait when hovering a mixed-in method"):
    val src =
      """trait hv_audit = {
        |  def audit_tag: string = created_at.upper()
        |}
        |table hv_events extends hv_audit = {
        |  id: int
        |  created_at: string
        |}
        |from hv_events
        |select id, _.audit_tag as tag""".stripMargin
    val offset = src.lastIndexOf("audit_tag") + 1
    val result = hover(src, offset)
    result.isDefined shouldBe true
    val content = result.get.content
    content shouldContain "member of hv_audit"
    content shouldContain "def audit_tag: string"

  test("should show the trait method signature when hovering through a trait-typed column"):
    val src =
      """trait hv_masked extends string = {
        |  def masked: string = sql"'***'"
        |}
        |table hv_secrets = {
        |  id: int
        |  secret: hv_masked
        |}
        |from hv_secrets
        |select secret.masked""".stripMargin
    val offset = src.lastIndexOf("masked") + 1
    val result = hover(src, offset)
    result.isDefined shouldBe true
    val content = result.get.content
    content shouldContain "member of hv_masked"
    content shouldContain "def masked: string"

end HoverProviderTest
