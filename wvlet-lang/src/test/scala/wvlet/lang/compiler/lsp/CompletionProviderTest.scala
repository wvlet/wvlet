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

class CompletionProviderTest extends UniTest:

  private def newCompiler: Compiler = Compiler(CompilerOptions(workEnv = WorkEnv(path = ".")))

  private def complete(content: String, offset: Int): List[CompletionItem] = CompletionProvider
    .complete(content, offset, newCompiler)

  test("should always offer keyword candidates"):
    val items  = complete("", 0)
    val labels = items.map(_.label).toSet
    labels shouldContain "from"
    labels shouldContain "select"
    labels shouldContain "where"
    // Keywords carry the Keyword kind
    items.find(_.label == "from").map(_.kind) shouldBe Some(CompletionItemKind.Keyword)

  test("should complete in-file model names"):
    val src =
      """model my_model = {
        |  from [[1, "alice", 10]] as person(id, name, age)
        |}
        |from my_model""".stripMargin
    val items     = complete(src, src.length)
    val modelItem = items.find(_.label == "my_model")
    modelItem.map(_.kind) shouldBe Some(CompletionItemKind.Class)
    modelItem.map(_.detail) shouldBe Some("model")

  test("should complete column names from an inline values relation"):
    val src    = """from [[1, "alice", 10], [2, "bob", 20]] as person(id, name, age)"""
    val items  = complete(src, src.length)
    val labels = items.map(_.label).toSet
    labels shouldContain "id"
    labels shouldContain "name"
    labels shouldContain "age"
    items.find(_.label == "id").map(_.kind) shouldBe Some(CompletionItemKind.Field)

  test("should complete columns of the input relation inside a select"):
    val src = "from [[1, \"alice\", 10]] as person(id, name, age)\nselect id"
    // Place the cursor inside the select projection
    val offset = src.length
    val labels = complete(src, offset).map(_.label).toSet
    labels shouldContain "id"
    labels shouldContain "name"
    labels shouldContain "age"

  test("should not throw on incomplete input with a trailing from"):
    val src = "from "
    // Must return at least keyword candidates without throwing
    val items = complete(src, src.length)
    items.map(_.label).toSet shouldContain "select"

  test("should not throw on an incomplete select projection"):
    val src   = "from [[1, \"alice\", 10]] as person(id, name, age)\nselect "
    val items = complete(src, src.length)
    // Keywords must still be present even if typing of the incomplete query fails
    items.map(_.label).toSet shouldContain "where"

  test("should suggest columns from the nearest query when the cursor is past the end of file"):
    // A large model at the top of the file plus a small query at the bottom. With a trailing
    // newline the cursor is outside every relation's span, so the fallback must pick the
    // nearest (bottom) query, not the widest relation inside the unrelated model
    val src =
      """model big_model = {
        |  from [[1, "x", 2.0, true, "y"]] as wide(model_c1, model_c2, model_c3, model_c4, model_c5)
        |}
        |from [[1, "alice"]] as person(id, name)
        |""".stripMargin
    val labels = complete(src, src.length).map(_.label).toSet
    labels shouldContain "id"
    labels shouldContain "name"
    labels shouldNotContain "model_c1"

  test("nodeAt should return None when the offset is outside every span"):
    // An empty source has no nodes covering a positive offset
    val emptyPlan = wvlet.lang.model.plan.LogicalPlan.empty
    CompletionProvider.nodeAt(emptyPlan, 5) shouldBe None

  test("should not retain stale document snapshots across requests"):
    // A long-lived compiler (editor session) serves many document snapshots; symbols of an
    // earlier snapshot must not leak into (or shadow) later requests
    val compiler = newCompiler
    val v1       =
      """model snapshot_model = {
        |  from [[1]] as t(v1_col)
        |}
        |""".stripMargin
    CompletionProvider.complete(v1, v1.length, compiler)

    // The definition existed only in the previous snapshot, so it must not resolve here
    val query  = "from snapshot_model\n"
    val labels = CompletionProvider.complete(query, query.length, compiler).map(_.label).toSet
    labels shouldNotContain "v1_col"

  test("should serve the updated schema after a document changes"):
    val compiler = newCompiler
    val v1       =
      """model edited_model = {
        |  from [[1]] as t(old_col)
        |}
        |from edited_model
        |""".stripMargin
    CompletionProvider.complete(v1, v1.length, compiler).map(_.label).toSet shouldContain "old_col"

    val v2 =
      """model edited_model = {
        |  from [[1, 2]] as t(old_col, new_col)
        |}
        |from edited_model
        |""".stripMargin
    val labels = CompletionProvider.complete(v2, v2.length, compiler).map(_.label).toSet
    labels shouldContain "new_col"

end CompletionProviderTest
