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
package wvlet.lang.ui.playground

import wvlet.lang.compiler.lsp.CompletionItemKind
import wvlet.lang.compiler.lsp.StdLibEntry
import wvlet.lang.compiler.lsp.StdLibReference
import wvlet.uni.test.UniTest

/** The Reference tab's function index, example runs, and the editor's hover and completion */
class ReferenceTest extends UniTest:
  private def entry(name: String, arity: Int = 0): StdLibEntry =
    StdLibReference.entries.find(e => e.name == name && e.arity == arity).get

  test("should list stdlib functions by category") {
    val titles = StdLibReference.sections.map(_.category.title)
    titles.head shouldBe "Text"
    titles shouldContain "Aggregate"
    titles shouldContain "Window"
  }

  test("should merge a function defined on several types into one entry") {
    val abs = entry("abs")
    abs.receivers shouldContain "int"
    abs.receivers shouldContain "double"
    abs.signature shouldBe "T.abs: T"
    abs.typeParam.get shouldContain "decimal"
  }

  test("should keep overloads of different arity apart") {
    entry("substring", 1).signature shouldBe "string.substring(start: int): string"
    entry("substring", 2).description shouldContain "1-origin"
    entry("lag", 2).label shouldBe "lag(offset, default)"
  }

  test("should filter functions by name and description") {
    StdLibReference.filter("upper").flatMap(_.entries).map(_.name) shouldContain "upper"
    StdLibReference.filter("no-such-function-xyz") shouldBe Nil
  }

  test("should find the examples that call a function") {
    val used = Reference.examplesUsing(entry("lower"))
    used.map(_.path) shouldContain "09_functions/01_string.wv"
  }

  test("should match top-level functions only as calls") {
    val ex = Seq(
      Examples.parse("x/01_a.wv", "-- A\nfrom t\nselect row_number() over (order by a)"),
      Examples.parse("x/02_b.wv", "-- B\n-- row_number( in a comment\nfrom t\nselect a.row_number")
    )
    Reference.examplesUsing(entry("row_number"), ex).map(_.title) shouldBe Seq("A")
  }

  private val engine = PlaygroundEngine(0.001)

  test("should run a function's examples on the engine") {
    engine.runExample("'Wvlet'.upper") shouldBe Right("'WVLET'")
    engine.runExample("from [[1], [2], [3]] as t(x) agg x.sum") shouldBe Right("6")
    engine.runExample("no_such_column.upper").isLeft shouldBe true
  }

  test("should open an example as a query") {
    Reference.queryOf("'a'.upper") shouldBe "select 'a'.upper\n"
    Reference.queryOf("from t select x") shouldBe "from t select x\n"
  }

  test("should hover a column with its type") {
    val text = "from nation\nselect n_name"
    val h    = engine.hover(text, text.indexOf("n_name") + 2)
    h.isDefined shouldBe true
    h.get.content shouldContain "n_name"
  }

  test("should hover a stdlib function with its docs and example") {
    val text = "from nation\nselect n_name.upper"
    val h    = engine.hover(text, text.indexOf("upper") + 1)
    h.isDefined shouldBe true
    h.get.content shouldContain "Example: `'Wvlet'.upper`"
  }

  test("should complete the columns of a table") {
    val text  = "from nation\nselect n_"
    val items = engine.complete(text, text.length)
    items.exists(i => i.label == "n_name" && i.kind == CompletionItemKind.Field) shouldBe true
  }

end ReferenceTest
