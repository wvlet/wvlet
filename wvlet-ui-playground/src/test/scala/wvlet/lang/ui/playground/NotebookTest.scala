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

import wvlet.lang.api.LinePosition
import wvlet.lang.compiler.lsp.StdLibReference
import wvlet.uni.test.UniTest

/** The notebook's text form, its markdown, and running a cell with the cells above it */
class NotebookTest extends UniTest:
  import CellKind.*

  test("should read a plain example as a text cell and a query cell") {
    val cells = NotebookText.parse("-- Read a table\n-- A query starts with `from`.\nfrom nation\n")
    cells shouldBe
      Seq(
        CellSource(Markdown, "## Read a table\n\nA query starts with `from`."),
        CellSource(Code, "from nation")
      )
  }

  test("should keep a notebook of one query cell as plain text") {
    val l = NotebookText.render(Seq(CellSource(Code, "from nation")))
    l.text shouldBe "from nation"
    l.startLines shouldBe IndexedSeq(1)
  }

  test("should round-trip cells through the marker format with their start lines") {
    val cells = Seq(
      CellSource(Markdown, "## Title\n\nSome `code`"),
      CellSource(Code, "val x = 1"),
      CellSource(Code, "from nation\nlimit 1")
    )
    val l = NotebookText.render(cells)
    l.text shouldContain "-- %% [markdown]\n-- ## Title\n--\n-- Some `code`"
    l.startLines shouldBe IndexedSeq(2, 6, 8)
    l.text.split("\n")(7) shouldBe "from nation"
    NotebookText.parse(l.text) shouldBe cells
  }

  test("should keep a commented query cell a query cell through its text") {
    val cells = Seq(CellSource(Code, "-- my note\nfrom nation"))
    NotebookText.parse(NotebookText.render(cells).text) shouldBe cells
  }

  test("should drop only the notebook's comments above a statement's SQL") {
    PlaygroundEngine.dropNotebookComments(
      Seq("-- %% [markdown]", "-- ## Title", "-- %%", "-- my note", "select 1")
    ) shouldBe Seq("-- my note", "select 1")
  }

  test("should open a function as its docs and one cell per example") {
    val lower = StdLibReference.entries.find(_.name == "lower").get
    val cells = NotebookText.ofFunction(lower)
    cells.head.kind shouldBe Markdown
    cells.head.text shouldContain "## `lower`"
    cells.head.text shouldContain "(#example="
    cells.tail.map(_.text) shouldBe Seq("select 'Wvlet'.lower")
  }

  test("should put top-level clauses of a one-line example on their own lines") {
    NotebookText.formatQuery("from [[1], [2]] as t(x) agg x.sum") shouldBe
      "from [[1], [2]] as t(x)\nagg x.sum"
    // Clauses inside parentheses and strings stay
    NotebookText.formatQuery("select x.lag over (order by x) as ' select '") shouldBe
      "select x.lag over (order by x) as ' select '"
  }

  test("should parse markdown blocks and inline spans") {
    wvlet
      .lang
      .ui
      .playground
      .Markdown
      .parse("## Title `x`\n\nOne\ntwo [link](#a=b)\n\n- a\n- **b**") shouldBe
      Seq(
        MdBlock.Heading(2, Seq(MdInline.Text("Title "), MdInline.Code("x"))),
        MdBlock.Paragraph(Seq(MdInline.Text("One two "), MdInline.Link("link", "#a=b"))),
        MdBlock.Bullets(Seq(Seq(MdInline.Text("a")), Seq(MdInline.Bold("b"))))
      )
  }

  test("should color the function, types, and literals of a signature") {
    val ts = wvlet.lang.ui.playground.Markdown.tokens("string.substring(start: int): string")
    ts.filter(_.kind == "fn").map(_.text) shouldBe Seq("substring")
    ts.filter(_.kind == "type").map(_.text) shouldBe Seq("string", "int", "string")
    wvlet.lang.ui.playground.Markdown.tokens("'Wvlet'.upper").map(_.kind) shouldBe
      Seq("string", "punct", "fn")
  }

  private val engine = PlaygroundEngine(0.001)

  test("should run a cell with the definitions of the cells above it") {
    val cells = Seq(
      CellSource(Markdown, "## Vals"),
      CellSource(Code, "val n = 3"),
      CellSource(Code, "from nation\nlimit n")
    )
    val l   = NotebookText.render(cells)
    val out = engine.run(l.text, position = LinePosition(l.startLines(2) + 1, 1))
    out.error shouldBe None
    out.result.get.totalRows shouldBe 3
  }

end NotebookTest
