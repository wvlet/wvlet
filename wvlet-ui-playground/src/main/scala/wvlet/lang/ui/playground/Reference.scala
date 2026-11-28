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

import wvlet.lang.compiler.lsp.StdLibEntry
import wvlet.lang.engine.StdLibExamples

/**
  * The function reference of the playground: the stdlib entries by category
  * ([[wvlet.lang.compiler.lsp.StdLibReference]], the same as the docs page), and the bundled
  * examples that call each function
  */
object Reference:

  /**
    * The bundled examples that call the function: `.name` for a member function (`n_name.upper`),
    * `name(` for a top-level one (`row_number()`)
    */
  def examplesUsing(e: StdLibEntry, examples: Seq[Example] = Examples.all): Seq[Example] =
    // stdlib names are identifiers, so they need no regex quoting
    val n       = e.name
    val pattern =
      if e.isTopLevel then
        s"(?:^|[^\\w.])${n}\\s*\\("
      else
        s"\\.${n}(?!\\w)"
    val r = pattern.r
    // Match line by line, skipping `--` comment lines, so prose mentioning a name doesn't count
    examples.filter(ex =>
      ex.text.linesIterator.exists(l => !l.trim.startsWith("--") && r.findFirstIn(l).isDefined)
    )

  /** The query text that opens an `@example` in the editor */
  def queryOf(example: String): String = StdLibExamples.queryOf(example) + "\n"

end Reference
