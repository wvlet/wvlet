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

/**
  * A bundled example query. The first comment line of the file is its title, and the comment lines
  * right after it are its description.
  *
  * @param path
  *   the file path under `src/main/wvlet/Examples`, e.g. `01_basics/03_where.wv`
  */
case class Example(
    path: String,
    category: String,
    title: String,
    description: String,
    text: String
):
  def mode: InputMode =
    if path.endsWith(".sql") then
      InputMode.SQL
    else
      InputMode.Wvlet

/** Examples of one category, in file order */
case class ExampleCategory(id: String, label: String, examples: Seq[Example])

/** The example library of the playground: a syntax and function reference that runs */
object Examples:

  // Folder name (without the order prefix) -> category label
  private val categoryLabels = Map(
    "basics"         -> "Basics",
    "columns"        -> "Column Operations",
    "aggregation"    -> "Aggregation",
    "joins"          -> "Joins",
    "set_operations" -> "Set Operations",
    "queries"        -> "Subqueries & Models",
    "window"         -> "Window Functions",
    "expressions"    -> "Expressions",
    "functions"      -> "Functions",
    "tpch"           -> "TPC-H Queries",
    "sql"            -> "SQL to Wvlet"
  )

  lazy val all: Seq[Example] = SampleQuery.allFiles.toSeq.map((path, text) => parse(path, text))

  lazy val categories: Seq[ExampleCategory] = all
    .groupBy(_.category)
    .toSeq
    .sortBy(_._1)
    .map { (folder, examples) =>
      ExampleCategory(folder, categoryLabel(folder), examples.sortBy(_.path))
    }

  /** The example shown on first visit */
  def default: Example = all
    .find(_.path.endsWith("03_aggregation/01_group_by.wv"))
    .getOrElse(all.head)

  def find(path: String): Option[Example] = all.find(_.path == path)

  def categoryLabel(folder: String): String =
    val name = folder.dropWhile(c => c.isDigit || c == '_')
    categoryLabels.getOrElse(name, name.replace('_', ' ').capitalize)

  /** Parse the title and description from the leading comment lines of an example */
  def parse(path: String, text: String): Example =
    val folder = path.split('/').dropRight(1).headOption.getOrElse("")
    val header =
      text
        .linesIterator
        .takeWhile(_.trim.startsWith("--"))
        .map(_.trim.stripPrefix("--").trim)
        .toList
    val fileName = path
      .split('/')
      .last
      .replaceAll("\\.(wv|sql)$", "")
      .dropWhile(c => c.isDigit || c == '_')
    val title = header.headOption.filter(_.nonEmpty).getOrElse(fileName.replace('_', ' '))
    val desc  = header.drop(1).mkString(" ")
    Example(path, folder, title.stripPrefix("SQL: "), desc, text)

end Examples
