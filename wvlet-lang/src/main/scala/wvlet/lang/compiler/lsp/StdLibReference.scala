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

/**
  * An entry of the function reference: one function name and arity in a category, merged across the
  * receiver types that define it (`abs` on int, long, ..., decimal is one entry) and across dialect
  * variants.
  *
  * @param receivers
  *   the receiver types in reference order; `List("")` for a top-level function
  * @param signature
  *   how to call it: `string.upper: string`, `T.abs: T` when the receivers share one signature up
  *   to their own type, or `row_number: long`
  * @param engines
  *   empty when a dialect-neutral definition serves every engine; otherwise notes like
  *   `duckdb, trino` (or `date: duckdb, trino` per receiver)
  */
case class StdLibEntry(
    category: StdLibCategory,
    name: String,
    arity: Int,
    receivers: List[String],
    signature: String,
    description: String,
    engines: String,
    examples: List[String]
):
  /** A stable id, unique across the reference */
  def key: String = s"${category.id}:${name}/${arity}"

  def isTopLevel: Boolean = receivers == List("")

  /** The name with its argument names, telling overloads apart: `lag(offset, default)` */
  def label: String =
    val open = signature.indexOf('(')
    if arity == 0 || open < 0 then
      name
    else
      val args = signature.substring(open + 1, signature.indexOf(')', open))
      s"${name}(${args.split(",").map(_.trim.takeWhile(_ != ':')).mkString(", ")})"

  /** The receiver types when the signature uses `T` for them, e.g. `int, long, double` */
  def typeParam: Option[String] = Option.when(signature.startsWith("T."))(receivers.mkString(", "))

  /** Whether the name, receiver types, or description contains the (lowercase) search text */
  def matches(q: String): Boolean =
    q.isEmpty || name.toLowerCase.contains(q) || description.toLowerCase.contains(q) ||
      receivers.exists(_ == q)

end StdLibEntry

/** A category of the function reference and its entries */
case class StdLibReferenceSection(category: StdLibCategory, entries: List[StdLibEntry])

/**
  * The stdlib function reference, shared by the docs page and the playground: entries by category,
  * in [[StdLibCategories.all]] order
  */
object StdLibReference:

  lazy val sections: List[StdLibReferenceSection] = StdLibIndex
    .byCategory
    .map(c => StdLibReferenceSection(c.category, entriesOf(c.category, c.functions)))

  lazy val entries: List[StdLibEntry] = sections.flatMap(_.entries)

  def find(key: String): Option[StdLibEntry] = entries.find(_.key == key)

  /** Sections with the entries matching the search text; sections with no match are left out */
  def filter(q: String): List[StdLibReferenceSection] =
    val s = q.trim.toLowerCase
    sections.flatMap { sec =>
      val ms = sec.entries.filter(_.matches(s))
      Option.when(ms.nonEmpty)(sec.copy(entries = ms))
    }

  private def entriesOf(
      category: StdLibCategory,
      functions: List[StdLibFunctionDoc]
  ): List[StdLibEntry] =
    // `functions` come by receiver type, then source order; group in that order
    val keys = functions.map(f => (f.name, f.arity)).distinct
    keys.map { (name, arity) =>
      val defs      = functions.filter(f => f.name == name && f.arity == arity)
      val receivers = defs.map(_.ownerType).distinct
      StdLibEntry(
        category = category,
        name = name,
        arity = arity,
        receivers = receivers,
        signature = signatureOf(receivers, defs),
        // A doc comment often covers a group of overloads (`substring(start)` and
        // `substring(start, length)`), so an undocumented overload takes its siblings' doc
        description = Some(descriptionOf(defs))
          .filter(_.nonEmpty)
          .getOrElse(
            descriptionOf(functions.filter(f => f.name == name && receivers.contains(f.ownerType)))
          ),
        engines = enginesOf(receivers, defs),
        examples = defs.flatMap(_.examples).distinct
      )
    }

  private def withReturn(sig: String, ret: String): String =
    if ret.isEmpty then
      sig
    else
      s"${sig}: ${ret}"

  private def signatureOf(receivers: List[String], defs: List[StdLibFunctionDoc]): String =
    def sigOf(d: StdLibFunctionDoc) = withReturn(d.signature, d.returnType)
    receivers match
      case List("") =>
        sigOf(defs.head)
      case List(r) =>
        s"${r}.${sigOf(defs.head)}"
      case _ =>
        // Receivers whose signatures differ only in their own type (`or_else(other: int): int`)
        // share one signature over `T`; otherwise show the first receiver's
        def generic(r: String): String =
          val d = defs.find(_.ownerType == r).get
          sigOf(d).replaceAll(s"\\b${r}\\b", "T")
        val shapes = receivers.map(generic).distinct
        if shapes.size == 1 then
          s"T.${shapes.head}"
        else
          s"${receivers.head}.${sigOf(defs.head)}"

  /**
    * The doc comment. A dialect variant's comment explains that engine's quirk, so a
    * dialect-neutral definition's comment wins
    */
  private def descriptionOf(defs: List[StdLibFunctionDoc]): String =
    val neutral = defs.filter(_.dialects.isEmpty)
    (neutral ++ defs).map(_.description).find(_.nonEmpty).getOrElse("")

  private def enginesOf(receivers: List[String], defs: List[StdLibFunctionDoc]): String =
    val specialized = receivers.flatMap { r =>
      val ds = defs.filter(_.ownerType == r)
      Option.when(!ds.exists(_.dialects.isEmpty))(r -> ds.flatMap(_.dialects).distinct)
    }
    specialized match
      case Nil =>
        ""
      case List((_, ds)) if receivers.size == 1 =>
        ds.mkString(", ")
      case rs =>
        rs.map((r, ds) => s"${r}: ${ds.mkString(", ")}").mkString("; ")

end StdLibReference
