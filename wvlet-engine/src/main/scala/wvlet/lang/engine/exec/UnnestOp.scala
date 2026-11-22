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
package wvlet.lang.engine.exec

import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*

/**
  * Expands list values into rows. For each input row, the lists are evaluated and the row repeats
  * once per element of the longest list; shorter lists are padded with NULLs, and NULL or empty
  * lists contribute no elements (a row whose lists are all empty produces nothing).
  *
  * Output: the input columns (when `keepInput`), then one column per list holding its elements.
  * With `outer` (LEFT JOIN unnest(...)), a row without elements is kept once with NULL elements
  */
class UnnestOp(
    child: Operator,
    lists: IndexedSeq[VExpr],
    keepInput: Boolean,
    val schema: Schema,
    outer: Boolean = false
) extends Operator:
  override def children: Seq[Operator] = Seq(child)
  override protected def details       = s"(${lists.map(_.sig).mkString(", ")})"

  private val elementTypes = schema.types.takeRight(lists.size)

  def next(): Batch =
    var out: Batch = null
    while out == null do
      val in = child.next()
      if in == null then
        return null
      out = expand(in)
    out

  private def elements(v: Any): Seq[Any] =
    v match
      case null =>
        Nil
      case s: Seq[?] =>
        s
      case m: Map[?, ?] =>
        // DuckDB unnests maps as key/value structs
        m.toSeq.map((k, x) => scala.collection.immutable.ListMap("key" -> k, "value" -> x))
      case other =>
        throw wvlet
          .lang
          .api
          .StatusCode
          .INVALID_ARGUMENT
          .newException(s"unnest expects a list, but got ${other}")

  private def expand(in: Batch): Batch =
    val vecs     = lists.map(_.eval(in))
    val rowIdx   = Array.newBuilder[Int]
    val builders = elementTypes.map(t => VecBuilder(t, in.size))
    var i        = 0
    while i < in.size do
      val ls = vecs.map(v => elements(v.get(i)))
      val n0 = ls.map(_.size).maxOption.getOrElse(0)
      val n  =
        if n0 == 0 && outer then
          1
        else
          n0
      var j = 0
      while j < n do
        rowIdx += i
        var k = 0
        while k < ls.size do
          val l = ls(k)
          if j < l.size && l(j) != null then
            builders(k).add(ValueOps.cast(l(j), PType.AnyT, elementTypes(k)))
          else
            builders(k).addNull()
          k += 1
        j += 1
      i += 1
    val idx = rowIdx.result()
    if idx.isEmpty then
      null
    else
      val inputCols =
        if keepInput then
          in.gather(idx, idx.length).columns
        else
          Array.empty[Vec]
      Batch(inputCols ++ builders.map(_.result()), idx.length)

  end expand

end UnnestOp
