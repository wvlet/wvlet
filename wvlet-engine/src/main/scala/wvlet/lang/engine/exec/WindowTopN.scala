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

import wvlet.lang.engine.vector.*

/**
  * The chunked input of a window under a rank bound ([[WindowOp.streamTopN]]): each chunk keeps
  * only the rows within the top of their partitions
  */
object WindowTopN:
  /** Input rows per chunk of a streamed top-N */
  val ChunkRows: Int = 64 * 1024

  /**
    * The rows of `b` within the top `t` of their partitions of the window of `c`
    * ([[WindowOp.topRows]]), in the order of `b`, or `b` itself when every row is
    */
  def keepTop(b: Batch, c: WindowCall, t: WindowOp.TopN): Batch =
    val n = b.size
    if n <= t.limit then
      return b
    val keyVecs = c.orderBy.map(_.expr.eval(b)).toArray
    val cmp     = SortOp.comparator(keyVecs, c.orderBy)
    val kept    = new Array[Boolean](n)
    var count   = 0
    val groups  =
      if c.partitionBy.isEmpty then
        new Array[Int](n)
      else
        GroupTable(c.partitionBy.map(_.ptype).toIndexedSeq).groupIds(
          c.partitionBy.map(_.eval(b)).toArray,
          n
        )
    groupRows(groups, null, n).foreach { part =>
      val top = WindowOp.topRows(part, t, cmp)
      var k   = 0
      while k < top.length do
        kept(top(k)) = true
        k += 1
      count += top.length
    }
    if count == n then
      b
    else
      val rows = new Array[Int](count)
      var j    = 0
      var i    = 0
      while i < n do
        if kept(i) then
          rows(j) = i
          j += 1
        i += 1
      b.gather(rows, count)

  end keepTop

  /**
    * `rows` (0 until n when null) grouped by their `groups` (ids from 0), in the order of `rows`
    * within a group: a counting sort by group id. Also groups the window partitions of [[WindowOp]]
    */
  private[exec] def groupRows(groups: Array[Int], rows: Array[Int], n: Int): Array[Array[Int]] =
    var numGroups = 0
    var i         = 0
    while i < n do
      if groups(i) >= numGroups then
        numGroups = groups(i) + 1
      i += 1
    val start = new Array[Int](numGroups + 1)
    i = 0
    while i < n do
      start(groups(i) + 1) += 1
      i += 1
    var g = 0
    while g < numGroups do
      start(g + 1) += start(g)
      g += 1
    val pos     = java.util.Arrays.copyOf(start, numGroups)
    val ordered = new Array[Int](n)
    i = 0
    while i < n do
      val gi = groups(i)
      ordered(pos(gi)) =
        if rows == null then
          i
        else
          rows(i)
      pos(gi) += 1
      i += 1
    val out = new Array[Array[Int]](numGroups)
    g = 0
    while g < numGroups do
      out(g) = java.util.Arrays.copyOfRange(ordered, start(g), start(g + 1))
      g += 1
    out

  end groupRows

end WindowTopN
