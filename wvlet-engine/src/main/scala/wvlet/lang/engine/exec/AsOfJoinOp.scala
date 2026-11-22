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
  * The inequality of an ASOF join, as `left <op> right`. Each left row matches the closest right
  * row satisfying it: the largest right value for `>=` / `>`, the smallest for `<=` / `<`
  */
enum AsOfOp:
  case Ge,
    Gt,
    Le,
    Lt

/**
  * ASOF join building on the right input: right rows are grouped by the equality keys and sorted by
  * the inequality value, and each left row binary-searches its group. Output: left columns then
  * right columns. `kind` is Inner or Left (unmatched left rows padded with NULLs)
  */
class AsOfJoinOp(
    left: Operator,
    right: Operator,
    leftKeys: IndexedSeq[VExpr],
    rightKeys: IndexedSeq[VExpr],
    leftValue: VExpr,
    rightValue: VExpr,
    op: AsOfOp,
    kind: JoinKind,
    val schema: Schema
) extends Operator:
  override def children: Seq[Operator] = Seq(left, right)
  override def name: String            = s"AsOfJoin[${kind}]"
  override protected def details       =
    s"(${leftKeys.map(_.sig).mkString(",")} = ${rightKeys.map(_.sig).mkString(",")}, ${leftValue
        .sig} ${op} ${rightValue.sig})"

  private var build: Batch      = null
  private var table: GroupTable = null
  // Per group: right row indexes sorted by value, and the values in the same order
  private var groupRows: Array[Array[Int]]   = null
  private var groupValues: Array[Array[Any]] = null

  private def buildTable(): Unit =
    build = Operator.materialize(right)
    val keyVecs = rightKeys.map(_.eval(build)).toArray
    val values  = rightValue.eval(build)
    // Rows with NULL keys or values never match
    val valid = (0 until build.size)
      .filter(i => !GroupKey.hasNull(keyVecs, i) && !values.isNull(i))
      .toArray
    val ids =
      if leftKeys.isEmpty then
        Array.fill(valid.length)(0)
      else
        table = GroupTable(rightKeys.map(_.ptype))
        table.groupIds(keyVecs.map(_.gather(valid, valid.length)), valid.length)
    val numGroups =
      if leftKeys.isEmpty then
        1
      else
        table.numGroups
    val grouped = Array.fill(numGroups)(Array.newBuilder[Int])
    valid.indices.foreach(k => grouped(ids(k)) += valid(k))
    // Sort each group by its values, read once per row
    val sorted = grouped.map { b =>
      b.result().map(r => (r, values.get(r))).sortWith((x, y) => ValueOps.compare(x._2, y._2) < 0)
    }
    groupRows = sorted.map(_.map(_._1))
    groupValues = sorted.map(_.map(_._2))

  def next(): Batch =
    if build == null then
      buildTable()
    var out: Batch = null
    while out == null do
      val probe = left.next()
      if probe == null then
        return null
      out = probeBatch(probe)
    out

  /** The index of the best match in sorted values, or -1 */
  private def search(vs: Array[Any], v: Any): Int =
    // Find the first position whose value is greater than (or not less than) v
    def firstAbove(strict: Boolean): Int =
      var lo = 0
      var hi = vs.length
      while lo < hi do
        val mid = (lo + hi) >>> 1
        val c   = ValueOps.compare(vs(mid), v)
        if c > 0 || (!strict && c == 0) then
          hi = mid
        else
          lo = mid + 1
      lo
    op match
      case AsOfOp.Ge =>
        // largest value <= v
        firstAbove(strict = true) - 1
      case AsOfOp.Gt =>
        // largest value < v
        firstAbove(strict = false) - 1
      case AsOfOp.Le =>
        // smallest value >= v
        val i = firstAbove(strict = false)
        if i < vs.length then
          i
        else
          -1
      case AsOfOp.Lt =>
        // smallest value > v
        val i = firstAbove(strict = true)
        if i < vs.length then
          i
        else
          -1

  end search

  private def probeBatch(probe: Batch): Batch =
    val keyVecs = leftKeys.map(_.eval(probe)).toArray
    val values  = leftValue.eval(probe)
    val ids     =
      if leftKeys.isEmpty then
        Array.fill(probe.size)(0)
      else
        table.lookup(keyVecs, probe.size)
    val li = Array.newBuilder[Int]
    val ri = Array.newBuilder[Int]
    var i  = 0
    while i < probe.size do
      val g = ids(i)
      val r =
        // lookup returns -1 for rows with a NULL key
        if g < 0 || values.isNull(i) then
          -1
        else
          val pos = search(groupValues(g), values.get(i))
          if pos < 0 then
            -1
          else
            groupRows(g)(pos)
      if r >= 0 || kind == JoinKind.Left then
        li += i
        ri += r
      i += 1
    val lIdx = li.result()
    val rIdx = ri.result()
    if lIdx.isEmpty then
      null
    else
      Batch(
        probe.gather(lIdx, lIdx.length).columns ++ build.gather(rIdx, rIdx.length).columns,
        lIdx.length
      )

  end probeBatch

end AsOfJoinOp
