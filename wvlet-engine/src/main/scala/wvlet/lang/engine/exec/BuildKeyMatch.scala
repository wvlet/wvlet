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

import wvlet.lang.engine.expr.VExpr
import wvlet.lang.engine.vector.*

/**
  * Whether a row's key is in the build of at least one of several hash joins above it
  * (`left_match_prefilter`): a filter below an aggregation whose output those LEFT joins probe,
  * when the filter above the joins drops every row none of them matched. TPC-DS q78 aggregates the
  * store_sales rows of one year by (year, item, customer) and keeps only the groups that web or
  * catalog sales have; ~6K of its ~5M input rows pass.
  *
  * Each [[BuildKeyMatch.Source]] holds the row's probe key of one join, over this filter's input,
  * and that join's build, bound when the join is made (the joins
  * [[wvlet.lang.engine.plan.Parallelizer]] makes bind it again). The joins build before their probe
  * side runs, so the filter reads their tables. A build that isn't ready, or a filter that keeps
  * most of the rows it has seen (more than half, checked on each batch once it has seen
  * [[BuildKeyMatch.SampleRows]]), keeps every row: the joins and the filter above them still drop
  * the rows, so the filter only saves work.
  */
final class BuildKeyMatch(val sources: IndexedSeq[BuildKeyMatch.Source]) extends VExpr:
  import BuildKeyMatch.*

  // Rows seen and kept, updated without synchronization by the parts that share the filter: the
  // counts are only a sample for turning the filter off
  private var seen = 0L
  private var kept = 0L
  @volatile
  private var off = false

  def ptype: PType = PType.Bool
  def sig: String  =
    s"build_key_match(${sources.map(_.keys.map(_.sig).mkString("[", ",", "]")).mkString(",")})"

  override def isConstant: Boolean = false

  // The key columns read, so that column passes see them (and an unknown remap fails loudly)
  override def children: Seq[VExpr] = sources.flatMap(_.keys)

  /** The rows the filter has seen and kept so far (for tests) */
  private[engine] def counts: (Long, Long) = (seen, kept)

  /**
    * Whether an aggregation right above the filter sizes its group table by the share of rows the
    * filter kept (`prefilter_presize`)
    */
  var sizesAggregation: Boolean = true

  /**
    * The group count to pre-size for an aggregation that `groups` would size before the filter: in
    * proportion to the rows kept so far, rounded up, or `groups` while the filter has seen no row
    * or once it turned off (it then passes every row)
    */
  def presize(groups: Int): Int =
    val total = seen
    val pass  = kept
    if off || total <= 0 || groups <= 0 then
      groups
    else
      math.min(groups.toLong, (groups.toLong * pass + total - 1) / total).toInt

  def eval(b: Batch): Vec =
    val n = b.size
    if off || sources.exists(s => s.build == null || !s.build.isBuilt) then
      return Vec.constant(PType.Bool, true, n)
    val out  = new Array[Boolean](n)
    var left = n
    var k    = 0
    while k < sources.size && left > 0 do
      val s     = sources(k)
      val first = s.build.firstMatches(s.keys.map(_.eval(b)).toArray, n)
      var i     = 0
      while i < n do
        if !out(i) && first(i) >= 0 then
          out(i) = true
          left -= 1
        i += 1
      k += 1
    val total = seen + n
    val pass  = kept + (n - left)
    seen = total
    kept = pass
    if total >= SampleRows && pass * 2 > total then
      off = true
    BoolVec(out, null)

end BuildKeyMatch

object BuildKeyMatch:
  /** The rows after which a filter that keeps more than half of them turns off */
  inline val SampleRows = 1L << 16

  /** The probe key of one join over the filter's input, and the join's build once bound */
  final class Source(val keys: IndexedSeq[VExpr]):
    @volatile
    var build: JoinBuild = null

  /** The prefilter that sizes an aggregation over `child` (`prefilter_presize`), or null */
  private def below(child: Operator): BuildKeyMatch =
    child match
      case f: FilterOp =>
        f.cond match
          case m: BuildKeyMatch if m.sizesAggregation =>
            m
          case _ =>
            null
      case _ =>
        null

  /** A batch an aggregation hasn't read yet ([[firstBatch]]) */
  private val Unread: Batch = Batch.empty(Schema(IndexedSeq.empty))

  /**
    * An aggregation's first input batch, read before it sizes its group table when the input is a
    * prefilter that sizes it (`prefilter_presize`), so the filter has a sample; otherwise a
    * placeholder that [[orNext]] replaces by the first batch
    */
  def firstBatch(child: Operator): Batch =
    if below(child) != null then
      child.next()
    else
      Unread

  /** `b`, or the next batch of `child` when `b` is the placeholder of [[firstBatch]] */
  def orNext(b: Batch, child: Operator): Batch =
    if b eq Unread then
      child.next()
    else
      b

  /** The groups to pre-size for an aggregation over `child` that `groups` would size */
  def presize(child: Operator, groups: Int): Int =
    val m = below(child)
    if m == null then
      groups
    else
      m.presize(groups)

  /** Make `j`'s build the one its filters read */
  def bind(j: HashJoinOp, sources: Seq[Source]): j.type =
    j.prefilters = sources
    sources.foreach(_.build = j.joinBuild)
    j

end BuildKeyMatch
