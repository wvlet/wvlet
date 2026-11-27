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

import wvlet.lang.engine.catalog.*
import wvlet.lang.engine.vector.*

/**
  * The output of a pipeline breaker (e.g. a partitioned aggregation), read as a table whose parts
  * are its batches, so that the operators above it run as morsels over the batches. The breaker is
  * drained once, by the first scan of this source: a [[MorselOp]] does this on the calling thread
  * before any part starts, so that tasks never wait for it. Its output order is kept: parts are
  * emitted in batch order. `name` labels the source in EXPLAIN.
  */
class BreakerSource(val op: Operator, val name: String) extends TableSource:
  def schema: Schema = op.schema

  private var drained: IndexedSeq[Batch] = null
  private var closed                     = false

  private def batches: IndexedSeq[Batch] =
    if drained == null then
      drained = op.drain().toIndexedSeq.filter(_.size > 0)
    drained

  private def selector(spec: ScanSpec): Batch => Batch =
    spec.columns match
      case None =>
        b => b
      case Some(cols) =>
        val idx = cols.map(c => schema.names.indexOf(c)).toArray
        b => b.select(idx)

  def scan(spec: ScanSpec): Iterator[Batch] = batches.iterator.map(selector(spec))

  override def canSplit: Boolean = true

  override def scanSplits(spec: ScanSpec): Option[ScanSplits] =
    val bs = batches
    if bs.size <= 1 then
      None
    else
      val sel = selector(spec)
      Some(ScanSplits(bs.map(b => () => Iterator.single(sel(b))), () => (), bs.map(_.size.toLong)))

  override def sliceSplits(spec: ScanSpec, parts: Int): Option[ScanSplits] =
    batches match
      case IndexedSeq(b) =>
        ScanSplits.slices(selector(spec)(b), parts)
      case _ =>
        None

  override def explainDetail(indent: Int): Option[String] = Some(op.explain(indent))

  /** Close the breaker and drop its output */
  def close(): Unit =
    if !closed then
      closed = true
      drained = null
      op.close()

end BreakerSource
