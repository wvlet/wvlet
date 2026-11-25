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

import scala.collection.mutable

/**
  * Morsel-driven execution of a scan pipeline: the scan is cut into parts (e.g. Parquet row
  * groups), and each part runs through its own copy of the operators above the scan (`pipeline`:
  * filters, projections, hash join probes) as a task of `runner`. Up to twice the runner's
  * parallelism of parts are in flight; results are emitted in part order, so the output equals the
  * sequential one. `prepare` runs on the calling thread before any part starts (hash join builds,
  * so that tasks never wait for other tasks; one step may run several builds on coordinator
  * threads, see [[TaskRunner.runTogether]]), and `release` when the operator closes.
  *
  * With `tail` (right / full joins), one more run over an empty input follows the parts, on the
  * calling thread, to emit the build rows no part matched.
  *
  * Falls back to running the pipeline over the whole scan when the source has a single part.
  *
  * Each part's pipeline gets an [[IteratorOp]] that carries the part's row count when the source
  * knows it, so that operators can size their state per part (see [[PartialGroupSizer]]).
  */
class MorselOp(
    val scan: ScanOp,
    pipeline: Operator => Operator,
    val schema: Schema,
    runner: TaskRunner,
    prepare: List[() => Unit] = Nil,
    release: List[() => Unit] = Nil,
    tail: Option[Operator => Operator] = None
) extends Operator:
  override def name: String            = s"Morsel[parallelism=${runner.parallelism}]"
  override def children: Seq[Operator] = Seq(pipeline(scan))
  private var started                  = false
  private var splits: ScanSplits       = null
  private var sequential: Operator     = null
  private val inFlight                 = mutable.Queue.empty[TaskRunner.Handle[Seq[Batch]]]
  private var nextPart                 = 0
  private var current: Iterator[Batch] = Iterator.empty
  private val window                   = math.max(2, runner.parallelism * 2)
  private var tailDone                 = false

  /** The number of steps run before the parts start (for tests) */
  def prepareSteps: Int = prepare.size

  /** The tail pipeline over an empty input, run on the calling thread after every part */
  private def runTail(): Seq[Batch] =
    val op = tail.get(IteratorOp(scan.schema, () => Iterator.empty, scan.name))
    try op.drain()
    finally op.close()

  private def runPart(k: Int): Seq[Batch] =
    val rows =
      if k < splits.rows.size then
        splits.rows(k)
      else
        -1L
    val src  = splits.parts(k)
    val part = IteratorOp(scan.schema, () => src().map(scan.narrow), scan.name, rows)
    val op   = pipeline(part)
    try op.drain()
    finally op.close()

  private def fill(): Unit =
    while inFlight.size < window && nextPart < splits.parts.size do
      val k = nextPart
      inFlight.enqueue(runner.submit(() => runPart(k)))
      nextPart += 1

  def next(): Batch =
    if !started then
      started = true
      // e.g. hash join builds, which parts share and must not wait for inside pool threads
      prepare.foreach(_())
      scan.source.scanSplits(scan.spec) match
        case Some(s) =>
          splits = s
        case None =>
          // A single run is both the only part and the tail
          sequential = tail.getOrElse(pipeline)(scan)
    if sequential != null then
      return sequential.next()
    while !current.hasNext do
      fill()
      if inFlight.isEmpty then
        if tail.isEmpty || tailDone then
          return null
        tailDone = true
        current = runTail().iterator
      else
        current = inFlight.dequeue().get().iterator
    current.next()

  override def close(): Unit =
    if sequential != null then
      sequential.close()
    if splits != null then
      // Parts that haven't started are dropped; running ones finish before the source closes
      inFlight.foreach(_.cancel())
      inFlight.foreach { h =>
        try
          h.get()
        catch
          case _: Throwable =>
      }
      inFlight.clear()
      splits.close()
      splits = null
    release.foreach(_())

end MorselOp
