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
  * With several `segments` (the inputs of a UNION ALL, `union_all_pipelines`), the parts of every
  * segment run through that segment's pipeline, in segment order and then part order, as the union
  * emits them. A segment's scan is split when its parts are first needed, on the calling thread; a
  * segment whose source has a single part runs on the calling thread, after the parts before it.
  *
  * With `tail` (right / full joins), one more run over an empty input of the first segment's scan
  * follows the parts, on the calling thread, to emit the build rows no part matched.
  *
  * Falls back to running the pipeline over the whole scan when a single segment's source has a
  * single part.
  *
  * Each part's pipeline gets an [[IteratorOp]] that carries the part's row count when the source
  * knows it, so that operators can size their state per part (see [[PartialGroupSizer]]).
  */
class MorselOp(
    val segments: Seq[MorselOp.Segment],
    val schema: Schema,
    runner: TaskRunner,
    prepare: List[() => Unit],
    release: List[() => Unit],
    tail: Option[Operator => Operator]
) extends Operator:
  import MorselOp.*

  /** A pipeline over one scan */
  def this(
      scan: ScanOp,
      pipeline: Operator => Operator,
      schema: Schema,
      runner: TaskRunner,
      prepare: List[() => Unit] = Nil,
      release: List[() => Unit] = Nil,
      tail: Option[Operator => Operator] = None
  ) = this(Seq(MorselOp.Segment(scan, pipeline)), schema, runner, prepare, release, tail)

  require(segments.nonEmpty, "MorselOp needs a segment")

  /** The scan of the first segment */
  def scan: ScanOp = segments.head.scan

  override def name: String            = s"Morsel[parallelism=${runner.parallelism}]"
  override def children: Seq[Operator] = segments.map(s => s.pipeline(s.scan))
  private var started                  = false
  private var sequential: Operator     = null
  // Parts in output order: a task, or a segment run on the calling thread
  private val inFlight = mutable.Queue.empty[Item]
  private var running  = 0
  // The next segment to open, and the parts left of the one opened last
  private var nextSegment              = 0
  private var openSegment: Segment     = null
  private var splits: ScanSplits       = null
  private var nextPart                 = 0
  private val opened                   = mutable.ArrayBuffer.empty[ScanSplits]
  private var current: Iterator[Batch] = Iterator.empty
  private var streaming: Operator      = null
  private val window                   = math.max(2, runner.parallelism * 2)
  private var tailDone                 = false

  /** The number of steps run before the parts start (for tests) */
  def prepareSteps: Int = prepare.size

  /** The tail pipeline over an empty input, run on the calling thread after every part */
  private def runTail(): Seq[Batch] =
    val op = tail.get(IteratorOp(scan.schema, () => Iterator.empty, scan.name))
    try op.drain()
    finally op.close()

  private def runPart(seg: Segment, s: ScanSplits, k: Int): Seq[Batch] =
    val rows =
      if k < s.rows.size then
        s.rows(k)
      else
        -1L
    val src  = s.parts(k)
    val sc   = seg.scan
    val part = IteratorOp(sc.schema, () => src().map(sc.narrow), sc.name, rows)
    val op   = seg.pipeline(part)
    try op.drain()
    finally op.close()

  /** Opens the next segment: its parts, or one run of it on the calling thread */
  private def openNext(): Boolean =
    if nextSegment >= segments.size then
      false
    else
      val seg = segments(nextSegment)
      nextSegment += 1
      seg.scan.source.scanSplits(seg.scan.spec) match
        case Some(s) =>
          opened += s
          openSegment = seg
          splits = s
          nextPart = 0
        case None =>
          inFlight.enqueue(Item.OnCaller(seg))
      true

  private def fill(): Unit =
    var more = true
    while more && running < window do
      if splits != null && nextPart < splits.parts.size then
        val (seg, s, k) = (openSegment, splits, nextPart)
        inFlight.enqueue(Item.Task(runner.submit(() => runPart(seg, s, k))))
        running += 1
        nextPart += 1
      else
        splits = null
        more = openNext()

  def next(): Batch =
    if !started then
      started = true
      // e.g. hash join builds, which parts share and must not wait for inside pool threads
      prepare.foreach(_())
      if segments.size == 1 then
        val s = segments.head
        s.scan.source.scanSplits(s.scan.spec) match
          case Some(sp) =>
            opened += sp
            openSegment = s
            splits = sp
            nextSegment = 1
          case None =>
            // A single run is both the only part and the tail
            sequential = tail.getOrElse(s.pipeline)(s.scan)
    if sequential != null then
      return sequential.next()
    while !current.hasNext do
      if streaming != null then
        val b = streaming.next()
        if b != null then
          return b
        streaming.close()
        streaming = null
      fill()
      if inFlight.isEmpty then
        if tail.isEmpty || tailDone then
          return null
        tailDone = true
        current = runTail().iterator
      else
        inFlight.dequeue() match
          case Item.Task(h) =>
            running -= 1
            current = h.get().iterator
          case Item.OnCaller(seg) =>
            streaming = seg.pipeline(seg.scan)
    current.next()

  end next

  override def close(): Unit =
    if sequential != null then
      sequential.close()
    if streaming != null then
      streaming.close()
      streaming = null
    // Parts that haven't started are dropped; running ones finish before the sources close
    val tasks = inFlight.collect { case Item.Task(h) =>
      h
    }
    tasks.foreach(_.cancel())
    tasks.foreach { h =>
      try
        h.get()
      catch
        case _: Throwable =>
    }
    inFlight.clear()
    opened.foreach(_.close())
    opened.clear()
    splits = null
    release.foreach(_())

end MorselOp

object MorselOp:
  /** A scan and the pipeline that each of its parts runs through */
  case class Segment(scan: ScanOp, pipeline: Operator => Operator)

  private enum Item:
    case Task(handle: TaskRunner.Handle[Seq[Batch]])
    case OnCaller(segment: Segment)
