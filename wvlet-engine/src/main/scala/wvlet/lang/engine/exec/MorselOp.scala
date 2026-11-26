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
  * With `tails` (right / full joins), one more run over an empty input of its scan follows the
  * parts, on the calling thread, to emit the build rows no part matched. A splittable tail
  * (`parallel_outer_tail`) of a large build instead runs as tasks over disjoint ranges of the build
  * rows, and several tails (the inputs of a UNION ALL) run as tasks, emitted in tail and range
  * order.
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
    tails: Seq[MorselOp.Tail]
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
      tail: Option[MorselOp.Tail] = None
  ) = this(Seq(MorselOp.Segment(scan, pipeline)), schema, runner, prepare, release, tail.toSeq)

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

  /** The number of tasks the tails ran as (for tests); 0 when they ran on the calling thread */
  def tailTasks: Int        = tailTaskCount
  private var tailTaskCount = 0

  /** The tail pipeline over an empty input, for the unmatched build rows in [from, until) */
  private def runTail(t: Tail, from: Int, until: Int): Seq[Batch] =
    val sc = t.scan.getOrElse(scan)
    val op = t.run(from, until)(IteratorOp(sc.schema, () => Iterator.empty, sc.name))
    try op.drain()
    finally op.close()

  /**
    * Run the tails after every part: as tasks over ranges of the build rows when a tail splits and
    * its build is large, queued in tail and range order; one run of a single tail on the calling
    * thread
    */
  private def startTails(): Unit =
    val runs = tails.flatMap(t => MorselOp.tailRanges(t, runner.parallelism).map((t, _)))
    if runs.size <= 1 then
      current = runs.flatMap((t, r) => runTail(t, r._1, r._2)).iterator
    else
      tailTaskCount = runs.size
      runs.foreach { case (t, (from, until)) =>
        inFlight.enqueue(Item.Task(runner.submit(() => runTail(t, from, until)), null))
        running += 1
      }

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

  /** Starts submitting the parts of seg */
  private def openParts(seg: Segment, s: ScanSplits): Unit =
    opened += s
    openSegment = seg
    splits = s
    nextPart = 0

  /** Opens the next segment: its parts, or one run of it on the calling thread */
  private def openNext(): Boolean =
    if nextSegment >= segments.size then
      false
    else
      val seg = segments(nextSegment)
      nextSegment += 1
      seg.scan.source.scanSplits(seg.scan.spec) match
        case Some(s) =>
          openParts(seg, s)
        case None =>
          seg.scan.withDecodeRunner(runner)
          inFlight.enqueue(Item.OnCaller(seg))
      true

  private def fill(): Unit =
    var more = true
    while more && running < window do
      if splits != null && nextPart < splits.parts.size then
        val (seg, s, k) = (openSegment, splits, nextPart)
        // The segment's last part closes its splits once its batches are taken
        val last =
          if k == s.parts.size - 1 then
            s
          else
            null
        inFlight.enqueue(Item.Task(runner.submit(() => runPart(seg, s, k)), last))
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
        nextSegment = 1
        s.scan.source.scanSplits(s.scan.spec) match
          case Some(sp) =>
            openParts(s, sp)
          case None =>
            // The run's scan may still decode its columns in parallel
            s.scan.withDecodeRunner(runner)
            // A single run is both the only part and the tail
            sequential = tails.headOption.map(_.run(0, Int.MaxValue)).getOrElse(s.pipeline)(s.scan)
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
        if tails.isEmpty || tailDone then
          return null
        tailDone = true
        startTails()
      else
        inFlight.dequeue() match
          case Item.Task(h, last) =>
            running -= 1
            current = h.get().iterator
            if last != null then
              // Every part of its segment has finished: release what the splits hold
              last.close()
              opened -= last
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
    val tasks = inFlight.collect { case Item.Task(h, _) =>
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

  /**
    * The run after every part that emits a right / full join's unmatched build rows. `run(from,
    * until)` makes the copy of the operators that emits those of the build rows at [from, until).
    * With `splittable` (one right / full join, `parallel_outer_tail`), several runs over disjoint
    * ranges together emit every unmatched row once; `buildRows` is the build's size once the parts
    * have run. `scan`: the scan whose empty input the tail reads, when not the first segment's (an
    * input of a UNION ALL)
    */
  case class Tail(
      run: (Int, Int) => Operator => Operator,
      buildRows: () => Int = () => 0,
      splittable: Boolean = false,
      scan: Option[ScanOp] = None
  ):
    /** The same tail with f over its output */
    def map(f: Operator => Operator): Tail = copy(run =
      (from, until) => x => f(run(from, until)(x))
    )

  object Tail:
    /** A tail that runs once, over every build row */
    def whole(run: Operator => Operator): Tail = Tail((_, _) => run)

  /** Build rows below which a tail runs once on the calling thread */
  val MinTailSplitRows: Int = 1 << 16

  /**
    * Ranges [from, until) of the build rows that the tail's runs cover, one when it doesn't split
    */
  private[exec] def tailRanges(t: Tail, parallelism: Int): Seq[(Int, Int)] =
    val n =
      if t.splittable && parallelism > 1 then
        t.buildRows()
      else
        0
    if n < MinTailSplitRows then
      Seq((0, Int.MaxValue))
    else
      val count = math.min(parallelism * 2, (n + MinTailSplitRows / 2 - 1) / (MinTailSplitRows / 2))
      val step  = (n + count - 1) / count
      (0 until count).map(k =>
        (
          k * step,
          if k == count - 1 then
            Int.MaxValue
          else
            (k + 1) * step
        )
      )

  private enum Item:
    /** A part; `last`, when it is its segment's last part, the segment's splits */
    case Task(handle: TaskRunner.Handle[Seq[Batch]], last: ScanSplits)
    case OnCaller(segment: Segment)

end MorselOp
