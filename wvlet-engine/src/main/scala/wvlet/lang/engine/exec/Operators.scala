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

import wvlet.lang.compiler.QueryCancel
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*

import scala.collection.mutable

/**
  * A pull-based physical operator. `next()` returns the next non-empty batch, or null at the end
  */
trait Operator:
  def schema: Schema
  def next(): Batch
  def children: Seq[Operator] = Nil
  def close(): Unit           = children.foreach(_.close())
  def name: String            = getClass.getSimpleName.stripSuffix("Op")

  /**
    * The physical plan for EXPLAIN, in flow style as the logical plan printer shows it: data flows
    * down, from the scans to this operator. The first child continues the flow (`↓ op`); the other
    * inputs (join build sides, shared CTE bodies) are nested above the operator that reads them (`↙
    * op`). Adding an operator to a plan thus adds one line instead of re-indenting its inputs
    */
  def explain(indent: Int = 0): String =
    val self =
      s"${name}${details} -> [${schema.fields.map(f => s"${f.name}:${f.ptype}").mkString(", ")}]"
    val inputs = children
    val flow   = inputs.headOption.map(_.explain(indent))
    val nested = inputs.drop(1).map(_.explain(indent + 1)) ++ extraInputs(indent + 1)
    val marker =
      if nested.nonEmpty then
        "↙ "
      else if flow.nonEmpty then
        "↓ "
      else
        ""
    (flow.toSeq ++ nested :+ s"${"  " * indent}${marker}${self}").mkString("\n")

  /**
    * The EXPLAIN text, at `indent`, of inputs that are not children (e.g. a shared CTE body that
    * runs once for every reader)
    */
  protected def extraInputs(indent: Int): Seq[String] = Nil

  /**
    * Inputs that are not children because they are computed once for several readers (e.g. the
    * right input of a parallel cross join), so that a walk over a plan can reach every operator
    */
  def sharedInputs: Seq[Operator] = Nil

  protected def details: String = ""

  /** Drain every remaining batch, compacted (see [[Batch]]) */
  def drain(): Seq[Batch] =
    val b = Seq.newBuilder[Batch]
    var x = next()
    while x != null do
      b += x.compact()
      x = next()
    b.result()

end Operator

object Operator:
  /** EXPLAIN marker of a filter or join that gathers its output eagerly (`late_gather` disabled) */
  private[exec] def eagerTag(lateGather: Boolean): String =
    if lateGather then
      ""
    else
      ", eager"

  /** Pull every row of the operator into one batch */
  def materialize(op: Operator): Batch = MemoryContext.NoOp.materialize(op)

/** Batches computed up front (VALUES, materialized subqueries, in-memory tables) */
class BatchesOp(val schema: Schema, batches: Seq[Batch], label: String = "Batches")
    extends Operator:
  private val it    = batches.iterator.filter(_.size > 0)
  def next(): Batch =
    if it.hasNext then
      QueryCancel.check() // Ctrl-C in the npm REPL (see ScanOp.next)
      it.next()
    else
      null

  override def name: String = label

/** Batches produced lazily (file scans) */
class IteratorOp(val schema: Schema, mk: () => Iterator[Batch], label: String, val rows: Long = -1L)
    extends Operator:
  private lazy val it       = mk()
  override def name: String = label
  def next(): Batch         =
    var b: Batch = null
    while b == null && it.hasNext do
      QueryCancel.check()
      val x = it.next()
      if x.size > 0 then
        b = x
    b

/** One row without columns: the input of a FROM-less SELECT */
class SingleRowOp extends Operator:
  private var done   = false
  val schema: Schema = Schema.empty
  def next(): Batch  =
    if done then
      null
    else
      done = true
      Batch.ofRows(1)

/**
  * Rows of the input where `cond` is true. With `keep`, only those input columns are output (in
  * that order), so columns only the condition reads aren't copied
  */
class FilterOp(val child: Operator, val cond: VExpr, val keep: Array[Int] = null) extends Operator:
  /** Whether the output columns are deferred instead of gathered (`late_gather`) */
  var lateGather: Boolean = true

  def withLateGather(enabled: Boolean): this.type =
    lateGather = enabled
    this

  /**
    * Whether AND / OR conditions run each part only on the surviving rows (`short_circuit_filters`)
    */
  var shortCircuit: Boolean = true

  /** Whether the parts of AND / OR run cheapest first (`reorder_conjuncts`) */
  var reorderConjuncts: Boolean = true

  /**
    * Whether parts of the condition over one dictionary-coded string column run once per dictionary
    * entry (`dictionary_filters`, [[DictionaryPredicates]])
    */
  var dictionaryFilters: Boolean = true

  /**
    * Whether per-entry parts of an AND / OR select positions over the surviving rows only
    * (`dictionary_selection`, [[Selection.DictLeaf]])
    */
  var dictionarySelection: Boolean = true

  /** The evaluation settings of `f`, for a copy of it over another input */
  def withSettingsOf(f: FilterOp): this.type =
    lateGather = f.lateGather
    shortCircuit = f.shortCircuit
    reorderConjuncts = f.reorderConjuncts
    dictionaryFilters = f.dictionaryFilters
    dictionarySelection = f.dictionarySelection
    this

  // The condition as evaluated, prepared on the first batch after the settings are final
  private lazy val evalCond: VExpr =
    if dictionaryFilters then
      DictionaryPredicates.wrap(cond, whole = dictionarySelection && shortCircuit)
    else
      cond

  // Prepared on the first batch, after the settings are final
  private lazy val selection: Selection.Node =
    if shortCircuit then
      val node = Selection.compile(evalCond, reorderConjuncts, dictionarySelection)
      if Selection.worthSelecting(node) then
        node
      else
        null
    else
      null

  val schema: Schema =
    if keep == null then
      child.schema
    else
      Schema(keep.toIndexedSeq.map(child.schema(_)))

  override def children: Seq[Operator] = Seq(child)
  override protected def details       =
    s"(${cond.sig}${
        if keep == null then
          ""
        else
          s", keep=${keep.mkString(",")}"
      }${Operator.eagerTag(lateGather)}${selectionTag})"

  // Marks a disabled short_circuit_filters / reorder_conjuncts on an AND / OR condition
  private def selectionTag: String =
    cond match
      case _: AndExpr | _: OrExpr if !shortCircuit =>
        ", full_eval"
      case _: AndExpr | _: OrExpr if !reorderConjuncts =>
        ", written_order"
      case _ =>
        ""

  def next(): Batch =
    var out: Batch = null
    while out == null do
      val b = child.next()
      if b == null then
        return null
      val in =
        if keep == null then
          b
        else
          b.select(keep)
      val r =
        selection match
          case null =>
            val mask = Kernels.trueMask(evalCond.eval(b))
            if lateGather then
              in.takeWhere(mask)
            else
              in.filter(mask)
          case node =>
            val idx = Selection.select(node, b)
            if idx == null then
              in
            else if lateGather then
              in.take(idx, idx.length)
            else
              in.gather(idx, idx.length)
      if r.size > 0 then
        out = r
    out

  end next

end FilterOp

class ProjectOp(val child: Operator, val exprs: IndexedSeq[VExpr], val schema: Schema)
    extends Operator:
  override def children: Seq[Operator] = Seq(child)
  override protected def details       = s"(${exprs.map(_.sig).mkString(", ")})"

  /**
    * Whether columns of a deferred input pass through deferred, and expressions over them compute
    * over their source rows when [[DenseSelectedRows]] allows (`dense_selected_rows`)
    */
  var denseSelected: Boolean = true

  /** Set [[denseSelected]] */
  def withDenseSelected(enabled: Boolean): this.type =
    denseSelected = enabled
    this

  def next(): Batch =
    val b = child.next()
    if b == null then
      null
    else if denseSelected && b.isDeferred then
      DenseSelectedRows.project(exprs, b)
    else
      Batch(exprs.map(_.eval(b)).toArray, b.size)

class LimitOp(val child: Operator, val limit: Long, val offset: Long) extends Operator:
  private var skipped                  = 0L
  private var emitted                  = 0L
  def schema: Schema                   = child.schema
  override def children: Seq[Operator] = Seq(child)
  override protected def details       = s"(${limit}, offset=${offset})"
  def next(): Batch                    =
    var out: Batch = null
    while out == null && emitted < limit do
      val b = child.next()
      if b == null then
        return null
      var from = 0
      if skipped < offset then
        val skip = math.min(offset - skipped, b.size.toLong).toInt
        skipped += skip
        from = skip
      if from < b.size then
        val take = math.min(limit - emitted, (b.size - from).toLong).toInt
        emitted += take
        out = b.slice(from, from + take)
    out

/** Concatenation of inputs with the same column count; values are cast to the output types */
class UnionAllOp(val inputs: Seq[Operator], val schema: Schema) extends Operator:
  private var current                  = 0
  override def children: Seq[Operator] = inputs
  def next(): Batch                    =
    while current < inputs.size do
      val b = inputs(current).next()
      if b != null then
        return Batch(
          b.columns.zipWithIndex.map((v, i) => Kernels.castTo(v, schema(i).ptype)),
          b.size
        )
      current += 1
    null

/** Removes duplicate rows */
class DistinctOp(val child: Operator) extends Operator:
  private var table: GroupIndex = null
  // The query's memory budget, captured where the distinct is planned: the seen rows' keys
  private val memory                        = MemoryContext.current.child()
  private val groups: MemoryContext.Growing = memory.growing(
    child.schema.types.map(MemoryContext.valueBytes).sum + HashAggOp.GroupTableBytes
  )

  /** Whether several integer, date, or decimal columns group by one packed key */
  var packKeys: Boolean = true

  /** Whether decimal columns are grouped by their unscaled Long values */
  var longDecimalKeys: Boolean = true

  /** Set [[packKeys]] and [[longDecimalKeys]] */
  def withKeyOptions(pack: Boolean, longDecimals: Boolean): this.type =
    packKeys = pack
    longDecimalKeys = longDecimals
    this

  def schema: Schema                   = child.schema
  override def children: Seq[Operator] = Seq(child)
  def next(): Batch                    =
    var out: Batch = null
    while out == null do
      val b = child.next()
      if b == null then
        // Every row was seen: the table is dropped
        table = null
        memory.release()
        return null
      if table == null then
        table = PackedGroupTable.forKeys(
          child.schema.types,
          0,
          packKeys,
          longDecimalKeys,
          stableCodes = true
        )
      val before = table.numGroups
      val ids    = table.groupIds(b.columns, b.size)
      groups.ensure(table.numGroups)
      // Keep the first row of every new group
      val keep = new Array[Int](b.size)
      var k    = 0
      var next = before
      var i    = 0
      while i < b.size do
        if ids(i) == next then
          keep(k) = i
          k += 1
          next += 1
        i += 1
      if k > 0 then
        out = b.gather(keep, k)
    end while
    out

  end next

end DistinctOp

/** INTERSECT / EXCEPT with set (distinct) or bag (ALL) semantics */
class SetOpOp(
    val left: Operator,
    val right: Operator,
    val isIntersect: Boolean,
    val all: Boolean,
    val schema: Schema
) extends Operator:
  private var done                     = false
  override def children: Seq[Operator] = Seq(left, right)
  override def name: String            =
    if isIntersect then
      "Intersect"
    else
      "Except"

  // The query's memory budget, captured where the set operation is planned: its key maps
  private val memory = MemoryContext.current.child()

  def next(): Batch =
    if done then
      return null
    done = true
    // A map entry per distinct row: its key's values and the entry around them
    val keys   = memory.growing(schema.types.map(MemoryContext.valueBytes).sum + SetOpOp.EntryBytes)
    val counts = mutable.HashMap.empty[AnyRef, Int]
    var b      = right.next()
    while b != null do
      val cols = b.columns.zipWithIndex.map((v, i) => Kernels.castTo(v, schema(i).ptype))
      var i    = 0
      while i < b.size do
        val k = GroupKey.of(cols, i)
        counts(k) = counts.getOrElse(k, 0) + 1
        i += 1
      keys.ensure(counts.size)
      b = right.next()
    val emitted = mutable.HashMap.empty[AnyRef, Int]
    val out     = schema.types.map(t => VecBuilder(t)).toArray
    var lb      = left.next()
    while lb != null do
      val cols = lb.columns.zipWithIndex.map((v, i) => Kernels.castTo(v, schema(i).ptype))
      var i    = 0
      while i < lb.size do
        val k       = GroupKey.of(cols, i)
        val inRight = counts.getOrElse(k, 0)
        val already = emitted.getOrElse(k, 0)
        val emit    =
          if isIntersect then
            if all then
              already < inRight
            else
              inRight > 0 && already == 0
          else if all then
            // EXCEPT ALL: emit occurrences beyond the right count
            val seenLeft = emitted.getOrElse(k, 0)
            emitted(k) = seenLeft + 1
            seenLeft + 1 > inRight
          else
            inRight == 0 && already == 0
        if emit then
          if isIntersect || !all then
            emitted(k) = already + 1
          var c = 0
          while c < cols.length do
            out(c).add(cols(c).get(i))
            c += 1
        else if isIntersect && !all then
          ()
        i += 1
      keys.ensure(counts.size + emitted.size)
      lb = left.next()
    end while
    // The maps are dropped on return
    memory.release()
    val vecs = out.map(_.result())
    val n    =
      if vecs.isEmpty then
        0
      else
        vecs(0).size
    if n == 0 then
      null
    else
      Batch(vecs, n)

  end next

end SetOpOp

object SetOpOp:
  // Bytes around one row's values in a key map: the entry, its key object, and boxed values
  private val EntryBytes = 64L

enum JoinKind:
  case Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
    Cross,
    /**
      * Build (right) rows with a match in the probe input, each once: a semi join with swapped
      * inputs
      */
    RightSemi,
    /** Build (right) rows without a match in the probe input: an anti join with swapped inputs */
    RightAnti

  /** Whether the join tracks which build rows matched, and emits build rows at the end */
  def tracksBuildMatches: Boolean =
    this match
      case Right | Full | RightSemi | RightAnti =>
        true
      case _ =>
        false

  /** Whether the output has only the build (right) columns */
  def outputsBuildOnly: Boolean = this == RightSemi || this == RightAnti

  /** Whether the output has only the probe (left) columns */
  def outputsProbeOnly: Boolean = this == Semi || this == Anti

/** Nested-loop join for non-equi conditions and cross joins (right input materialized) */
class NestedLoopJoinOp(
    val left: Operator,
    val right: Operator,
    val cond: Option[VExpr],
    val kind: JoinKind,
    val schema: Schema
) extends Operator:
  override def children: Seq[Operator]     = Seq(left, right)
  override def name: String                = s"NestedLoopJoin[${kind}]"
  override protected def details           = cond.map(c => s"(${c.sig})").getOrElse("")
  private var build: Batch                 = null
  private var buildMatched: Array[Boolean] = null
  private var emittedUnmatched             = false
  private var finished                     = false
  // The query's memory budget, captured where the join is planned: the build rows
  private val memory = MemoryContext.current.child()

  def next(): Batch =
    if finished then
      return null
    if build == null then
      build = memory.materialize(right)
      if kind == JoinKind.Right || kind == JoinKind.Full then
        buildMatched = new Array[Boolean](build.size)
    var out: Batch = null
    while out == null do
      val probe = left.next()
      if probe == null then
        val rest = emitUnmatchedBuild()
        if rest == null then
          // Probed and emitted: the build is dropped
          finished = true
          build = null
          buildMatched = null
          memory.release()
        return rest
      out = probeBatch(probe)
    out

  private def emitUnmatchedBuild(): Batch =
    if buildMatched == null || emittedUnmatched then
      return null
    emittedUnmatched = true
    val idx = HashJoinOp.positionsOf(buildMatched, build.size, false)
    if idx.isEmpty then
      return null
    val n       = idx.length
    val leftPad = left.schema.types.map(t => Vec.nullVec(t, n)).toArray
    Batch(leftPad ++ build.gather(idx, n).columns, n)

  private def probeBatch(probe: Batch): Batch =
    val nl = probe.size
    val nr = build.size
    // Process the cross product in chunks of left rows to bound memory
    val li    = mutable.ArrayBuilder.make[Int]
    val ri    = mutable.ArrayBuilder.make[Int]
    val chunk = math.max(1, 65536 / math.max(1, nr))
    var start = 0
    while start < nl do
      val end = math.min(nl, start + chunk)
      val n   = (end - start) * nr
      if n > 0 then
        val lIdx = new Array[Int](n)
        val rIdx = new Array[Int](n)
        var k    = 0
        var i    = start
        while i < end do
          var j = 0
          while j < nr do
            lIdx(k) = i
            rIdx(k) = j
            k += 1
            j += 1
          i += 1
        cond match
          case Some(c) =>
            val joined = Batch(probe.gather(lIdx, n).columns ++ build.gather(rIdx, n).columns, n)
            val mask   = Kernels.trueMask(c.eval(joined))
            var x      = 0
            while x < n do
              if mask(x) then
                li += lIdx(x)
                ri += rIdx(x)
              x += 1
          case None =>
            li ++= lIdx
            ri ++= rIdx
      start = end
    end while
    val lIdx = li.result()
    val rIdx = ri.result()
    kind match
      case JoinKind.Inner | JoinKind.Cross | JoinKind.Right =>
        if buildMatched != null then
          rIdx.foreach(r => buildMatched(r) = true)
        if lIdx.isEmpty then
          null
        else
          Batch(
            probe.gather(lIdx, lIdx.length).columns ++ build.gather(rIdx, rIdx.length).columns,
            lIdx.length
          )
      case JoinKind.Left | JoinKind.Full =>
        if buildMatched != null then
          rIdx.foreach(r => buildMatched(r) = true)
        val matched = new Array[Boolean](nl)
        lIdx.foreach(l => matched(l) = true)
        val unmatched = (0 until nl).filter(!matched(_)).toArray
        val allL      = lIdx ++ unmatched
        val allR      = rIdx ++ Vec.filledInts(unmatched.length, -1)
        if allL.isEmpty then
          null
        else
          Batch(
            probe.gather(allL, allL.length).columns ++ build.gather(allR, allR.length).columns,
            allL.length
          )
      case JoinKind.Semi =>
        val matched = new Array[Boolean](nl)
        lIdx.foreach(l => matched(l) = true)
        val r = probe.filter(matched)
        if r.size == 0 then
          null
        else
          r
      case JoinKind.Anti =>
        val keep = Vec.allTrue(nl)
        lIdx.foreach(l => keep(l) = false)
        val r = probe.filter(keep)
        if r.size == 0 then
          null
        else
          r
      case JoinKind.RightSemi | JoinKind.RightAnti =>
        // Only hash joins swap semi / anti inputs
        throw IllegalStateException(s"Unsupported nested-loop join kind: ${kind}")
    end match

  end probeBatch

end NestedLoopJoinOp
