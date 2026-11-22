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

  /** Indented physical plan for EXPLAIN-style debugging */
  def explain(indent: Int = 0): String =
    val self =
      s"${"  " * indent}${name}${details} -> [${schema
          .fields
          .map(f => s"${f.name}:${f.ptype}")
          .mkString(", ")}]"
    (self +: children.map(_.explain(indent + 1))).mkString("\n")

  protected def details: String = ""

  /** Drain every remaining batch */
  def drain(): Seq[Batch] =
    val b = Seq.newBuilder[Batch]
    var x = next()
    while x != null do
      b += x
      x = next()
    b.result()

end Operator

object Operator:
  /** Pull every row of the operator into one batch */
  def materialize(op: Operator): Batch =
    val batches = op.drain()
    if batches.isEmpty then
      Batch.empty(op.schema)
    else
      Batch.concat(op.schema, batches)

/** Batches computed up front (VALUES, materialized subqueries, in-memory tables) */
class BatchesOp(val schema: Schema, batches: Seq[Batch], label: String = "Batches")
    extends Operator:
  private val it    = batches.iterator.filter(_.size > 0)
  def next(): Batch =
    if it.hasNext then
      it.next()
    else
      null

  override def name: String = label

/** Batches produced lazily (file scans) */
class IteratorOp(val schema: Schema, mk: () => Iterator[Batch], label: String) extends Operator:
  private lazy val it       = mk()
  override def name: String = label
  def next(): Batch         =
    var b: Batch = null
    while b == null && it.hasNext do
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
      })"

  def next(): Batch =
    var out: Batch = null
    while out == null do
      val b = child.next()
      if b == null then
        return null
      val mask = Kernels.trueMask(cond.eval(b))
      val in   =
        if keep == null then
          b
        else
          b.select(keep)
      val r = in.filter(mask)
      if r.size > 0 then
        out = r
    out

end FilterOp

class ProjectOp(val child: Operator, val exprs: IndexedSeq[VExpr], val schema: Schema)
    extends Operator:
  override def children: Seq[Operator] = Seq(child)
  override protected def details       = s"(${exprs.map(_.sig).mkString(", ")})"
  def next(): Batch                    =
    val b = child.next()
    if b == null then
      null
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

case class SortKey(expr: VExpr, ascending: Boolean, nullsFirst: Boolean)

/** Full sort; with a limit only the top rows are kept */
class SortOp(val child: Operator, val keys: Seq[SortKey], val limit: Option[Long] = None)
    extends Operator:
  private var result: Iterator[Batch]  = null
  def schema: Schema                   = child.schema
  override def children: Seq[Operator] = Seq(child)
  override protected def details       =
    s"(${keys
        .map(k =>
          s"${k.expr.sig}${
              if k.ascending then
                ""
              else
                " desc"
            }"
        )
        .mkString(", ")}${limit.map(l => s", limit=${l}").getOrElse("")})"

  def next(): Batch =
    if result == null then
      result = sortAll()
    if result.hasNext then
      result.next()
    else
      null

  private def sortAll(): Iterator[Batch] =
    val all = Operator.materialize(child)
    if all.size == 0 then
      return Iterator.empty
    val keyVecs = keys.map(k => k.expr.eval(all)).toArray
    val cmp     = SortOp.comparator(keyVecs, keys)
    val idx     = Array.tabulate(all.size)(i => Integer.valueOf(i))
    java.util.Arrays.sort(idx, cmp)
    val n      = limit.map(l => math.min(l, all.size.toLong).toInt).getOrElse(all.size)
    val sorted = all.gather(idx.take(n).map(_.intValue()), n)
    Iterator
      .range(0, n, Batch.DefaultSize)
      .map(from => sorted.slice(from, from + Batch.DefaultSize))

end SortOp

object SortOp:
  def comparator(keyVecs: Array[Vec], keys: Seq[SortKey]): java.util.Comparator[Integer] =
    val asc        = keys.map(_.ascending).toArray
    val nullsFirst = keys.map(_.nullsFirst).toArray
    (a: Integer, b: Integer) =>
      val i = a.intValue()
      val j = b.intValue()
      var c = 0
      var k = 0
      while c == 0 && k < keyVecs.length do
        val v  = keyVecs(k)
        val ni = v.isNull(i)
        val nj = v.isNull(j)
        c =
          if ni && nj then
            0
          else if ni then
            if nullsFirst(k) then
              -1
            else
              1
          else if nj then
            if nullsFirst(k) then
              1
            else
              -1
          else
            val r =
              v match
                case l: LongVec =>
                  java.lang.Long.compare(l.values(i), l.values(j))
                case x: IntVec =>
                  Integer.compare(x.values(i), x.values(j))
                case d: DoubleVec =>
                  java.lang.Double.compare(d.values(i), d.values(j))
                case s: StrVec =>
                  s.values(i).compareTo(s.values(j))
                case other =>
                  ValueOps.compare(other.get(i), other.get(j))
            if asc(k) then
              r
            else
              -r
        k += 1
      end while
      c

  end comparator

end SortOp

/**
  * Hash aggregation. Output columns: grouping keys, then one column per aggregate. Without keys,
  * exactly one row is produced, even for an empty input
  */
class HashAggOp(
    val child: Operator,
    val keys: IndexedSeq[VExpr],
    val aggs: IndexedSeq[AggCall],
    val schema: Schema
) extends Operator:
  private var done                     = false
  override def children: Seq[Operator] = Seq(child)
  override protected def details       =
    s"(keys=[${keys.map(_.sig).mkString(", ")}], aggs=[${aggs.map(_.sig).mkString(", ")}])"

  def next(): Batch =
    if done then
      return null
    done = true
    val table = GroupTable(keys.map(_.ptype))
    val accs  = aggs.map(_.newAccumulator())
    var b     = child.next()
    while b != null do
      val groups =
        if keys.isEmpty then
          new Array[Int](b.size)
        else
          table.groupIds(keys.map(_.eval(b)).toArray, b.size)
      val numGroups =
        if keys.isEmpty then
          1
        else
          table.numGroups
      var a = 0
      while a < accs.size do
        val agg = aggs(a)
        val acc = accs(a)
        acc.ensureGroups(numGroups)
        val argVecs = agg.args.map(_.eval(b)).toArray
        agg.filter match
          case Some(f) =>
            val mask = Kernels.trueMask(f.eval(b))
            var cnt  = 0
            var i    = 0
            while i < b.size do
              if mask(i) then
                cnt += 1
              i += 1
            val idx = new Array[Int](cnt)
            val gs  = new Array[Int](cnt)
            var j   = 0
            i = 0
            while i < b.size do
              if mask(i) then
                idx(j) = i
                gs(j) = groups(i)
                j += 1
              i += 1
            acc.update(gs, cnt, argVecs.map(_.gather(idx, cnt)))
          case None =>
            acc.update(groups, b.size, argVecs)
        a += 1
      b = child.next()
    end while
    val numGroups =
      if keys.isEmpty then
        1
      else
        table.numGroups
    if numGroups == 0 then
      return null
    accs.foreach(_.ensureGroups(numGroups))
    val keyVecs =
      if keys.isEmpty then
        Array.empty[Vec]
      else
        table.keyVectors()
    val aggVecs = accs.map(_.result(numGroups))
    Batch(keyVecs ++ aggVecs, numGroups)

  end next

end HashAggOp

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
  private val table                    = GroupTable(child.schema.types)
  def schema: Schema                   = child.schema
  override def children: Seq[Operator] = Seq(child)
  def next(): Batch                    =
    var out: Batch = null
    while out == null do
      val b = child.next()
      if b == null then
        return null
      val before = table.numGroups
      val ids    = table.groupIds(b.columns, b.size)
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
    out

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

  def next(): Batch =
    if done then
      return null
    done = true
    val counts = mutable.HashMap.empty[AnyRef, Int]
    var b      = right.next()
    while b != null do
      val cols = b.columns.zipWithIndex.map((v, i) => Kernels.castTo(v, schema(i).ptype))
      var i    = 0
      while i < b.size do
        val k = GroupKey.of(cols, i)
        counts(k) = counts.getOrElse(k, 0) + 1
        i += 1
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
      lb = left.next()
    end while
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

enum JoinKind:
  case Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
    Cross

/**
  * The hash table of a join's build (right) input: its rows, and for each distinct key the chain of
  * rows holding it. Built once on first use, then only read, so probes may share it across threads
  */
final class JoinBuild(right: Operator, rightKeys: IndexedSeq[VExpr]):
  @volatile
  private var built = false

  var build: Batch      = null
  var table: GroupTable = null
  var head: Array[Int]  = null
  var chain: Array[Int] = null

  def ensureBuilt(): Unit =
    if !built then
      synchronized {
        if !built then
          buildTable()
          built = true
      }

  private def buildTable(): Unit =
    build = Operator.materialize(right)
    val keyVecs = rightKeys.map(_.eval(build)).toArray
    table = GroupTable(rightKeys.map(_.ptype))
    // Rows with NULL keys never match
    val valid = new Array[Int](build.size)
    var nv    = 0
    var i     = 0
    while i < build.size do
      if !GroupKey.hasNull(keyVecs, i) then
        valid(nv) = i
        nv += 1
      i += 1
    val validKeys = keyVecs.map(_.gather(valid, nv))
    val ids       = table.groupIds(validKeys, nv)
    head = Array.fill(table.numGroups)(-1)
    chain = Array.fill(build.size)(-1)
    // Insert in reverse so chains enumerate rows in input order
    var j = nv - 1
    while j >= 0 do
      val row = valid(j)
      val g   = ids(j)
      chain(row) = head(g)
      head(g) = row
      j -= 1

end JoinBuild

/**
  * Equi hash join building on the right input. Output: left columns then right columns (Semi /
  * Anti: left columns only). `residual` is an extra condition over the joined row.
  *
  * `shared` is a build already made for this join elsewhere: copies of the join that probe parts of
  * the left input in parallel share one build
  */
class HashJoinOp(
    val left: Operator,
    val right: Operator,
    val leftKeys: IndexedSeq[VExpr],
    val rightKeys: IndexedSeq[VExpr],
    val kind: JoinKind,
    val residual: Option[VExpr],
    val schema: Schema,
    shared: JoinBuild = null
) extends Operator:
  override def children: Seq[Operator] = Seq(left, right)
  override def name: String            = s"HashJoin[${kind}]"
  override protected def details       =
    s"(${leftKeys.map(_.sig).mkString(",")} = ${rightKeys.map(_.sig).mkString(",")}${residual
        .map(r => s", residual=${r.sig}")
        .getOrElse("")})"

  /** The build this join probes */
  val joinBuild: JoinBuild =
    if shared != null then
      shared
    else
      JoinBuild(right, rightKeys)

  /** The same join over another left input, sharing this join's build */
  def withLeft(newLeft: Operator): HashJoinOp = HashJoinOp(
    newLeft,
    right,
    leftKeys,
    rightKeys,
    kind,
    residual,
    schema,
    joinBuild
  )

  override def close(): Unit =
    // A shared build's right input belongs to the join that made the build
    if shared == null then
      super.close()
    else
      left.close()

  private var built                        = false
  private var build: Batch                 = null
  private var table: GroupTable            = null
  private var head: Array[Int]             = null
  private var chain: Array[Int]            = null
  private var buildMatched: Array[Boolean] = null
  private var emittedUnmatched             = false
  private val leftWidth                    = left.schema.size

  private def buildTable(): Unit =
    joinBuild.ensureBuilt()
    build = joinBuild.build
    table = joinBuild.table
    head = joinBuild.head
    chain = joinBuild.chain
    if kind == JoinKind.Right || kind == JoinKind.Full then
      buildMatched = new Array[Boolean](build.size)
    built = true

  def next(): Batch =
    if !built then
      buildTable()
    var out: Batch = null
    while out == null do
      val probe = left.next()
      if probe == null then
        return emitUnmatchedBuild()
      out = probeBatch(probe)
    out

  private def emitUnmatchedBuild(): Batch =
    if buildMatched == null || emittedUnmatched then
      return null
    emittedUnmatched = true
    val idx = (0 until build.size).filter(i => !buildMatched(i)).toArray
    if idx.isEmpty then
      return null
    val n       = idx.length
    val pad     = Array.fill(n)(-1)
    val leftPad = left.schema.types.map(t => Vec.nullVec(t, n)).toArray
    Batch(leftPad ++ build.gather(idx, n).columns, n)

  private def probeBatch(probe: Batch): Batch =
    val keyVecs = leftKeys.map(_.eval(probe)).toArray
    val ids     = table.lookup(keyVecs, probe.size)
    val li      = mutable.ArrayBuilder.make[Int]
    val ri      = mutable.ArrayBuilder.make[Int]
    var i       = 0
    while i < probe.size do
      val g = ids(i)
      if g >= 0 then
        var r = head(g)
        while r >= 0 do
          li += i
          ri += r
          r = chain(r)
      i += 1
    var lIdx = li.result()
    var rIdx = ri.result()
    // Apply the residual condition on candidate pairs
    residual.foreach { cond =>
      if lIdx.nonEmpty then
        val joined = Batch(
          probe.gather(lIdx, lIdx.length).columns ++ build.gather(rIdx, rIdx.length).columns,
          lIdx.length
        )
        val mask = Kernels.trueMask(cond.eval(joined))
        val keep = (0 until lIdx.length).filter(mask(_)).toArray
        lIdx = keep.map(lIdx)
        rIdx = keep.map(rIdx)
    }
    kind match
      case JoinKind.Inner | JoinKind.Cross =>
        if lIdx.isEmpty then
          Batch(probe.gather(lIdx, 0).columns ++ build.gather(rIdx, 0).columns, 0) match
            case _ =>
              null
        else
          Batch(
            probe.gather(lIdx, lIdx.length).columns ++ build.gather(rIdx, rIdx.length).columns,
            lIdx.length
          )
      case JoinKind.Left | JoinKind.Full | JoinKind.Right =>
        if buildMatched != null then
          rIdx.foreach(r => buildMatched(r) = true)
        if kind == JoinKind.Right then
          if lIdx.isEmpty then
            null
          else
            Batch(
              probe.gather(lIdx, lIdx.length).columns ++ build.gather(rIdx, rIdx.length).columns,
              lIdx.length
            )
        else
          val matched = new Array[Boolean](probe.size)
          lIdx.foreach(l => matched(l) = true)
          val unmatched = (0 until probe.size).filter(!matched(_)).toArray
          val allL      = lIdx ++ unmatched
          val allR      = rIdx ++ Array.fill(unmatched.length)(-1)
          if allL.isEmpty then
            null
          else
            Batch(
              probe.gather(allL, allL.length).columns ++ build.gather(allR, allR.length).columns,
              allL.length
            )
      case JoinKind.Semi =>
        val matched = new Array[Boolean](probe.size)
        lIdx.foreach(l => matched(l) = true)
        val r = probe.filter(matched)
        if r.size == 0 then
          null
        else
          r
      case JoinKind.Anti =>
        val keep = new Array[Boolean](probe.size)
        java.util.Arrays.fill(keep, true)
        lIdx.foreach(l => keep(l) = false)
        val r = probe.filter(keep)
        if r.size == 0 then
          null
        else
          r
    end match

  end probeBatch

end HashJoinOp

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

  def next(): Batch =
    if build == null then
      build = Operator.materialize(right)
      if kind == JoinKind.Right || kind == JoinKind.Full then
        buildMatched = new Array[Boolean](build.size)
    var out: Batch = null
    while out == null do
      val probe = left.next()
      if probe == null then
        return emitUnmatchedBuild()
      out = probeBatch(probe)
    out

  private def emitUnmatchedBuild(): Batch =
    if buildMatched == null || emittedUnmatched then
      return null
    emittedUnmatched = true
    val idx = (0 until build.size).filter(i => !buildMatched(i)).toArray
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
        val allR      = rIdx ++ Array.fill(unmatched.length)(-1)
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
        val keep = Array.fill(nl)(true)
        lIdx.foreach(l => keep(l) = false)
        val r = probe.filter(keep)
        if r.size == 0 then
          null
        else
          r
    end match

  end probeBatch

end NestedLoopJoinOp
