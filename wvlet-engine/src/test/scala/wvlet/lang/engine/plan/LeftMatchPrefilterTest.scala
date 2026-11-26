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
package wvlet.lang.engine.plan

import wvlet.lang.engine.catalog.*
import wvlet.lang.engine.exec.*
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

/**
  * An aggregation whose output LEFT joins probe, under a filter that needs a match of one of them,
  * filters its input by their builds first (`left_match_prefilter`), with the same result
  */
class LeftMatchPrefilterTest extends UniTest:

  /** Runs each task when it is submitted, but plans as if tasks ran four at a time */
  private object FourAtATime extends TaskRunner:
    def parallelism: Int                               = 4
    def submit[A](task: () => A): TaskRunner.Handle[A] = TaskRunner.Sequential.submit(task)

  /** A table whose batches are its parts */
  private class Parts(val name: String, val schema: Schema, batches: Seq[Batch])
      extends TableSource:
    def scan(spec: ScanSpec): Iterator[Batch]                   = batches.iterator
    override def canSplit: Boolean                              = true
    override def scanSplits(spec: ScanSpec): Option[ScanSplits] = Some(
      ScanSplits(batches.toIndexedSeq.map(b => () => Iterator.single(b)), () => ())
    )

  private def longs(xs: Seq[Option[Long]]): Vec = LongVec(
    PType.Int64,
    xs.map(_.getOrElse(0L)).toArray,
    xs.map(_.isEmpty).toArray
  )

  /** A scan of (a, b, v) rows, `rowsPerPart` rows per part */
  private def table(name: String, rows: Seq[(Option[Long], Long, Long)], rowsPerPart: Int): ScanOp =
    val schema = Schema(
      IndexedSeq(
        Field(s"${name}_a", PType.Int64),
        Field(s"${name}_b", PType.Int64),
        Field(s"${name}_v", PType.Int64)
      )
    )
    val batches =
      rows
        .grouped(rowsPerPart)
        .map { part =>
          Batch(
            Array(
              longs(part.map(_._1)),
              longs(part.map(r => Some(r._2))),
              longs(part.map(r => Some(r._3)))
            ),
            part.size
          )
        }
        .toSeq
    val t = Parts(name, schema, batches)
    ScanOp(t, t.schema, None, None)

  // 600 probe rows over keys (a, b), a in 0..99 and b in 0..2, and a NULL a every 50 rows
  private def probe = table(
    "p",
    (0 until 600).map(i => (Option.when(i % 50 != 7)((i % 100).toLong), (i % 3).toLong, i.toLong)),
    100
  )

  // (1, 1) and (3, 0) have web rows; (4, 1) has one with v = 0, which the filter above drops
  private def web = table("w", Seq((Some(1L), 1L, 5L), (Some(3L), 0L, 7L), (Some(4L), 1L, 0L)), 2)

  // (1, 1) also has catalog rows, and (10, 1) only those
  private def catalog = table("c", Seq((Some(1L), 1L, 2L), (Some(10L), 1L, 3L)), 1)

  private def col(i: Int) = ColRef(i, PType.Int64, s"c${i}")

  private val zero = Const(0L, PType.Int64)

  private def positive(i: Int): VExpr = Compare(
    CmpOp.Gt,
    Coalesce(Seq(col(i), zero), PType.Int64),
    zero
  )

  /**
    * sum(v) of probe grouped by (a, b), renamed, LEFT JOIN web and catalog on (a, b), then `cond`
    * over the 9 joined columns (web's v at #5, catalog's at #8)
    */
  private def plan(cond: VExpr, joinOn: Int = 0): FilterOp =
    val p   = probe
    val agg = HashAggOp(
      p,
      IndexedSeq(col(0), col(1)),
      IndexedSeq(AggCall("sum", Seq(col(2)), false, None, PType.Int64)),
      Schema(IndexedSeq(Field("a", PType.Int64), Field("b", PType.Int64), Field("s", PType.Int64)))
    )
    val renamed = ProjectOp(agg, IndexedSeq(col(0), col(1), col(2)), agg.schema)
    def left(l: Operator, r: Operator) = HashJoinOp(
      l,
      r,
      IndexedSeq(col(joinOn), col(1)),
      IndexedSeq(col(0), col(1)),
      JoinKind.Left,
      None,
      l.schema ++ r.schema
    )
    val w = web
    val c = catalog
    FilterOp(left(left(renamed, w), c), cond)

  private val eitherMatched = OrExpr(positive(5), positive(8))

  private def rows(op: Operator): Seq[Seq[Any]] =
    try op
        .drain()
        .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).toSeq))
        .sortBy(_.toString)
    finally op.close()

  /** The prefilter conditions of a plan */
  private def prefilters(op: Operator): Seq[BuildKeyMatch] =
    val own =
      op match
        case f: FilterOp =>
          f.cond match
            case m: BuildKeyMatch =>
              Seq(m)
            case _ =>
              Nil
        case _ =>
          Nil
    own ++ op.children.flatMap(prefilters)

  test("should filter the aggregation input by the builds of both joins") {
    val plain     = rows(plan(eitherMatched))
    val rewritten = LeftMatchPrefilter(plan(eitherMatched))
    val Seq(m)    = prefilters(rewritten): @unchecked
    rewritten.explain() shouldContain "build_key_match([#0,#1],[#0,#1])"
    rows(rewritten) shouldBe plain
    // Two input rows each of (1, 1), (3, 0), (4, 1) and (10, 1) pass; (4, 1) fails the filter above
    plain.map(r => (r(0), r(1))).toSet shouldBe Set((1L, 1L), (3L, 0L), (10L, 1L))
    val (seen, kept) = m.counts
    seen shouldBe 600L
    kept shouldBe 8L
  }

  test("should filter the same way when the plan runs in parallel") {
    val plain     = rows(Parallelizer(plan(eitherMatched), FourAtATime))
    val rewritten = LeftMatchPrefilter(plan(eitherMatched))
    // The parts share the condition, bound to the builds of the joins Parallelizer makes
    val Seq(m) = prefilters(rewritten): @unchecked
    val par    = Parallelizer(rewritten, FourAtATime)
    par.explain() shouldContain "build_key_match"
    rows(par) shouldBe plain
    m.counts shouldBe (600L, 8L)
  }

  test("should give the joins whose builds the filter reads a miss filter") {
    val rewritten = LeftMatchPrefilter(plan(eitherMatched))
    val joins     = collection.mutable.ArrayBuffer.empty[HashJoinOp]
    Planner.foreachHashJoin(rewritten)(joins += _)
    joins.map(Planner.missFilterFits).toSeq shouldBe Seq(true, true)
  }

  test("should leave the plan alone when a part of the OR holds without a match") {
    val cond = OrExpr(positive(5), IsNullExpr(col(8), negate = false))
    prefilters(LeftMatchPrefilter(plan(cond))) shouldBe Nil
  }

  test("should leave the plan alone when the OR also reads probe columns") {
    val cond = OrExpr(positive(5), Compare(CmpOp.Gt, col(2), zero))
    prefilters(LeftMatchPrefilter(plan(cond))) shouldBe Nil
  }

  test("should leave the plan alone when the joins probe an aggregate value") {
    prefilters(LeftMatchPrefilter(plan(eitherMatched, joinOn = 2))) shouldBe Nil
  }

  test("should need a match of one join only, for a conjunct over that join alone") {
    val cond      = AndExpr(positive(8), Compare(CmpOp.Gt, col(2), zero))
    val plain     = rows(plan(cond))
    val rewritten = LeftMatchPrefilter(plan(cond))
    rewritten.explain() shouldContain "build_key_match([#0,#1])"
    rows(rewritten) shouldBe plain
    plain.map(r => (r(0), r(1))).toSet shouldBe Set((1L, 1L), (10L, 1L))
  }

  test("should keep every row before the build is ready, and once it keeps most rows") {
    val keys = table("k", (0L until 100L).map(i => (Some(i), 0L, 0L)), 100)
    val src  = BuildKeyMatch.Source(IndexedSeq(col(0)))
    val m    = BuildKeyMatch(IndexedSeq(src))
    // Keys 0 until 200, half of them in the build
    val half           = Batch(Array(longs((0 until 4000).map(i => Some((i % 200).toLong)))), 4000)
    val all            = Batch(Array(longs((0 until 4000).map(i => Some((i % 100).toLong)))), 4000)
    def kept(b: Batch) = m.eval(b).asInstanceOf[BoolVec].values.count(identity)
    src.build = JoinBuild(keys, IndexedSeq(col(0)))
    kept(half) shouldBe 4000
    m.counts shouldBe (0L, 0L)
    src.build.ensureBuilt()
    // 17 batches pass the 64K-row sample with exactly half kept: the filter stays on
    (0 until 17).foreach(_ => kept(half) shouldBe 2000)
    m.counts shouldBe (68000L, 34000L)
    kept(all) shouldBe 4000
    // More than half kept past the sample: every row passes from now on
    kept(half) shouldBe 4000
    m.counts shouldBe (72000L, 38000L)
  }

end LeftMatchPrefilterTest
