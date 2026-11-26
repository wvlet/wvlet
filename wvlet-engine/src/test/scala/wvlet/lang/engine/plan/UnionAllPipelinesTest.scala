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
  * A UNION ALL of scan pipelines runs as one [[MorselOp]] (`union_all_pipelines`): the operators
  * above the union run on the parts of every input, in input order and then part order
  */
class UnionAllPipelinesTest extends UniTest:

  /** Runs each task when it is submitted, but plans as if tasks ran four at a time */
  private object FourAtATime extends TaskRunner:
    def parallelism: Int                               = 4
    def submit[A](task: () => A): TaskRunner.Handle[A] = TaskRunner.Sequential.submit(task)

  /** A table whose batches are its parts, or one part when `split` is false */
  private class Parts(val name: String, val schema: Schema, batches: Seq[Batch], split: Boolean)
      extends TableSource:
    def scan(spec: ScanSpec): Iterator[Batch]                   = batches.iterator
    override def canSplit: Boolean                              = true
    override def scanSplits(spec: ScanSpec): Option[ScanSplits] =
      if split && batches.size > 1 then
        Some(ScanSplits(batches.toIndexedSeq.map(b => () => Iterator.single(b)), () => ()))
      else
        None

  private def longs(xs: Long*): Vec = LongVec(PType.Int64, xs.toArray, null)
  private def ints(xs: Int*): Vec   = IntVec(PType.Int32, xs.toArray, null)

  private val longSchema = Schema(IndexedSeq(Field("k", PType.Int64), Field("v", PType.Int64)))
  private val intSchema  = Schema(IndexedSeq(Field("k", PType.Int32), Field("v", PType.Int32)))

  private def scan(t: Parts): ScanOp = ScanOp(t, t.schema, None, None)

  // Keys 1-3 on the probe side; the build has keys 1, 2, and 4
  private def union: UnionAllOp =
    val a = Parts(
      "a",
      longSchema,
      Seq(Batch(Array(longs(1, 2), longs(10, 20)), 2), Batch(Array(longs(3, 1), longs(30, 11)), 2)),
      split = true
    )
    // A single part, run on the calling thread
    val b = Parts("b", longSchema, Seq(Batch(Array(longs(2, 3), longs(21, 31)), 2)), split = false)
    // Int32 columns, cast to the union's Int64 ones
    val c = Parts(
      "c",
      intSchema,
      Seq(Batch(Array(ints(1), ints(12)), 1), Batch(Array(ints(2), ints(22)), 1)),
      split = true
    )
    UnionAllOp(Seq(scan(a), scan(b), scan(c)), longSchema)

  private def build: Operator =
    val d = MemTable(
      "d",
      Schema(IndexedSeq(Field("dk", PType.Int64), Field("name", PType.Str))),
      Seq(Batch(Array(longs(1, 2, 4), StrVec(Array("one", "two", "four"), null)), 3))
    )
    ScanOp(d, d.schema, None, None)

  private def join(kind: JoinKind): HashJoinOp =
    val u = union
    val b = build
    HashJoinOp(
      u,
      b,
      IndexedSeq(ColRef(0, PType.Int64, "k")),
      IndexedSeq(ColRef(0, PType.Int64, "dk")),
      kind,
      None,
      Schema(u.schema.fields ++ b.schema.fields)
    )

  private def rows(op: Operator): Seq[Seq[Any]] =
    try op
        .drain()
        .flatMap(b =>
          (0 until b.size).map(i =>
            b.columns
              .map(v =>
                if v.isNull(i) then
                  null
                else
                  v.get(i)
              )
              .toSeq
          )
        )
    finally op.close()

  private def parallel(
      op: Operator,
      unionPipelines: Boolean = true,
      parallelOuterTail: Boolean = true
  ): Operator = Parallelizer(
    op,
    FourAtATime,
    unionPipelines = unionPipelines,
    parallelOuterTail = parallelOuterTail
  )

  test("probe a hash join over a UNION ALL as one morsel pipeline, in input and part order") {
    val op = parallel(join(JoinKind.Inner))
    op shouldMatch { case m: MorselOp =>
      m.segments.size shouldBe 3
    }
    op.explain() shouldNotContain "UnionAll"
    // Input order, then part order, then probe order within a part
    val out = rows(op)
    out shouldBe rows(join(JoinKind.Inner))
    out.map(_.take(2)) shouldBe
      Seq(Seq(1L, 10L), Seq(2L, 20L), Seq(1L, 11L), Seq(2L, 21L), Seq(1L, 12L), Seq(2L, 22L))
  }

  test("emit the unmatched build rows of a right join over a UNION ALL once, after every part") {
    val op = parallel(join(JoinKind.Right))
    op shouldMatch { case _: MorselOp =>
    }
    val out = rows(op)
    out.sortBy(_.toString) shouldBe rows(join(JoinKind.Right)).sortBy(_.toString)
    out.count(_(2) == 4L) shouldBe 1
  }

  test("aggregate a UNION ALL in two phases over the parts of every input") {
    val sumType        = Aggregates.resultType("sum", Seq(PType.Int64))
    def agg: HashAggOp =
      val j = join(JoinKind.Inner)
      HashAggOp(
        j,
        IndexedSeq(ColRef(3, PType.Str, "name")),
        IndexedSeq(AggCall("sum", Seq(ColRef(1, PType.Int64, "v")), false, None, sumType)),
        Schema(IndexedSeq(Field("name", PType.Str), Field("s", sumType)))
      )
    val op = parallel(agg)
    op.explain() shouldNotContain "UnionAll"
    rows(op).sortBy(_.toString) shouldBe rows(agg).sortBy(_.toString)
  }

  def tailUnderJoin: Operator =
    val right = join(JoinKind.Right)
    val u     = UnionAllOp(Seq(right, join(JoinKind.Inner)), right.schema)
    val b     = build
    HashJoinOp(
      u,
      b,
      IndexedSeq(ColRef(2, PType.Int64, "dk")),
      IndexedSeq(ColRef(0, PType.Int64, "dk")),
      JoinKind.Inner,
      None,
      Schema(u.schema.fields ++ b.schema.fields)
    )

  test("keep the union's own pipelines when an input emits unmatched build rows after its parts") {
    val op = parallel(tailUnderJoin, parallelOuterTail = false)
    // The right join's tail runs after its own parts, so the join above runs over the union
    op shouldMatch { case _: HashJoinOp =>
    }
    op.explain() shouldContain "UnionAll"
    rows(op).sortBy(_.toString) shouldBe rows(tailUnderJoin).sortBy(_.toString)
  }

  test(
    "run an input's unmatched build rows through the union's pipeline with parallel_outer_tail"
  ) {
    val op = parallel(tailUnderJoin)
    // The right join's tail runs after every part of the union, through the join above it
    op shouldMatch { case _: MorselOp =>
    }
    op.explain() shouldNotContain "UnionAll"
    rows(op).sortBy(_.toString) shouldBe rows(tailUnderJoin).sortBy(_.toString)
  }

  test("keep the union's own pipelines without the toggle, or with nothing above it") {
    parallel(join(JoinKind.Inner), unionPipelines = false).explain() shouldContain "UnionAll"
    val op = parallel(union)
    op shouldMatch { case _: UnionAllOp =>
    }
    rows(op) shouldBe rows(union)
  }

end UnionAllPipelinesTest
