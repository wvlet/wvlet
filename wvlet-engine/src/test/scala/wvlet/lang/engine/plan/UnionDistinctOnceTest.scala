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
  * UNION (distinct) over scan pipelines runs one two-phase distinct over the union of the inputs
  * (`union_distinct_once`), with nested UNION (distinct) inputs flattened into it
  */
class UnionDistinctOnceTest extends UniTest:

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

  private def longs(xs: Long*): Vec = LongVec(PType.Int64, xs.toArray, null)

  private val schema = Schema(IndexedSeq(Field("k", PType.Int64), Field("v", PType.Int64)))

  /** A scan of two parts, each a batch of (k, v) pairs */
  private def table(name: String, a: Seq[(Long, Long)], b: Seq[(Long, Long)]): ScanOp =
    def batch(rows: Seq[(Long, Long)]) = Batch(
      Array(longs(rows.map(_._1)*), longs(rows.map(_._2)*)),
      rows.size
    )
    val t = Parts(name, schema, Seq(batch(a), batch(b)))
    ScanOp(t, t.schema, None, None)

  // Duplicates within a part, across the parts of an input, and across inputs
  private def a = table("a", Seq(1L -> 10L, 1L -> 10L, 2L -> 20L), Seq(2L -> 20L, 3L -> 30L))
  private def b = table("b", Seq(3L -> 30L, 4L -> 40L), Seq(4L -> 40L, 1L -> 10L))
  private def c = table("c", Seq(5L -> 50L), Seq(5L -> 50L, 2L -> 20L))

  /** distinct(a ∪ distinct(b ∪ c)) */
  private def nested: Operator = DistinctOp(
    UnionAllOp(Seq(a, DistinctOp(UnionAllOp(Seq(b, c), schema))), schema)
  )

  private def rows(op: Operator): Seq[Seq[Any]] =
    try op
        .drain()
        .flatMap(b => (0 until b.size).map(i => b.columns.map(_.get(i)).toSeq))
        .sortBy(_.toString)
    finally op.close()

  private def parallel(op: Operator, once: Boolean = true): Operator = Parallelizer(
    op,
    FourAtATime,
    unionDistinctOnce = once
  )

  private def count(text: String, word: String): Int = text.split(word, -1).length - 1

  private val expected = Seq(1L -> 10L, 2L -> 20L, 3L -> 30L, 4L -> 40L, 5L -> 50L)
    .map((k, v) => Seq(k, v))
    .sortBy(_.toString)

  test("should run nested UNION (distinct) as one two-phase distinct over every input") {
    val op   = parallel(nested)
    val plan = op.explain()
    plan shouldNotContain "Distinct"
    plan shouldNotContain "UnionAll"
    count(plan, "PartitionedAgg") shouldBe 1
    rows(op) shouldBe expected
  }

  test("should reduce each input to its distinct rows first without the toggle") {
    val op = parallel(nested, once = false)
    // The distinct rows of a, b, and c, then of b ∪ c, then of the outer union
    count(op.explain(), "PartitionedAgg") shouldBe 5
    rows(op) shouldBe expected
  }

  test("should keep a nested union whose column types differ from the outer one") {
    // A union of Int32 columns, cast to the outer union's Int64 ones
    val intSchema = Schema(IndexedSeq(Field("k", PType.Int32), Field("v", PType.Int32)))
    val t         = Parts(
      "i",
      intSchema,
      Seq(
        Batch(
          Array(IntVec(PType.Int32, Array(1, 7), null), IntVec(PType.Int32, Array(10, 70), null)),
          2
        )
      )
    )
    def inner = DistinctOp(UnionAllOp(Seq(ScanOp(t, t.schema, None, None)), intSchema))
    val op    = parallel(DistinctOp(UnionAllOp(Seq(a, inner), schema)))
    // Not flattened: the distinct rows of a, of the inner union, then of the outer union
    count(op.explain(), "PartitionedAgg") shouldBe 3
    rows(op) shouldBe Seq(Seq(1L, 10L), Seq(2L, 20L), Seq(3L, 30L), Seq(7L, 70L)).sortBy(_.toString)
  }

  test("should keep the per-input distincts when an input emits unmatched build rows") {
    val d = MemTable("d", schema, Seq(Batch(Array(longs(1, 6), longs(100, 600)), 2)))
    // A right join emits its unmatched build rows after its parts: no union pipeline
    def keys: Operator =
      val right = HashJoinOp(
        a,
        ScanOp(d, d.schema, None, None),
        IndexedSeq(ColRef(0, PType.Int64, "k")),
        IndexedSeq(ColRef(0, PType.Int64, "k")),
        JoinKind.Right,
        None,
        Schema(schema.fields ++ schema.fields.map(f => f.copy(name = s"d_${f.name}")))
      )
      ProjectOp(
        right,
        IndexedSeq(ColRef(2, PType.Int64, "d_k"), ColRef(3, PType.Int64, "d_v")),
        schema
      )
    def plan = DistinctOp(UnionAllOp(Seq(keys, DistinctOp(UnionAllOp(Seq(b, c), schema))), schema))
    val op   = parallel(plan)
    op.explain() shouldContain "UnionAll"
    val want = (expected ++ Seq(Seq(1L, 100L), Seq(6L, 600L))).sortBy(_.toString)
    rows(op) shouldBe want
    rows(plan) shouldBe want
  }

end UnionDistinctOnceTest
