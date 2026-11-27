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
import wvlet.lang.engine.expr.*
import wvlet.lang.engine.plan.Parallelizer
import wvlet.lang.engine.vector.*
import wvlet.uni.test.UniTest

/**
  * Every flag of a hash join survives [[withFlagsOf]] and both of [[Parallelizer]]'s join rebuilds
  * (a pipeline's join and a join over a non-pipeline input). The flags are found by reflection
  * (each public Boolean `var` of [[HashJoinOp]] and [[JoinBuild]]), so a new flag that a rebuild
  * doesn't copy fails here without editing this test
  */
class EngineJoinFlagsTest extends UniTest:

  /** Plans as if tasks ran four at a time, running each when it is submitted */
  private object FourAtATime extends TaskRunner:
    def parallelism: Int                               = 4
    def submit[A](task: () => A): TaskRunner.Handle[A] = TaskRunner.Sequential.submit(task)

  /** Computed while building, not set by the planner */
  private val buildState = Set("uniqueKeys")

  private case class Flag(owner: Class[?], name: String):
    private def target(j: HashJoinOp): AnyRef =
      if owner == classOf[JoinBuild] then
        j.joinBuild
      else
        j

    def get(j: HashJoinOp): Boolean = owner.getMethod(name).invoke(target(j)).asInstanceOf[Boolean]
    def set(j: HashJoinOp, v: Boolean): Unit = owner
      .getMethod(s"${name}_$$eq", classOf[Boolean])
      .invoke(target(j), Boolean.box(v))

  private def flags(owner: Class[?]): Seq[Flag] = owner
    .getMethods
    .toSeq
    .filter(m => m.getName.endsWith("_$eq") && m.getParameterTypes.toSeq == Seq(classOf[Boolean]))
    .map(m => Flag(owner, m.getName.stripSuffix("_$eq")))
    .filterNot(f => buildState(f.name))

  private val allFlags = flags(classOf[HashJoinOp]) ++ flags(classOf[JoinBuild])

  private def col(i: Int): VExpr = ColRef(i, PType.Int64, s"c${i}")

  private val schema = Schema(IndexedSeq(Field("k", PType.Int64)))

  private def batch(xs: Long*): Batch = Batch(
    Array(LongVec(PType.Int64, xs.toArray, null)),
    xs.size
  )

  /** A table whose batches are its parts, so that a join over its scan runs as morsels */
  private class Parts(val name: String) extends TableSource:
    val schema: Schema                                          = EngineJoinFlagsTest.this.schema
    private val parts                                           = Seq(batch(1L, 2L), batch(3L, 4L))
    def scan(spec: ScanSpec): Iterator[Batch]                   = parts.iterator
    override def canSplit: Boolean                              = true
    override def scanSplits(spec: ScanSpec): Option[ScanSplits] = Some(
      ScanSplits(parts.toIndexedSeq.map(b => () => Iterator.single(b)), () => ())
    )

  private def morsels(op: Operator): Seq[MorselOp] =
    op match
      case m: MorselOp =>
        Seq(m)
      case _ =>
        op.children.flatMap(morsels)

  private def joins(op: Operator): Seq[HashJoinOp] =
    op match
      case j: HashJoinOp =>
        Seq(j)
      case _ =>
        op.children.flatMap(joins)

  private def batches(name: String): BatchesOp = BatchesOp(
    schema,
    Seq(Batch(Array(LongVec(PType.Int64, Array(1L, 2L), null)), 2)),
    name
  )

  /** A LEFT join without a residual, so that every flag (`unmatchedOnly` too) can be set */
  private def join(left: Operator): HashJoinOp = HashJoinOp(
    left,
    batches("r"),
    IndexedSeq(col(0)),
    IndexedSeq(col(0)),
    JoinKind.Left,
    None,
    Schema(schema.fields ++ schema.fields)
  )

  /** The join with every flag flipped from its default */
  private def flipped(left: Operator): HashJoinOp =
    val j = join(left)
    allFlags.foreach(f => f.set(j, !f.get(j)))
    j

  private def checkSame(original: HashJoinOp, rebuilt: HashJoinOp): Unit =
    (rebuilt ne original) shouldBe true
    allFlags.foreach { f =>
      (f.name, f.get(rebuilt)) shouldBe (f.name, f.get(original))
    }

  test("should find the planner-set flags of a hash join") {
    allFlags.map(_.name).toSet shouldContain "lateGather"
    allFlags.map(_.name).toSet shouldContain "parallelKeys"
  }

  test("should keep every flag in withFlagsOf") {
    val original = flipped(batches("l"))
    checkSame(original, join(batches("l")).withFlagsOf(original))
  }

  test("should keep every flag when Parallelizer rebuilds a join over a non-pipeline input") {
    val original = flipped(batches("l"))
    Parallelizer(original, FourAtATime) match
      case j: HashJoinOp =>
        checkSame(original, j)
      case other =>
        fail(s"Expected a hash join, got ${other.name}")
  }

  test("should keep every flag in the join of a Parallelizer pipeline") {
    val parts    = Parts("l")
    val original = flipped(ScanOp(parts, parts.schema, None, None))
    val segments = morsels(Parallelizer(original, FourAtATime)).flatMap(_.segments)
    segments.size shouldBe 1
    val copies = joins(segments.head.pipeline(segments.head.scan))
    copies.size shouldBe 1
    checkSame(original, copies.head)
  }

end EngineJoinFlagsTest
