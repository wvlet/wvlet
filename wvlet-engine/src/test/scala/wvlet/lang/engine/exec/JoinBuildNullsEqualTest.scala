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
import wvlet.uni.test.UniTest

/**
  * Join builds whose NULL keys match NULL probe keys (`IS NOT DISTINCT FROM`), for set operations
  */
class JoinBuildNullsEqualTest extends UniTest:
  private def nullsOf(xs: Seq[Any]): Array[Boolean] =
    if xs.contains(null) then
      xs.map(_ == null).toArray
    else
      null

  private def ints(xs: Any*): Vec = IntVec(
    PType.Int32,
    xs.map {
        case null =>
          0
        case i: Int =>
          i
      }
      .toArray,
    nullsOf(xs)
  )

  private def strs(xs: String*): Vec = StrVec(xs.toArray, nullsOf(xs))

  /** Runs tasks on the calling thread but reports parallelism, so hashed builds partition */
  private object PartitioningRunner extends TaskRunner:
    def parallelism: Int                               = 4
    def submit[A](task: () => A): TaskRunner.Handle[A] = TaskRunner.Sequential.submit(task)

  private def firstMatches(
      build: Array[Vec],
      probe: Array[Vec],
      nullsEqual: Boolean,
      runner: TaskRunner = TaskRunner.Sequential
  ): (JoinBuild, Seq[Int]) =
    val schema = Schema(build.indices.map(i => Field(s"k${i}", build(i).ptype)))
    val keys   = build.indices.map(i => ColRef(i, build(i).ptype, s"k${i}"))
    val jb     = JoinBuild(
      BatchesOp(schema, Seq(Batch(build, build(0).size))),
      keys,
      runner = runner,
      nullsEqual = nullsEqual
    )
    jb.ensureBuilt()
    (jb, jb.firstMatches(probe, probe(0).size).toSeq)

  test("should match NULL probe keys with NULL build keys only when NULLs are equal") {
    val build = Array(ints(1, null, 3))
    val probe = Array(ints(null, 3, 2))
    firstMatches(build, probe, nullsEqual = false)._2 shouldBe Seq(-1, 2, -1)
    val (jb, m) = firstMatches(build, probe, nullsEqual = true)
    m shouldBe Seq(1, 2, -1)
    // A NULL build key keeps the build off the direct index
    jb.usesDirectIndex shouldBe false
    jb.usesFlatTable shouldBe false
  }

  test("should keep the direct index when the build has no NULL key") {
    val (jb, m) = firstMatches(Array(ints(1, 2, 3)), Array(ints(null, 3)), nullsEqual = true)
    m shouldBe Seq(-1, 2)
    jb.usesDirectIndex shouldBe true
  }

  test("should match multi-column keys with NULLs in some columns") {
    val build = Array(ints(1, 1, null, null), strs("a", null, "b", null))
    val probe = Array(ints(1, 1, null, null, 1), strs(null, "a", null, "b", "b"))
    firstMatches(build, probe, nullsEqual = true)._2 shouldBe Seq(1, 0, 3, 2, -1)
    firstMatches(build, probe, nullsEqual = false)._2 shouldBe Seq(-1, 0, -1, -1, -1)
  }

  test("should match NULL keys in partitioned hash tables") {
    val n     = 70000
    val build = Array(
      ints(
        (0 until n).map(i =>
          if i % 1000 == 7 then
            null
          else
            i
        )*
      )
    )
    val probe   = Array(ints(null, 5, 1007, n + 1))
    val (jb, m) = firstMatches(build, probe, nullsEqual = true, PartitioningRunner)
    m shouldBe Seq(7, 5, -1, -1)
    jb.uniqueKeys shouldBe false
  }

end JoinBuildNullsEqualTest
