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

import wvlet.lang.engine.WvletEngine
import wvlet.lang.engine.plan.Optimization
import wvlet.lang.engine.plan.OptimizerOptions
import wvlet.uni.test.UniTest

class SpillThresholdTest extends UniTest:
  private val mb = 1L << 20

  test("should leave a per-thread allowance below half the heap") {
    SpillThreshold.withHeadroom(4096 * mb, 20, 32 * mb) shouldBe (2048 - 640) * mb
    SpillThreshold.withHeadroom(32768 * mb, 20, 32 * mb) shouldBe (16384 - 640) * mb
  }

  test("should keep at least an eighth of a small heap") {
    SpillThreshold.withHeadroom(1536 * mb, 20, 32 * mb) shouldBe 192 * mb
    SpillThreshold.withHeadroom(768 * mb, 20, 32 * mb) shouldBe 96 * mb
  }

  test("should be half the heap without an allowance, and unbounded for an unbounded heap") {
    SpillThreshold.withHeadroom(4096 * mb, 20, 0L) shouldBe 2048 * mb
    SpillThreshold.withHeadroom(Long.MaxValue, 20, 32 * mb) shouldBe Long.MaxValue
  }

  test("should give queries the headroom threshold by default, and half the heap without it") {
    def threshold(opts: OptimizerOptions): Long =
      val m = WvletEngine(optimizer = opts).newQueryMemory()
      try m.spillThresholdBytes
      finally m.close()
    if sys.props.contains("wvlet.engine.spillThreshold") ||
      sys.props.contains("wvlet.engine.spillThreadAllowance")
    then
      ignore("A wvlet.engine.spill* system property overrides the default threshold")
    else
      val on  = threshold(OptimizerOptions())
      val off = threshold(OptimizerOptions().withDisabled(Optimization.SpillHeadroom))
      on shouldBe math.min(off, SpillCompat.headroomThresholdBytes(TaskRunner.default.parallelism))
      off shouldBe math.min(MemoryPool.process.queryLimitBytes, SpillCompat.defaultThresholdBytes)
      (on < off) shouldBe true
  }

  test("should keep an explicit threshold") {
    val m = WvletEngine().withSpillThreshold(5 * mb).newQueryMemory()
    try m.spillThresholdBytes shouldBe 5 * mb
    finally m.close()
  }

end SpillThresholdTest
